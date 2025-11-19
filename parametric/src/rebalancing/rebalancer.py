"""
Main rebalancing engine.

Orchestrates the rebalancing process: trigger detection, optimization, trade generation, compliance, execution.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from src.core.config import Config
from src.core.database import Account, RebalancingEvent
from src.optimization import PortfolioOptimizer, TrackingErrorCalculator
from src.tax_harvesting import TaxLossHarvestingFinder


class Rebalancer:
    """
    Main rebalancing engine.

    Handles:
    - Rebalancing trigger detection
    - Running optimization with tax-loss harvesting
    - Trade generation
    - Compliance checking
    - Trade execution
    """

    def __init__(self, session: Session):
        """
        Initialize Rebalancer with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.tracking_error_calc = TrackingErrorCalculator(session)
        self.optimizer = PortfolioOptimizer(session)
        self.tax_harvest_finder = TaxLossHarvestingFinder(session)
        self.tracking_error_threshold = Config.TRACKING_ERROR_THRESHOLD

    def check_rebalancing_needed(
        self,
        account_id: int,
        rebalancing_type: str = "threshold",
    ) -> dict:
        """
        Check if rebalancing is needed for an account.

        Args:
            account_id: Account ID
            rebalancing_type: Type of check ('threshold', 'tax_loss', 'scheduled', 'manual')

        Returns:
            Dictionary with:
            {
                'rebalancing_needed': bool,
                'reason': str,
                'current_tracking_error': float,
                'tax_opportunities': int,
                'details': dict
            }
        """
        result = {
            "rebalancing_needed": False,
            "reason": "",
            "current_tracking_error": 0.0,
            "tax_opportunities": 0,
            "details": {},
        }

        if rebalancing_type == "threshold":
            # Check tracking error threshold
            try:
                te_result = self.tracking_error_calc.calculate_tracking_error(account_id)
                current_te = te_result["tracking_error"]
            except Exception:
                # If benchmark not assigned or TE not computable, treat as 0 for threshold checks
                current_te = 0.0

            result["current_tracking_error"] = current_te

            if current_te > self.tracking_error_threshold:
                result["rebalancing_needed"] = True
                result["reason"] = f"Tracking error ({current_te:.4f}) exceeds threshold ({self.tracking_error_threshold:.4f})"
                result["details"] = {"tracking_error": current_te, "threshold": self.tracking_error_threshold}

        elif rebalancing_type == "tax_loss":
            # Check for tax-loss harvesting opportunities
            opportunities = self.tax_harvest_finder.find_opportunities(
                account_id=account_id,
                min_loss_threshold=Decimal(str(Config.MIN_TAX_LOSS_THRESHOLD)),
                max_opportunities=10,
            )

            result["tax_opportunities"] = len(opportunities)

            if opportunities:
                # Check if any opportunities are significant
                significant_opportunities = [
                    opp for opp in opportunities if opp.tax_benefit > Decimal("500") and not opp.wash_sale_violation
                ]

                if significant_opportunities:
                    result["rebalancing_needed"] = True
                    result["reason"] = f"Found {len(significant_opportunities)} significant tax-loss harvesting opportunities"
                    result["details"] = {
                        "total_opportunities": len(opportunities),
                        "significant_opportunities": len(significant_opportunities),
                        "total_tax_benefit": sum(opp.tax_benefit for opp in significant_opportunities),
                    }

        elif rebalancing_type == "scheduled":
            # Scheduled rebalancing (e.g., monthly)
            # Check last rebalancing date
            last_rebalancing = (
                self.session.query(RebalancingEvent)
                .filter(RebalancingEvent.account_id == account_id, RebalancingEvent.status == "executed")
                .order_by(RebalancingEvent.rebalancing_date.desc())
                .first()
            )

            if not last_rebalancing:
                result["rebalancing_needed"] = True
                result["reason"] = "No previous rebalancing found - initial rebalancing needed"
            else:
                days_since_rebalancing = (date.today() - last_rebalancing.rebalancing_date).days
                if days_since_rebalancing >= 30:  # Monthly rebalancing
                    result["rebalancing_needed"] = True
                    result["reason"] = f"Scheduled rebalancing - {days_since_rebalancing} days since last rebalancing"
                    result["details"] = {"days_since_rebalancing": days_since_rebalancing}

        elif rebalancing_type == "manual":
            # Manual rebalancing - always needed if requested
            result["rebalancing_needed"] = True
            result["reason"] = "Manual rebalancing requested"

        return result

    def rebalance_account(
        self,
        account_id: int,
        rebalancing_type: str = "threshold",
        max_tracking_error: Optional[float] = None,
        auto_execute: bool = False,
    ) -> dict:
        """
        Rebalance an account's portfolio.

        Args:
            account_id: Account ID
            rebalancing_type: Type of rebalancing ('threshold', 'tax_loss', 'scheduled', 'manual')
            max_tracking_error: Maximum allowed tracking error (optional)
            auto_execute: If True, automatically execute trades (default: False)

        Returns:
            Dictionary with rebalancing results:
            {
                'rebalancing_event_id': int,
                'status': str,
                'trades': list,
                'tracking_error_before': float,
                'tracking_error_after': float,
                'tax_benefit': float,
                'message': str
            }
        """
        # Check if rebalancing is needed
        check_result = self.check_rebalancing_needed(account_id, rebalancing_type)
        if not check_result["rebalancing_needed"] and rebalancing_type != "manual":
            return {
                "rebalancing_event_id": None,
                "status": "skipped",
                "trades": [],
                "tracking_error_before": check_result["current_tracking_error"],
                "tracking_error_after": check_result["current_tracking_error"],
                "tax_benefit": 0.0,
                "message": check_result["reason"],
            }

        # Get account
        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            raise ValueError(f"Account {account_id} not found")

        # Calculate tracking error before
        try:
            if rebalancing_type == "threshold":
                # Use same norm-based metric as optimizer for apples-to-apples comparison
                import numpy as np
                cw_df = self.tracking_error_calc.get_current_weights(account_id)
                bw_df = self.tracking_error_calc.get_benchmark_weights(account_id)
                all_ids = sorted(set(cw_df["security_id"].tolist()) | set(bw_df["security_id"].tolist()))
                cw = {int(r["security_id"]): float(r["weight"]) for _, r in cw_df.iterrows()}
                bw = {int(r["security_id"]): float(r["weight"]) for _, r in bw_df.iterrows()}
                current_vec = np.array([cw.get(i, 0.0) for i in all_ids], dtype=float)
                bench_vec = np.array([bw.get(i, 0.0) for i in all_ids], dtype=float)
                # Normalize benchmark to 1 if needed
                s = bench_vec.sum()
                if s > 0:
                    bench_vec = bench_vec / s
                tracking_error_before = float(np.linalg.norm(current_vec - bench_vec))
            else:
                te_before = self.tracking_error_calc.calculate_tracking_error(account_id)
                tracking_error_before = te_before["tracking_error"]
        except Exception:
            tracking_error_before = 0.0

        # Find tax-loss harvesting opportunities
        tax_opportunities = self.tax_harvest_finder.find_opportunities(
            account_id=account_id,
            min_loss_threshold=Decimal(str(Config.MIN_TAX_LOSS_THRESHOLD)),
            max_opportunities=20,
        )

        # Set max tracking error if not provided
        if max_tracking_error is None:
            max_tracking_error = self.tracking_error_threshold * 1.5  # Allow 50% above threshold

        # Run optimization (for threshold-driven rebalancing, avoid hard TE constraint to prevent infeasibility)
        te_constraint = None if rebalancing_type == "threshold" else max_tracking_error
        opt_result = self.optimizer.optimize_with_tax_harvesting(
            account_id=account_id,
            tax_loss_opportunities=tax_opportunities,
            max_tracking_error=te_constraint,
        )

        if opt_result["status"] not in ["optimal", "optimal_inaccurate"]:
            return {
                "rebalancing_event_id": None,
                "status": "failed",
                "trades": [],
                "tracking_error_before": tracking_error_before,
                "tracking_error_after": tracking_error_before,
                "tax_benefit": 0.0,
                "message": f"Optimization failed: {opt_result['status']}",
            }

        # Create rebalancing event
        rebalancing_event = RebalancingEvent(
            account_id=account_id,
            rebalancing_date=date.today(),
            rebalancing_type=rebalancing_type,
            tracking_error_before=Decimal(str(tracking_error_before)),
            status="pending",
        )

        self.session.add(rebalancing_event)
        self.session.commit()
        self.session.refresh(rebalancing_event)

        # Generate trades from optimization results
        from src.rebalancing.trade_generator import TradeGenerator

        trade_generator = TradeGenerator(self.session)
        trades = trade_generator.generate_trades_from_optimization(
            rebalancing_event_id=rebalancing_event.rebalancing_id,
            opt_result=opt_result,
            tax_opportunities=tax_opportunities,
        )

        # Calculate tracking error after (estimated)
        tracking_error_after = opt_result["tracking_error"]

        # Calculate total tax benefit
        total_tax_benefit = sum(opp.tax_benefit for opp in tax_opportunities[:10])

        # Update rebalancing event
        rebalancing_event.tracking_error_after = Decimal(str(tracking_error_after))
        rebalancing_event.tax_benefit = Decimal(str(total_tax_benefit))

        if auto_execute:
            # Execute trades
            from src.rebalancing.compliance import ComplianceChecker

            compliance_checker = ComplianceChecker(self.session)
            compliance_result = compliance_checker.check_trades(trades, account_id)

            if compliance_result["passed"]:
                trade_generator.execute_trades(trades, account_id)
                rebalancing_event.status = "executed"
                message = f"Rebalancing executed: {len(trades)} trades"
            else:
                rebalancing_event.status = "failed"
                message = f"Rebalancing failed compliance: {compliance_result['errors']}"
        else:
            rebalancing_event.status = "pending"
            message = f"Rebalancing pending approval: {len(trades)} trades generated"

        self.session.commit()

        return {
            "rebalancing_event_id": rebalancing_event.rebalancing_id,
            "status": rebalancing_event.status,
            "trades": [trade.to_dict() if hasattr(trade, "to_dict") else str(trade) for trade in trades],
            "tracking_error_before": float(tracking_error_before),
            "tracking_error_after": float(tracking_error_after),
            "tax_benefit": float(total_tax_benefit),
            "message": message,
        }

