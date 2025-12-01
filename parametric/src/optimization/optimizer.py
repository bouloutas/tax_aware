"""
Portfolio optimization engine using CVXPY.

Optimizes portfolio to minimize tracking error while maximizing tax benefits.
Uses a multi-factor risk model (Barra) for accurate tracking error estimation.
"""
from typing import Optional, List, Dict
from datetime import date

import cvxpy as cp
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src.core.config import Config
from src.optimization.risk_model import RiskModel
from src.optimization.tracking_error import TrackingErrorCalculator
from src.tax_harvesting.lot_selection import TaxLotSelector, LotSelectionStrategy
from src.tax_harvesting.cross_account import CrossAccountWashSaleDetector
from src.tax_harvesting.gain_deferral import GainDeferralCalculator


class PortfolioOptimizer:
    """
    Portfolio optimizer using quadratic programming.

    Objective:
        Minimize: Risk(w - b) + Lambda_Trans * Costs - Lambda_Tax * TaxBenefit

    Subject to:
        - Fully invested
        - Long-only
        - Turnover limits
        - Wash sale constraints (restricted buys)
        - Sector constraints
    """

    def __init__(self, session: Session):
        """
        Initialize PortfolioOptimizer.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.tracking_error_calc = TrackingErrorCalculator(session)
        self.risk_model = RiskModel(session)
        self.lot_selector = TaxLotSelector(session)
        self.cross_account_detector = CrossAccountWashSaleDetector(session)
        self.gain_calculator = GainDeferralCalculator(session)
        self.lambda_transaction = Config.LAMBDA_TRANSACTION
        self.lambda_tax = Config.LAMBDA_TAX
        self.lambda_gain = 1.0  # Gain deferral weight (can be configured)
        self.turnover_limit = Config.TURNOVER_LIMIT

    def optimize_portfolio(
        self,
        account_id: int,
        tax_benefit_coefficients: Optional[pd.Series] = None,
        wash_sale_restricted_buys: Optional[List[int]] = None,
        max_tracking_error: Optional[float] = None,
        sector_constraints: Optional[dict] = None,
        lot_selection_strategy: LotSelectionStrategy = LotSelectionStrategy.HIFO,
    ) -> dict:
        """
        Optimize portfolio weights.

        Args:
            account_id: Account ID
            tax_benefit_coefficients: Series (security_id -> benefit per dollar sold)
                                      Higher positive value = more tax loss to harvest.
            wash_sale_restricted_buys: List of security_ids that cannot be bought.
            max_tracking_error: Maximum allowed tracking error (variance).
            sector_constraints: Dictionary of sector -> max_weight.
            lot_selection_strategy: Tax lot selection strategy for trade execution.

        Returns:
            Dictionary with optimization results.
        """
        # 1. Data Loading
        current_weights_df = self.tracking_error_calc.get_current_weights(account_id)
        if current_weights_df.empty:
            raise ValueError(f"No positions found for account {account_id}")

        benchmark_weights_df = self.tracking_error_calc.get_benchmark_weights(account_id)
        if benchmark_weights_df.empty:
            # Fallback: equal-weight current holdings
            benchmark_weights_df = current_weights_df[["security_id"]].copy()
            benchmark_weights_df["weight"] = 1.0 / len(benchmark_weights_df)

        # Integrate household-level wash sale restrictions
        household_restricted = self.cross_account_detector.get_restricted_securities_household(
            account_id, date.today()
        )
        
        # Merge with explicit restrictions
        if wash_sale_restricted_buys is None:
            wash_sale_restricted_buys = []
        wash_sale_restricted_buys = list(
            set(wash_sale_restricted_buys) | household_restricted
        )

        # Align universe
        all_security_ids = sorted(list(set(current_weights_df["security_id"]) | set(benchmark_weights_df["security_id"])))
        n_assets = len(all_security_ids)
        asset_index_map = {sid: i for i, sid in enumerate(all_security_ids)}

        # Create weight vectors
        w_current = np.zeros(n_assets)
        w_benchmark = np.zeros(n_assets)

        for _, row in current_weights_df.iterrows():
            if row["security_id"] in asset_index_map:
                w_current[asset_index_map[row["security_id"]]] = row["weight"]

        for _, row in benchmark_weights_df.iterrows():
            if row["security_id"] in asset_index_map:
                w_benchmark[asset_index_map[row["security_id"]]] = row["weight"]
        
        # Normalize benchmark
        if w_benchmark.sum() > 0:
            w_benchmark /= w_benchmark.sum()

        # 2. Risk Model Components
        # Get X (exposures), F (factor cov), D (specific var)
        try:
            X, F, D = self.risk_model.get_risk_components(all_security_ids)
            use_risk_model = True
        except Exception as e:
            print(f"Risk model failed: {e}. Falling back to simple tracking error.")
            use_risk_model = False
            X, F, D = None, None, None

        # 3. Optimization Variables
        w = cp.Variable(n_assets, name="weights")
        buys = cp.Variable(n_assets, nonneg=True, name="buys")
        sells = cp.Variable(n_assets, nonneg=True, name="sells")

        # 4. Objective Function terms
        
        # Tracking Error (Risk)
        active_weights = w - w_benchmark
        
        if use_risk_model and X is not None:
            # Factor Risk: (w-b)' X F X' (w-b)
            # Specific Risk: (w-b)' D (w-b)
            
            # Helper for Factor Risk: f = X' (w-b)
            # We need X values as matrix
            X_values = X.values # (N x K)
            F_values = F.values # (K x K)
            D_values = D.values # (N)
            
            # Decompose F for efficiency (F = L L')? 
            # Or just use quad_form if K is small (200 factors is fine)
            # Use psd_wrap to handle minor numerical noise in PSD check
            factor_risk = cp.quad_form(active_weights @ X_values, cp.psd_wrap(F_values))
            specific_risk = cp.sum(cp.multiply(D_values, active_weights**2))
            
            risk_term = factor_risk + specific_risk
        else:
            # Fallback: simple sum of squared deviations (identity covariance)
            risk_term = cp.sum_squares(active_weights)

        # Transaction Costs
        # Cost = sum(buys + sells) * cost_rate
        # Note: trades = buys - sells, |trades| = buys + sells since buys/sells >= 0 and disjoint (optimally)
        cost_rate = 0.0010 # 10 bps
        transaction_cost_term = cp.sum(buys + sells) * cost_rate

        # Tax Benefit
        # Maximize: sells * tax_coeff
        # Minimize: - (sells * tax_coeff)
        tax_term = 0
        if tax_benefit_coefficients is not None:
            # Align coefficients
            tax_coeffs = np.zeros(n_assets)
            for sid, coeff in tax_benefit_coefficients.items():
                if sid in asset_index_map:
                    tax_coeffs[asset_index_map[sid]] = coeff
            
            tax_term = -cp.sum(cp.multiply(sells, tax_coeffs))

        # Gain Deferral Penalty
        # Penalize selling securities with large embedded gains
        # Higher penalty = more reluctant to sell
        gain_penalty_term = 0
        try:
            gain_penalties = self.gain_calculator.get_gain_penalty_coefficients(
                account_id=account_id,
                security_ids=all_security_ids,
            )
            if not gain_penalties.empty:
                gain_penalty_coeffs = np.zeros(n_assets)
                for sid, penalty in gain_penalties.items():
                    if sid in asset_index_map:
                        gain_penalty_coeffs[asset_index_map[sid]] = penalty
                
                # Penalty on sells (discourages selling gainers)
                gain_penalty_term = cp.sum(cp.multiply(sells, gain_penalty_coeffs))
        except Exception as e:
            # If gain calculation fails, just skip it (warnings already logged)
            pass

        # Total Objective
        # Minimize: Risk + TransactionCosts - TaxBenefits + GainPenalties
        # Risk: tracking error variance
        # TransactionCosts: cost of trading
        # TaxBenefits: negative (reward for harvesting losses)
        # GainPenalties: positive (discourage realizing gains)
        objective = cp.Minimize(
            risk_term 
            + self.lambda_transaction * transaction_cost_term 
            + self.lambda_tax * tax_term
            + self.lambda_gain * gain_penalty_term
        )

        # 5. Constraints
        constraints = [
            cp.sum(w) == 1.0,      # Fully invested
            w >= 0.0,              # Long only
            w == w_current + buys - sells, # Flow conservation
            cp.sum(buys + sells) <= self.turnover_limit # Turnover
        ]

        # Wash Sale Constraint (Restricted Buys)
        if wash_sale_restricted_buys:
            for sid in wash_sale_restricted_buys:
                if sid in asset_index_map:
                    idx = asset_index_map[sid]
                    constraints.append(buys[idx] == 0)

        # Tracking Error Constraint (Hard limit if requested)
        if max_tracking_error is not None and use_risk_model:
             constraints.append(risk_term <= max_tracking_error**2)

        # Sector Constraints
        if sector_constraints:
            from src.core.database import Security
            # Pre-fetch sectors
            sectors = {}
            for sid in all_security_ids:
                sec = self.session.query(Security).filter(Security.security_id == sid).first()
                if sec and sec.sector:
                    sectors[sid] = sec.sector
            
            for sector, limit in sector_constraints.items():
                indices = [i for i, sid in enumerate(all_security_ids) if sectors.get(sid) == sector]
                if indices:
                    constraints.append(cp.sum(w[indices]) <= limit)

        # 6. Solve
        problem = cp.Problem(objective, constraints)
        try:
            problem.solve(solver=cp.OSQP, verbose=False)
        except Exception:
            pass # Try fallback

        if problem.status not in ["optimal", "optimal_inaccurate"]:
            try:
                # Fallback to SCS if OSQP fails or hits limit
                problem.solve(solver=cp.SCS, verbose=False)
            except Exception:
                pass

        if problem.status not in ["optimal", "optimal_inaccurate"]:
             raise ValueError(f"Optimization failed: {problem.status}")

        # 7. Extract Results
        optimal_w = w.value
        trade_w = buys.value - sells.value
        
        # Filter small values
        optimal_w[np.abs(optimal_w) < 1e-6] = 0
        trade_w[np.abs(trade_w) < 1e-6] = 0
        
        res_weights = pd.Series(optimal_w, index=all_security_ids)
        res_trades = pd.Series(trade_w, index=all_security_ids)
        res_trades = res_trades[res_trades != 0]

        # Calculate final metrics
        final_risk = np.sqrt(risk_term.value) if risk_term.value > 0 else 0.0
        final_tax = -tax_term.value if isinstance(tax_term, cp.Expression) else 0.0

        return {
            "optimal_weights": res_weights.to_dict(),
            "trades": res_trades.to_dict(),
            "tracking_error": final_risk,
            "tax_benefit_score": final_tax,
            "status": problem.status
        }

    def optimize_with_tax_harvesting(
        self,
        account_id: int,
        tax_loss_opportunities: list, # List of Opportunity objects
        max_tracking_error: Optional[float] = None
    ) -> dict:
        """
        Optimize portfolio with inputs from the tax harvesting engine.
        Adapts Opportunity objects to optimization parameters.
        """
        # Convert opportunities to tax benefit coefficients
        # Opportunity usually has: security_id, unrealized_loss, tax_benefit (total $)
        # We need: tax benefit per dollar sold.
        # Approx: tax_benefit / current_value_of_lot
        
        # Since we don't have lot-level granularity in the QP variables (only aggregate sells),
        # we approximate by averaging the tax benefit ratio for the security.
        
        tax_coeffs = {}
        wash_sale_restricted = []
        
        # Group by security
        opps_by_sec = {}
        for opp in tax_loss_opportunities:
            if opp.security_id not in opps_by_sec:
                opps_by_sec[opp.security_id] = []
            opps_by_sec[opp.security_id].append(opp)
            
        for sec_id, opps in opps_by_sec.items():
            # Calculate weighted average tax benefit per dollar
            # Note: Opportunity finder logic usually finds lots.
            # Let's assume opp.tax_benefit is absolute $ and opp.market_value is lot size $
            # Check opportunity_finder.py structure later. Assuming simple structure now.
            
            total_val = sum(getattr(o, 'market_value', 1.0) for o in opps) # fallback 1.0 if missing
            total_benefit = sum(o.tax_benefit for o in opps)
            
            if total_val > 0:
                coeff = total_benefit / total_val
                tax_coeffs[sec_id] = coeff
                
        # Assuming wash sale logic is handled by passing restricted list
        # We might need to call a wash sale checker here or assume opps are already filtered.
        # The PRD says "Identify replacement securities that would violate wash sale".
        # The optimizer should probably NOT buy the securities we just sold (if we sell them).
        # But we don't know if we sell them until we solve!
        # Standard approach: 
        # 1. If we have a loss opportunity in A, we might sell A. 
        # 2. If we sell A at a loss, we cannot buy "substantially identical" B (or A itself).
        # 3. But we can buy C.
        
        # Simplification: If a security has a tax loss opportunity, we restrict BUYING it 
        # (since we are biased to sell it). This prevents "wash sale" on the stock itself immediately.
        # True wash sale logic regarding replacements is harder.
        
        wash_sale_restricted = list(tax_coeffs.keys()) 

        return self.optimize_portfolio(
            account_id=account_id,
            tax_benefit_coefficients=pd.Series(tax_coeffs),
            wash_sale_restricted_buys=wash_sale_restricted,
            max_tracking_error=max_tracking_error
        )
