"""
Portfolio optimization engine.

This module provides portfolio optimization functionality:
- Tracking error calculation
- Portfolio optimization with CVXPY
- Risk model implementation
- Sector/exposure constraints
"""
from src.optimization.optimizer import PortfolioOptimizer
from src.optimization.risk_model import RiskModel
from src.optimization.tracking_error import TrackingErrorCalculator

__all__ = [
    "TrackingErrorCalculator",
    "RiskModel",
    "PortfolioOptimizer",
]
