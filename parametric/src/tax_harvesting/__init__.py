"""
Tax-loss harvesting engine.

This module provides the core tax-loss harvesting functionality:
- Wash sale detection
- Replacement security identification
- Tax benefit calculation
- Opportunity identification
"""
from src.tax_harvesting.opportunity_finder import (
    TaxLossHarvestingFinder,
    TaxLossHarvestingOpportunity,
)
from src.tax_harvesting.replacement_security import ReplacementSecurityFinder
from src.tax_harvesting.tax_benefit import TaxBenefitCalculator
from src.tax_harvesting.wash_sale import WashSaleDetector

__all__ = [
    "WashSaleDetector",
    "ReplacementSecurityFinder",
    "TaxBenefitCalculator",
    "TaxLossHarvestingFinder",
    "TaxLossHarvestingOpportunity",
]
