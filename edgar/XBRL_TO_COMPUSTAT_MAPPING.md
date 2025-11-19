# XBRL to Compustat Item Mapping

**Purpose:** Comprehensive mapping from XBRL us-gaap tags to Compustat item codes  
**Last Updated:** November 18, 2025

## Mapping Strategy

1. **Direct Mapping:** XBRL tag → Compustat item (one-to-one)
2. **Variant Mapping:** Multiple XBRL tag variants → Same Compustat item
3. **Calculated Items:** Derived from multiple XBRL tags
4. **Tag Normalization:** Handle case, namespace, and naming variations

## Comprehensive Mapping Table

### Income Statement Items

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| REVTQ | RevenueFromContractWithCustomerExcludingAssessedTax, Revenues, RevenueFromContractWithCustomerIncludingAssessedTax | Revenue (quarterly) |
| SALEQ | SalesRevenueNet, SalesRevenueServicesNet, SalesRevenueGoodsNet | Sales (quarterly) |
| COGSQ | CostOfGoodsAndServicesSold, CostOfRevenue, CostOfSales, CostOfGoodsSold | Cost of Goods Sold |
| XSGAQ | SellingGeneralAndAdministrativeExpense, SellingAndMarketingExpense, GeneralAndAdministrativeExpense | SGA Expense |
| XRDQ | ResearchAndDevelopmentExpense, ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost | R&D Expense |
| XOPRQ | OperatingExpenses, OperatingCostsAndExpenses | Operating Expenses |
| XINTQ | InterestExpenseDebt, InterestExpense, InterestAndDebtExpense, InterestExpenseNet | Interest Expense |
| XIDOQ | InterestAndDividendIncomeOperating, InterestIncomeOperating | Interest/Dividend Income Operating |
| XIQ | InterestExpense | Interest Expense (alternative) |
| DPQ | DepreciationDepletionAndAmortization, DepreciationAndAmortization, Depreciation | Depreciation |
| DOQ | DepreciationOther | Depreciation Other |
| OIADPQ | OperatingIncomeLoss, IncomeLossFromOperations | Operating Income |
| OIBDPQ | OperatingIncomeLoss + DepreciationDepletionAndAmortization (calculated) | EBITDA |
| NOPIQ | OperatingIncomeLoss (net operating income) | Net Operating Income |
| PIQ | IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest, IncomeBeforeTax | Pretax Income |
| IBQ | IncomeBeforeExtraordinaryItems, IncomeBeforeExtraordinaryItemsAndDiscontinuedOperations | Income Before |
| IBCOMQ | IncomeBeforeExtraordinaryItemsCommonStockholders | Income Before Common |
| IBADJQ | IncomeBeforeExtraordinaryItemsAdjusted | Income Before Adjusted |
| NIQ | NetIncomeLoss, ProfitLoss, IncomeLossFromContinuingOperations, NetIncome | Net Income |
| TXTQ | IncomeTaxExpenseBenefit, ProvisionForIncomeTaxes, IncomeTaxExpense | Tax Expense |
| TXPQ | IncomeTaxPayable, TaxesPayable | Tax Payable |
| TXDIQ | DeferredIncomeTaxExpenseBenefit, DeferredTaxes | Tax Deferred Income |
| TXDITCQ | DeferredTaxAssetsNet, DeferredTaxLiabilitiesNet | Deferred Taxes |

### Balance Sheet - Assets

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| ATQ | Assets, AssetsTotal | Total Assets |
| ACTQ | AssetsCurrent, CurrentAssets | Current Assets |
| CHEQ | CashAndCashEquivalentsAtCarryingValue, Cash, CashCashEquivalentsAndShortTermInvestments | Cash and Equivalents |
| RECTQ | AccountsReceivableNetCurrent, AccountsReceivableNet, TradeAndOtherReceivables | Receivables |
| INVTQ | InventoryNet, Inventory | Inventory |
| PPENTQ | PropertyPlantAndEquipmentNet, PropertyPlantAndEquipment | PPE Net |
| INTANQ | IntangibleAssetsNetExcludingGoodwill, IntangibleAssetsNet | Intangible Assets |
| INTANOQ | IntangibleAssetsNetOther | Intangible Assets Other |
| IVSTQ | InvestmentsCurrent, ShortTermInvestments | Investments Short-term |
| IVLTQ | InvestmentsNoncurrent, LongTermInvestments | Investments Long-term |
| AOQ | AssetsOther | Assets Other |
| ACOQ | AssetsCurrentOther | Assets Current Other |
| ANCQ | AssetsNoncurrentOther | Assets Noncurrent Other |
| ALTOQ | AssetsLiabilitiesOther | Assets/Liabilities Other |
| WCAPQ | WorkingCapital | Working Capital |
| FCAQ | FixedAssetsCapitalized | Fixed Assets Capitalized |
| REQ | RetainedEarnings | Retained Earnings |
| REUNAQ | RetainedEarningsUnappropriated | Retained Earnings Unappropriated |

### Balance Sheet - Liabilities

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| LTQ | Liabilities, LiabilitiesTotal | Total Liabilities |
| LCTQ | LiabilitiesCurrent, CurrentLiabilities | Current Liabilities |
| APQ | AccountsPayableCurrent, AccountsPayable, TradeAndOtherPayables | Accounts Payable |
| DLCQ | DebtCurrent, ShortTermBorrowings, CommercialPaper, DebtCurrentAndCapitalLeaseObligations | Short-term Debt |
| DLTTQ | LongTermDebtAndCapitalLeaseObligations, LongTermDebt, DebtNoncurrent | Long-term Debt |
| LOQ | LiabilitiesOther | Liabilities Other |
| LCOQ | LiabilitiesCurrentOther | Liabilities Current Other |
| LLCQ | LeaseLiabilitiesCurrent, OperatingLeaseLiabilitiesCurrent | Lease Liabilities Current |
| LLLTQ | LeaseLiabilitiesNoncurrent, OperatingLeaseLiabilitiesNoncurrent | Lease Liabilities Long-term |
| LLTQ | LeaseLiabilities, OperatingLeaseLiabilities | Lease Liabilities Total |
| LOXDRQ | LiabilitiesOtherExcludingDerivatives | Liabilities Other Excluding Derivatives |
| LSEQ | LiabilitiesStockholdersEquity | Liabilities Stockholders Equity |
| LTMIBQ | LiabilitiesTotalMinusIncomeBefore | Liabilities Total Minus Income Before |
| MIBQ | MinorityInterestBalanceSheet | Minority Interest Balance Sheet |
| MIBTQ | MinorityInterestBalanceSheetTotal | Minority Interest Balance Sheet Total |

### Balance Sheet - Equity

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| CEQQ | StockholdersEquity, Equity, CommonStockholdersEquity, StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest | Common Equity |
| SEQQ | StockholdersEquity, ShareholdersEquity | Shareholders Equity |
| PSTKQ | PreferredStockValue, PreferredStock | Preferred Stock |
| PSTKRQ | PreferredStockRedeemable | Preferred Stock Redeemable |
| PSTKNQ | PreferredStockNonredeemable | Preferred Stock Nonredeemable |
| CSTKQ | CommonStockValue, CommonStock | Common Stock |
| CSTKEQ | CommonStockEquity | Common Stock Equity |
| CAPSQ | CapitalStock | Capital Stock |
| TSTKQ | TreasuryStock | Treasury Stock |
| MIIQ | NoncontrollingInterest, MinorityInterest | Minority Interest |
| ACOMINCQ | AccumulatedOtherComprehensiveIncome | Accumulated Other Comprehensive Income |
| DCOMQ | DeferredCompensation | Deferred Compensation |
| SEQOQ | StockholdersEquityOther | Stockholders Equity Other |
| ICAPTQ | InvestedCapital | Invested Capital |

### Shares and EPS

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| CSHOQ | EntityCommonStockSharesOutstanding, CommonStockSharesOutstanding | Common Shares Outstanding |
| CSHPRQ | WeightedAverageNumberOfSharesOutstandingBasic, WeightedAverageNumberOfSharesOutstanding | Shares Outstanding Basic |
| CSH12Q | WeightedAverageNumberOfSharesOutstandingBasic (12-month) | Shares Outstanding 12-month |
| CSHFDQ | WeightedAverageNumberOfSharesOutstandingDiluted | Shares Outstanding Fully Diluted |
| CSHFD12 | WeightedAverageNumberOfSharesOutstandingDiluted (12-month) | Shares Outstanding Fully Diluted 12-month |
| CSHIQ | SharesIssued | Shares Issued |
| EPSPXQ | EarningsPerShareBasic, EarningsPerShare | EPS Basic |
| EPSPIQ | EarningsPerShareDiluted | EPS Diluted |
| EPSFIQ | EarningsPerShareFullyDiluted | EPS Fully Diluted |
| EPSFXQ | EarningsPerShareFullyDiluted | EPS Fully Diluted |
| EPSX12 | EarningsPerShare (12-month) | EPS 12-month |
| EPSFI12 | EarningsPerShareFullyDiluted (12-month) | EPS Fully Diluted 12-month |
| EPSPI12 | EarningsPerShareDiluted (12-month) | EPS Diluted 12-month |
| OEPS12 | OperatingEarningsPerShare (12-month) | Operating EPS 12-month |
| OPEPSQ | OperatingEarningsPerShare | Operating EPS |
| OEPSXQ | OperatingEarningsPerSharePrimary | Operating EPS Primary |
| OEPF12 | OperatingEarningsPerShareFullyDiluted (12-month) | Operating EPS Fully Diluted 12-month |
| SPIQ | SalesPerShare | Sales Per Share |
| DILAVQ | DilutedAverageShares | Diluted Average Shares |
| DILADQ | DilutedAverageSharesDenominator | Diluted Average Shares Denominator |
| ESOPTQ | EmployeeStockOptionPlanShares | Employee Stock Option Plan Shares |
| ESOPCTQ | EmployeeStockOptionPlanSharesConvertible | Employee Stock Option Plan Shares Convertible |
| STKCOQ | StockCompensation | Stock Compensation |

### Cash Flow Items

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| OANCFQ | NetCashProvidedByUsedInOperatingActivities, CashFlowFromOperatingActivities | Operating Cash Flow |
| IVNCFQ | NetCashProvidedByUsedInInvestingActivities, CashFlowFromInvestingActivities | Investing Cash Flow |
| FINCFQ | NetCashProvidedByUsedInFinancingActivities, CashFlowFromFinancingActivities | Financing Cash Flow |
| CAPXQ | CapitalExpenditures, PaymentsToAcquirePropertyPlantAndEquipment | CapEx |
| CHQ | CashAndCashEquivalentsIncreaseDecrease | Cash Increase/Decrease |
| DRCQ | DepreciationReconciliation | Depreciation Reconciliation |
| DRLTQ | DepreciationReconciliationLongTerm | Depreciation Reconciliation Long-term |
| RDIPQ | ResearchAndDevelopmentExpenseInProcess | R&D Expense In Process |
| RDIPAQ | ResearchAndDevelopmentExpenseInProcessAcquired | R&D Expense In Process Acquired |
| ACCHGQ | AccountsReceivableChange | Accounts Receivable Change |
| DVPQ | DividendsPaid, DividendsPaidCommonStock | Dividends Paid |

### Other Items

| Compustat Item | XBRL Tag Variants | Description |
|----------------|-------------------|-------------|
| ROUANTQ | RightOfUseAssetNet | Right of Use Asset Net |

## Implementation Notes

1. **Tag Normalization:** Convert XBRL tags to lowercase, remove namespace prefixes
2. **Multiple Variants:** Try multiple tag variants, use first match
3. **Calculated Items:** OIBDPQ = OIADPQ + DPQ (if both available)
4. **Unit Conversion:** Handle millions, thousands, actuals
5. **Context Matching:** Match XBRL contexts to reporting periods

## Usage

```python
# Map XBRL tag to Compustat item
def map_xbrl_to_compustat(xbrl_tag: str) -> Optional[str]:
    # Normalize tag
    normalized = xbrl_tag.lower().replace('us-gaap:', '').replace(':', '_')
    
    # Look up in mapping table
    for compustat_item, variants in MAPPING_TABLE.items():
        for variant in variants:
            if normalized == variant.lower():
                return compustat_item
    
    return None
```

