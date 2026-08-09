"""
Bronze → Silver data quality gate.
Defines hard constraints (Error), soft constraints (Warning), and metrics.

Production decisions:
- Error constraints reject rows → quarantine
- Warning constraints flag rows but allow passage
- Metrics are logged to MLflow/Log Analytics for trend monitoring
"""

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import (
    AnalysisRunner, Size, Completeness, Uniqueness, 
    Mean, StandardDeviation, Minimum, Maximum,
    CountDistinct
)


def create_transaction_error_checks(spark):
    """Hard constraints — failure means row goes to quarantine."""
    return (Check(spark, CheckLevel.Error, "bronze_txn_hard_gate")
        .isComplete("TransactionID")
        .isUnique("TransactionID")
        .isNonNegative("TransactionAmt")
        .satisfies("TransactionAmt > 0", "amount_positive", lambda x: x >= 0.999)
        .isComplete("TransactionDT")
        .isNonNegative("TransactionDT")
        .isContainedIn("ProductCD", ["W", "H", "C", "S", "R"])
        .isContainedIn("isFraud", ["0", "1", 0, 1])
    )


def create_transaction_warning_checks(spark):
    """Soft constraints — failure means row is flagged but not rejected."""
    return (Check(spark, CheckLevel.Warning, "bronze_txn_soft_gate")
        .satisfies("TransactionAmt < 50000", "amount_upper_bound", lambda x: x >= 0.999)
        .hasCompleteness("card1", lambda x: x >= 0.99)
        .hasCompleteness("card4", lambda x: x >= 0.98)
        .hasCompleteness("card6", lambda x: x >= 0.98)
        .isContainedIn("card4", ["visa", "mastercard", "american express", "discover"], lambda x: x >= 0.99)
        .isContainedIn("card6", ["debit", "credit", "debit or credit", "charge card"], lambda x: x >= 0.99)
        .hasCompleteness("addr1", lambda x: x >= 0.85)
        .hasCompleteness("addr2", lambda x: x >= 0.95)
    )


def create_identity_error_checks(spark):
    """Hard constraints for identity table."""
    return (Check(spark, CheckLevel.Error, "bronze_identity_hard_gate")
        .isComplete("TransactionID")
        .isUnique("TransactionID")
    )


def run_data_profiling(spark, df, table_name):
    """
    Run statistical profiling on the data.
    Results are logged to MLflow for trend monitoring across loads.
    """
    analysis_result = (AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("TransactionID"))
        .addAnalyzer(Completeness("TransactionAmt"))
        .addAnalyzer(Mean("TransactionAmt"))
        .addAnalyzer(StandardDeviation("TransactionAmt"))
        .addAnalyzer(Minimum("TransactionAmt"))
        .addAnalyzer(Maximum("TransactionAmt"))
        .addAnalyzer(CountDistinct("ProductCD"))
        .addAnalyzer(Completeness("card1"))
        .addAnalyzer(Completeness("P_emaildomain"))
        .run()
    )
    return analysis_result
