"""
Post-transformation Silver quality checks.
These validate that transformations were applied correctly,
not just that the source data is valid.
"""

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite


def create_silver_post_transform_checks(spark, fraud_prevalence_bounds: tuple = (0.0, 1.0)):
    """
    Verify Silver transformations produced correct results.

    fraud_prevalence_bounds: expected (min, max) fraud rate for the specific
    data source being validated. silver.transactions is shared across sources
    with very different natural fraud rates (IEEE-CIS batch ~3.5%, Kaggle
    streaming replay ~0.17%) -- the (0.02, 0.06) range this was originally
    hardcoded to only holds for IEEE-CIS. Callers validating a specific
    source should pass that source's expected bounds; the wide (0.0, 1.0)
    default effectively skips the check rather than false-failing on data
    it was never calibrated for.
    """
    lo, hi = fraud_prevalence_bounds
    structural_check = (Check(spark, CheckLevel.Error, "silver_post_transform")
        .isComplete("transaction_id")
        .isUnique("transaction_id")
        .isComplete("event_time")
        .isComplete("product_cd")
        .isComplete("card4")
        .isComplete("card6")
        .isComplete("log_amount")
        .isNonNegative("log_amount")
        .isContainedIn("is_fraud", [0, 1])
    )
    # Fraud prevalence is a business/statistical signal that legitimately
    # varies by source and drifts over time -- Warning, not Error, so a real
    # (and expected) shift flags for review instead of hard-blocking the
    # pipeline, consistent with Phase 1's Error/Warning/Metric gate design.
    prevalence_check = (Check(spark, CheckLevel.Warning, "silver_fraud_prevalence")
        .satisfies("is_fraud = 1", "fraud_prevalence", lambda x: lo <= x <= hi)
    )
    return [structural_check, prevalence_check]
