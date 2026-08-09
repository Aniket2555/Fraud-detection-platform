"""
Point-in-Time Feature Retrieval Engine.
Joins observation dataset with feature tables without temporal data leakage.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, row_number
from pyspark.sql.window import Window


def point_in_time_feature_join(
    observation_df: DataFrame,
    feature_df: DataFrame,
    entity_key: str,
    obs_time_col: str = "event_time_ts",
    feat_time_col: str = "feature_timestamp",
    max_lookback_seconds: int = 86400
) -> DataFrame:
    """
    Performs As-Of / Point-in-Time join between observation DataFrame and Feature DataFrame.
    Guarantees feature_timestamp <= observation_timestamp (STRICT temporal ordering).

    The feature-side entity_key and feat_time_col columns are dropped from
    the result (the obs-side copies are retained and are identical by the
    join condition). Without this, chaining multiple PIT joins via
    multi_entity_pit_join -- which production code does, reusing entity_key
    "card_id" for both card_velocity and geo_velocity in the same chain, and
    every feature table conventionally naming its timestamp column
    "feature_timestamp" -- leaves two identically-named columns in the
    result. The next join in the chain then fails with
    AnalysisException: AMBIGUOUS_REFERENCE when it re-aliases the whole
    accumulated frame as "obs" and looks up entity_key/obs_time_col.
    """
    obs_alias = observation_df.alias("obs")
    feat_alias = feature_df.alias("feat")

    join_cond = [
        col(f"obs.{entity_key}") == col(f"feat.{entity_key}"),
        col(f"feat.{feat_time_col}") <= col(f"obs.{obs_time_col}"),
        col(f"feat.{feat_time_col}") >= (col(f"obs.{obs_time_col}") - expr(f"INTERVAL {max_lookback_seconds} SECONDS"))
    ]

    joined_df = obs_alias.join(feat_alias, join_cond, "left")

    window_spec = Window.partitionBy(
        col("obs.transaction_id")
    ).orderBy(col(f"feat.{feat_time_col}").desc())

    result_df = (
        joined_df
        .withColumn("_pit_rank", row_number().over(window_spec))
        .filter(col("_pit_rank") == 1)
        .drop("_pit_rank")
        .drop(feat_alias[entity_key])
        .drop(feat_alias[feat_time_col])
    )

    return result_df


def multi_entity_pit_join(
    observation_df: DataFrame,
    feature_table_specs: list
) -> DataFrame:
    """
    Chains multiple PIT joins for different entity keys (card_id, customer_id, merchant_id).

    Each spec is either (feature_df, entity_key) -- using the default 86400s
    (1 day) lookback, appropriate for the 5m/1h/24h velocity and geo windows
    -- or (feature_df, entity_key, max_lookback_seconds) for feature families
    refreshed on a slower cadence (e.g. daily behavioral baselines or
    merchant risk, which need a longer lookback to tolerate a missed run).
    """
    result = observation_df
    for spec in feature_table_specs:
        if len(spec) == 3:
            feature_df, entity_key, max_lookback_seconds = spec
        else:
            feature_df, entity_key = spec
            max_lookback_seconds = 86400
        result = point_in_time_feature_join(
            observation_df=result,
            feature_df=feature_df,
            entity_key=entity_key,
            max_lookback_seconds=max_lookback_seconds
        )
    return result
