"""
Geo-Velocity Calculations: Haversine Distance, Implied Speed, Impossible Travel.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, radians, sin, cos, atan2, sqrt, lit, when, unix_timestamp, lag
)
from pyspark.sql.window import Window

IMPOSSIBLE_TRAVEL_SPEED_KMH = 900.0
EARTH_RADIUS_KM = 6371.0
MIN_TIME_DELTA_HOURS = 0.001


def haversine_distance_expr(lat1_col, lon1_col, lat2_col, lon2_col):
    """Returns PySpark Column expression for Haversine distance in kilometers."""
    dlat = radians(lat2_col - lat1_col)
    dlon = radians(lon2_col - lon1_col)

    a = (sin(dlat / 2.0) ** 2) + \
        cos(radians(lat1_col)) * cos(radians(lat2_col)) * (sin(dlon / 2.0) ** 2)

    c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a))
    return EARTH_RADIUS_KM * c


def compute_geo_velocity_batch(df: DataFrame) -> DataFrame:
    """
    Computes geo-velocity features across historical/batch dataset.
    Uses window lag() over (card_id, event_time_ts).
    """
    window_spec = Window.partitionBy("card_id").orderBy("event_time_ts")

    df_with_prev = (
        df
        .withColumn("prev_lat", lag("latitude", 1).over(window_spec))
        .withColumn("prev_lon", lag("longitude", 1).over(window_spec))
        .withColumn("prev_time", lag("event_time_ts", 1).over(window_spec))
    )

    df_geo = (
        df_with_prev
        .withColumn(
            "geo_dist_km",
            when(
                col("prev_lat").isNotNull() & col("latitude").isNotNull(),
                haversine_distance_expr(
                    col("prev_lat"), col("prev_lon"),
                    col("latitude"), col("longitude")
                )
            ).otherwise(lit(0.0))
        )
        .withColumn(
            "time_delta_hours",
            when(
                col("prev_time").isNotNull(),
                (unix_timestamp("event_time_ts") - unix_timestamp("prev_time")) / 3600.0
            ).otherwise(lit(0.0))
        )
        .withColumn(
            "geo_implied_speed_kmh",
            when(
                col("time_delta_hours") > MIN_TIME_DELTA_HOURS,
                col("geo_dist_km") / col("time_delta_hours")
            ).otherwise(lit(0.0))
        )
        .withColumn(
            "geo_flag_impossible_travel",
            when(col("geo_implied_speed_kmh") > IMPOSSIBLE_TRAVEL_SPEED_KMH, 1).otherwise(0)
        )
    )
    return df_geo
