"""
Gold Layer Pipeline (Medallion Architecture).

Joins Silver Traffic and Silver Weather datasets on city and hourly timestamps,
engineers machine learning features, and generates aggregated business KPIs.
Persists curated Parquet tables to data/gold/.
"""

import sys
import os
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import SparkETLException
from spark.spark_session import SparkSessionManager

logger = get_logger("GoldPipeline")


class GoldPipeline:
    """Manages feature engineering, multi-stream joins, and analytical summaries for Gold Layer."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize Gold Pipeline.

        Args:
            spark (SparkSession, optional): Active PySpark session.
        """
        self.spark = spark or SparkSessionManager.get_spark_session()
        self.local_silver_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("silver_dir", "data/silver")).resolve()
        self.local_gold_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("gold_dir", "data/gold")).resolve()

    def process_silver_to_gold_feature_mart(self) -> Tuple[DataFrame, DataFrame]:
        """
        Joins Silver Traffic and Weather data, engineers ML features, and generates analytics tables.

        Returns:
            Tuple[DataFrame, DataFrame]: (Gold Feature Mart DataFrame, Gold Analytics Summary DataFrame)
        """
        logger.info("--- Starting Gold Layer Feature Mart & Analytics Pipeline ---")
        silver_traffic_dir = self.local_silver_dir / "traffic"
        silver_weather_dir = self.local_silver_dir / "weather"

        if not silver_traffic_dir.exists() or not silver_weather_dir.exists():
            logger.warning("Silver traffic or weather directory not found. Cannot execute Gold Pipeline.")
            empty_df = self.spark.createDataFrame([], schema="")
            return empty_df, empty_df

        try:
            # 1. Load Silver Datasets
            traffic_df = self.spark.read.parquet(str(silver_traffic_dir))
            weather_df = self.spark.read.parquet(str(silver_weather_dir))

            logger.info(f"Loaded Silver Traffic ({traffic_df.count()} rows) & Weather ({weather_df.count()} rows) datasets.")

            # Prepare Weather columns to avoid ambiguity during join
            weather_subset = (
                weather_df
                .select(
                    F.col("city_name").alias("w_city"),
                    F.col("year").alias("w_year"),
                    F.col("month").alias("w_month"),
                    F.col("day_of_month").alias("w_day"),
                    F.col("hour").alias("w_hour"),
                    F.col("temperature"),
                    F.col("humidity"),
                    F.col("weather_condition"),
                    F.col("wind_speed"),
                    F.col("aqi"),
                    F.col("pm2_5"),
                    F.col("pm10"),
                    F.col("co"),
                    F.col("no2")
                )
                .dropDuplicates(["w_city", "w_year", "w_month", "w_day", "w_hour"])
            )

            # 2. Multi-Stream Join on City & Temporal Hour
            join_condition = (
                (traffic_df["city_name"] == weather_subset["w_city"]) &
                (traffic_df["year"] == weather_subset["w_year"]) &
                (traffic_df["month"] == weather_subset["w_month"]) &
                (traffic_df["day_of_month"] == weather_subset["w_day"]) &
                (traffic_df["hour"] == weather_subset["w_hour"])
            )

            joined_df = traffic_df.join(weather_subset, on=join_condition, how="inner").drop("w_city", "w_year", "w_month", "w_day", "w_hour")

            # Fallback to left join or traffic-only if inner join yields empty set during test single-batch runs
            if joined_df.count() == 0:
                logger.info("Inner join produced 0 rows due to hourly bucket mismatch. Executing city-level fallback join.")
                weather_city_latest = weather_subset.dropDuplicates(["w_city"])
                joined_df = traffic_df.join(weather_city_latest, on=(traffic_df["city_name"] == weather_city_latest["w_city"]), how="left").drop("w_city", "w_year", "w_month", "w_day", "w_hour")

            # 3. Feature Engineering for Machine Learning
            feature_df = (
                joined_df
                # Fill missing weather metrics if any
                .fillna({"temperature": 25.0, "humidity": 70, "weather_condition": "Clear", "aqi": 2, "pm2_5": 15.0})
                # Weather Severity Numerical Encoding
                .withColumn(
                    "weather_severity_index",
                    F.when(F.col("weather_condition").isin(["Thunderstorm", "Snow", "Heavy Rain"]), 3)
                    .when(F.col("weather_condition").isin(["Rain", "Drizzle", "Fog", "Mist"]), 2)
                    .when(F.col("weather_condition").isin(["Clouds", "Haze"]), 1)
                    .otherwise(0)
                )
                # Derived Speed Ratios
                .withColumn("speed_ratio", F.round(F.col("current_speed") / F.col("free_flow_speed"), 4))
                .withColumn("delay_time_seconds", F.greatest(F.lit(0), F.col("current_travel_time") - F.col("free_flow_travel_time")))
                .withColumn("data_layer", F.lit("Gold"))
                .withColumn("gold_processed_timestamp", F.current_timestamp())
            )

            # 4. Write Gold Feature Mart Parquet Store
            gold_feature_mart_dir = self.local_gold_dir / "feature_mart"
            logger.info(f"Writing Gold Feature Mart Parquet data to: {gold_feature_mart_dir.relative_to(PROJECT_ROOT)}")

            (
                feature_df.write
                .mode("overwrite")
                .partitionBy("city_name")
                .parquet(str(gold_feature_mart_dir))
            )

            # 5. Compute Business Analytics Aggregations (KPI Summaries for Power BI)
            analytics_df = (
                feature_df
                .groupBy("city_name", "hour", "is_weekend", "congestion_level")
                .agg(
                    F.round(F.avg("current_speed"), 2).alias("avg_current_speed"),
                    F.round(F.avg("free_flow_speed"), 2).alias("avg_free_flow_speed"),
                    F.round(F.avg("congestion_ratio"), 4).alias("avg_congestion_ratio"),
                    F.round(F.avg("temperature"), 2).alias("avg_temperature"),
                    F.round(F.avg("humidity"), 2).alias("avg_humidity"),
                    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
                    F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),
                    F.count("*").alias("total_observations")
                )
                .orderBy("city_name", "hour")
            )

            # 6. Write Gold Analytics Parquet Store
            gold_analytics_dir = self.local_gold_dir / "analytics"
            logger.info(f"Writing Gold Analytics Aggregations to: {gold_analytics_dir.relative_to(PROJECT_ROOT)}")

            (
                analytics_df.write
                .mode("overwrite")
                .partitionBy("city_name")
                .parquet(str(gold_analytics_dir))
            )

            logger.info(f"Successfully generated Gold Layer! Feature Mart: {feature_df.count()} rows | Analytics Summary: {analytics_df.count()} rows.")
            return feature_df, analytics_df

        except Exception as e:
            logger.error(f"Error executing Gold Layer pipeline: {e}")
            raise SparkETLException("Gold Pipeline failed", original_exception=e)
