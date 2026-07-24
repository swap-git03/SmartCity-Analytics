"""
Silver Layer Pipeline (Medallion Architecture).

Cleanses, deduplicates, validates boundaries, and enriches Bronze datasets with temporal features.
Persists standardized clean Parquet files to data/silver/.
"""

import sys
import os
from pathlib import Path
from typing import Optional

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

logger = get_logger("SilverPipeline")


class SilverPipeline:
    """Manages data cleansing, deduplication, and standardization for the Silver Layer."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize Silver Pipeline.

        Args:
            spark (SparkSession, optional): Active PySpark session.
        """
        self.spark = spark or SparkSessionManager.get_spark_session()
        self.local_bronze_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("bronze_dir", "data/bronze")).resolve()
        self.local_silver_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("silver_dir", "data/silver")).resolve()

    def process_bronze_to_silver_traffic(self) -> DataFrame:
        """
        Cleanses Bronze traffic Parquet/JSON data into Silver Traffic Layer.

        Returns:
            DataFrame: Cleaned PySpark Silver Traffic DataFrame.
        """
        logger.info("--- Starting Silver Layer Processing for Traffic Data ---")
        topic = settings.KAFKA_TOPICS.get("traffic_raw", "traffic-raw-events")
        bronze_parquet_dir = self.local_bronze_dir / "parquet" / topic

        if not bronze_parquet_dir.exists():
            logger.warning(f"Bronze traffic directory not found at {bronze_parquet_dir}. Cannot execute Silver Traffic pipeline.")
            return self.spark.createDataFrame([], schema="")

        try:
            # 1. Read Bronze Traffic Parquet Data
            bronze_df = self.spark.read.parquet(str(bronze_parquet_dir))
            initial_count = bronze_df.count()
            logger.info(f"Read {initial_count} records from Bronze Traffic Parquet store.")

            # 2. Deduplication on composite key (city_name, timestamp)
            dedup_df = bronze_df.dropDuplicates(["city_name", "timestamp"])

            # 3. Data Cleansing & Boundary Validation
            clean_df = (
                dedup_df
                # Drop rows with null essential attributes
                .dropna(subset=["city_name", "timestamp", "current_speed", "free_flow_speed"])
                # Filter outlier speed values
                .filter(F.col("current_speed") >= 0.0)
                .filter(F.col("free_flow_speed") > 0.0)
                # Parse timestamp
                .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
            )

            # 4. Temporal Feature Enrichment
            silver_df = (
                clean_df
                .withColumn("hour", F.hour(F.col("event_timestamp")))
                .withColumn("day_of_week", F.dayofweek(F.col("event_timestamp")))
                .withColumn("day_of_month", F.dayofmonth(F.col("event_timestamp")))
                .withColumn("month", F.month(F.col("event_timestamp")))
                .withColumn("year", F.year(F.col("event_timestamp")))
                .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
                .withColumn("processed_timestamp", F.current_timestamp())
                .withColumn("data_layer", F.lit("Silver"))
            )

            # 5. Write to Silver Parquet Store partitioned by city_name
            output_dir = self.local_silver_dir / "traffic"
            logger.info(f"Writing Silver Traffic Parquet data to: {output_dir.relative_to(PROJECT_ROOT)}")

            (
                silver_df.write
                .mode("overwrite")
                .partitionBy("city_name")
                .parquet(str(output_dir))
            )

            final_count = silver_df.count()
            logger.info(f"Successfully processed Silver Traffic Layer: {final_count} clean records (Deduplicated: {initial_count - final_count} duplicates).")
            return silver_df

        except Exception as e:
            logger.error(f"Error processing Silver Traffic pipeline: {e}")
            raise SparkETLException("Silver Traffic pipeline failed", original_exception=e)

    def process_bronze_to_silver_weather(self) -> DataFrame:
        """
        Cleanses Bronze weather/AQI Parquet/JSON data into Silver Weather Layer.

        Returns:
            DataFrame: Cleaned PySpark Silver Weather DataFrame.
        """
        logger.info("--- Starting Silver Layer Processing for Weather & AQI Data ---")
        topic = settings.KAFKA_TOPICS.get("weather_raw", "weather-raw-events")
        bronze_parquet_dir = self.local_bronze_dir / "parquet" / topic

        if not bronze_parquet_dir.exists():
            logger.warning(f"Bronze weather directory not found at {bronze_parquet_dir}. Cannot execute Silver Weather pipeline.")
            return self.spark.createDataFrame([], schema="")

        try:
            # 1. Read Bronze Weather Parquet Data
            bronze_df = self.spark.read.parquet(str(bronze_parquet_dir))
            initial_count = bronze_df.count()
            logger.info(f"Read {initial_count} records from Bronze Weather Parquet store.")

            # 2. Deduplication on composite key (city_name, timestamp)
            dedup_df = bronze_df.dropDuplicates(["city_name", "timestamp"])

            # 3. Data Cleansing & Boundary Validation
            clean_df = (
                dedup_df
                # Drop rows with null essential attributes
                .dropna(subset=["city_name", "timestamp", "temperature", "aqi"])
                # Filter valid meteorological & air quality boundaries
                .filter((F.col("temperature") >= -50.0) & (F.col("temperature") <= 60.0))
                .filter((F.col("humidity") >= 0) & (F.col("humidity") <= 100))
                .filter((F.col("aqi") >= 1) & (F.col("aqi") <= 5))
                .filter(F.col("pm2_5") >= 0.0)
                # Parse timestamp
                .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
            )

            # 4. Temporal Feature Enrichment
            silver_df = (
                clean_df
                .withColumn("hour", F.hour(F.col("event_timestamp")))
                .withColumn("day_of_week", F.dayofweek(F.col("event_timestamp")))
                .withColumn("day_of_month", F.dayofmonth(F.col("event_timestamp")))
                .withColumn("month", F.month(F.col("event_timestamp")))
                .withColumn("year", F.year(F.col("event_timestamp")))
                .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
                .withColumn("processed_timestamp", F.current_timestamp())
                .withColumn("data_layer", F.lit("Silver"))
            )

            # 5. Write to Silver Parquet Store partitioned by city_name
            output_dir = self.local_silver_dir / "weather"
            logger.info(f"Writing Silver Weather Parquet data to: {output_dir.relative_to(PROJECT_ROOT)}")

            (
                silver_df.write
                .mode("overwrite")
                .partitionBy("city_name")
                .parquet(str(output_dir))
            )

            final_count = silver_df.count()
            logger.info(f"Successfully processed Silver Weather Layer: {final_count} clean records (Deduplicated: {initial_count - final_count} duplicates).")
            return silver_df

        except Exception as e:
            logger.error(f"Error processing Silver Weather pipeline: {e}")
            raise SparkETLException("Silver Weather pipeline failed", original_exception=e)
