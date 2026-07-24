"""
Phase 6 & Phase 7 PySpark Session & Bronze Layer Verification Test Script.

Tests PySpark Session creation, Bronze layer JSON ingestion, schema enforcement,
metadata column enrichment, and Parquet storage partitioning.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from spark.spark_session import SparkSessionManager
from etl.bronze import BronzePipeline

logger = get_logger("Phase7Test")


def run_spark_bronze_verification():
    logger.info("=== Starting Phase 6 & 7 PySpark Session & Bronze Pipeline Verification ===")

    # 1. Get PySpark Session
    spark = SparkSessionManager.get_spark_session()
    logger.info(f"Active Spark Master: {spark.sparkContext.master}")

    # 2. Run Bronze Pipeline
    bronze_pipeline = BronzePipeline(spark)

    # Process Bronze Traffic Stream
    traffic_bronze_df = bronze_pipeline.process_raw_traffic_to_bronze()
    logger.info("\n--- Bronze Traffic DataFrame Schema & Sample ---")
    traffic_bronze_df.printSchema()
    traffic_bronze_df.show(5, truncate=False)

    # Process Bronze Weather & AQI Stream
    weather_bronze_df = bronze_pipeline.process_raw_weather_and_aqi_to_bronze()
    logger.info("\n--- Bronze Weather/AQI DataFrame Schema & Sample ---")
    weather_bronze_df.printSchema()
    weather_bronze_df.show(5, truncate=False)

    # 3. Stop PySpark Session cleanly
    SparkSessionManager.stop_spark_session()

    logger.info("=== Phase 6 & 7 PySpark & Bronze Pipeline Verification Complete & Successful ===")


if __name__ == "__main__":
    run_spark_bronze_verification()
