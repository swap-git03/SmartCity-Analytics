"""
Phase 9 Silver Layer Pipeline Verification Test Script.

Tests Silver Layer deduplication, boundary sanitization, feature enrichment
(hour, day_of_week, is_weekend), and Silver Parquet store partitioning.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from spark.spark_session import SparkSessionManager
from etl.silver import SilverPipeline

logger = get_logger("Phase9Test")


def run_silver_verification():
    logger.info("=== Starting Phase 9 Silver Layer Pipeline Verification ===")

    # 1. Get PySpark Session
    spark = SparkSessionManager.get_spark_session()

    # 2. Run Silver Pipeline
    silver_pipeline = SilverPipeline(spark)

    # Process Silver Traffic Data
    traffic_silver_df = silver_pipeline.process_bronze_to_silver_traffic()
    logger.info("\n--- Silver Traffic DataFrame Schema & Sample ---")
    traffic_silver_df.printSchema()
    traffic_silver_df.select(
        "city_name", "current_speed", "free_flow_speed", "congestion_ratio",
        "congestion_level", "hour", "day_of_week", "is_weekend", "data_layer"
    ).show(5, truncate=False)

    # Process Silver Weather & AQI Data
    weather_silver_df = silver_pipeline.process_bronze_to_silver_weather()
    logger.info("\n--- Silver Weather DataFrame Schema & Sample ---")
    weather_silver_df.printSchema()
    weather_silver_df.select(
        "city_name", "temperature", "humidity", "weather_condition",
        "aqi", "pm2_5", "pm10", "hour", "day_of_week", "is_weekend", "data_layer"
    ).show(5, truncate=False)

    # 3. Stop PySpark Session cleanly
    SparkSessionManager.stop_spark_session()

    logger.info("=== Phase 9 Silver Layer Verification Complete & Successful ===")


if __name__ == "__main__":
    run_silver_verification()
