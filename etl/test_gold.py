"""
Phase 10 Gold Layer Pipeline Verification Test Script.

Tests Silver Traffic & Weather joins, feature engineering (speed_ratio, weather_severity_index),
analytical aggregations (city_hourly_summary), and Gold Parquet storage.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from spark.spark_session import SparkSessionManager
from etl.gold import GoldPipeline

logger = get_logger("Phase10Test")


def run_gold_verification():
    logger.info("=== Starting Phase 10 Gold Layer Pipeline Verification ===")

    # 1. Get PySpark Session
    spark = SparkSessionManager.get_spark_session()

    # 2. Run Gold Pipeline
    gold_pipeline = GoldPipeline(spark)
    feature_mart_df, analytics_df = gold_pipeline.process_silver_to_gold_feature_mart()

    logger.info("\n--- Gold Feature Mart DataFrame Schema & Sample ---")
    feature_mart_df.printSchema()
    feature_mart_df.select(
        "city_name", "current_speed", "free_flow_speed", "speed_ratio", "delay_time_seconds",
        "weather_condition", "weather_severity_index", "aqi", "pm2_5", "congestion_level", "data_layer"
    ).show(5, truncate=False)

    logger.info("\n--- Gold Business Analytics KPI Aggregation Sample ---")
    analytics_df.printSchema()
    analytics_df.show(10, truncate=False)

    # 3. Stop PySpark Session cleanly
    SparkSessionManager.stop_spark_session()

    logger.info("=== Phase 10 Gold Layer Verification Complete & Successful ===")


if __name__ == "__main__":
    run_gold_verification()
