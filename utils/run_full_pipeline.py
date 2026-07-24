"""
UrbanPulse End-to-End Mumbai Smart Mobility Pipeline Automation Script.

Orchestrates the entire data engineering lifecycle for Mumbai 15 Junctions:
API Ingestion -> Kafka Streaming -> PySpark Bronze -> Silver -> Gold -> AWS S3 Cloud Sync -> ML Training -> Power BI Export.
"""

import sys
import time
from pathlib import Path

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from kafka.city_producer import CityDataProducer
from spark.spark_session import SparkSessionManager
from etl.bronze import BronzePipeline
from etl.silver import SilverPipeline
from etl.gold import GoldPipeline
from aws.s3_uploader import S3Uploader
from ml.train_models import TrafficMLPipeline
from powerbi.export_powerbi_data import export_powerbi_datasets

logger = get_logger("UrbanPulseOrchestrator")


def run_full_pipeline():
    start_time = time.time()
    logger.info("==========================================================================")
    logger.info("=== STARTING URBANPULSE MUMBAI SMART MOBILITY & ENVIRONMENTAL PIPELINE ===")
    logger.info("==========================================================================")

    # 1. Live API Data Fetch & Kafka Event Streaming
    logger.info("\n>>> STEP 1 & 2: Ingesting Live APIs & Streaming 15 Mumbai Locations to Kafka...")
    producer = CityDataProducer()
    producer.poll_and_publish_single_burst()

    # 2. Initialize PySpark Session
    logger.info("\n>>> STEP 3: Initializing PySpark Session Engine...")
    spark = SparkSessionManager.get_spark_session()

    # 3. Medallion Bronze Layer Processing
    logger.info("\n>>> STEP 4: Executing PySpark Bronze Medallion Layer...")
    bronze_pipeline = BronzePipeline(spark)
    traffic_bronze_df = bronze_pipeline.process_raw_traffic_to_bronze()
    weather_bronze_df = bronze_pipeline.process_raw_weather_and_aqi_to_bronze()

    # 4. Medallion Silver Layer Processing
    logger.info("\n>>> STEP 5: Executing PySpark Silver Medallion Layer (Data Cleansing & Features)...")
    silver_pipeline = SilverPipeline(spark)
    traffic_silver_df = silver_pipeline.process_bronze_to_silver_traffic()
    weather_silver_df = silver_pipeline.process_bronze_to_silver_weather()

    # 5. Medallion Gold Layer Processing
    logger.info("\n>>> STEP 6: Executing PySpark Gold Medallion Layer (Feature Mart & Analytics)...")
    gold_pipeline = GoldPipeline(spark)
    feature_mart_df, analytics_df = gold_pipeline.process_silver_to_gold_feature_mart()

    # Stop PySpark session cleanly before S3 upload
    SparkSessionManager.stop_spark_session()

    # 6. AWS S3 Cloud Synchronization
    logger.info("\n>>> STEP 7: Synchronizing Medallion Layers to AWS S3 Cloud Storage...")
    uploader = S3Uploader()
    uploader.sync_directory(PROJECT_ROOT / "data" / "bronze", "bronze")
    uploader.sync_directory(PROJECT_ROOT / "data" / "silver", "silver")
    uploader.sync_directory(PROJECT_ROOT / "data" / "gold", "gold")

    # 7. Machine Learning Pipeline (Model 1 Random Forest Speed + Model 2 XGBoost AQI)
    logger.info("\n>>> STEP 8: Training Machine Learning Models & Performing Inference...")
    ml_pipeline = TrafficMLPipeline()
    df_gold = ml_pipeline.load_gold_data()
    ml_pipeline.train_speed_regressor(df_gold)
    ml_pipeline.train_aqi_regressor(df_gold)

    # 8. Power BI Data Export
    logger.info("\n>>> STEP 9: Exporting Gold Datasets to Power BI CSV Format...")
    export_powerbi_datasets()

    total_duration = round(time.time() - start_time, 2)
    logger.info("==========================================================================")
    logger.info(f"=== URBANPULSE PIPELINE EXECUTED SUCCESSFULLY IN {total_duration} SECONDS ===")
    logger.info("==========================================================================")


if __name__ == "__main__":
    run_full_pipeline()
