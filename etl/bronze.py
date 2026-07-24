"""
Bronze Layer Pipeline (Medallion Architecture).

Ingests raw streaming JSON files/buffers from Kafka topics or local fallback storage,
adds ingestion metadata, and converts raw payloads to partitioned Bronze Parquet datasets.
"""

import sys
import os
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import SparkETLException
from spark.spark_session import SparkSessionManager

logger = get_logger("BronzePipeline")


class BronzePipeline:
    """Manages raw ingestion into the Medallion Bronze Layer."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize Bronze Pipeline.

        Args:
            spark (SparkSession, optional): Active PySpark session.
        """
        self.spark = spark or SparkSessionManager.get_spark_session()
        self.local_bronze_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("bronze_dir", "data/bronze")).resolve()

    def process_raw_traffic_to_bronze(self) -> DataFrame:
        """
        Ingests raw traffic JSON payloads, enriches with Bronze metadata, and writes to Bronze Parquet.

        Returns:
            DataFrame: PySpark Bronze Traffic DataFrame.
        """
        logger.info("--- Starting Bronze Processing for Traffic Stream ---")
        topic = settings.KAFKA_TOPICS.get("traffic_raw", "traffic-raw-events")
        input_dir = self.local_bronze_dir / topic

        if not input_dir.exists() or not list(input_dir.glob("*.json")):
            logger.warning(f"No Bronze traffic JSON files found in {input_dir}. Cannot process Traffic Bronze Layer.")
            return self.spark.createDataFrame([], schema=self.get_traffic_schema())

        try:
            # Read raw JSON files from Bronze Landing Zone
            raw_df = self.spark.read.option("multiline", "true").json(str(input_dir / "*.json"))

            # Add Bronze Layer Metadata Columns
            bronze_df = (
                raw_df
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("data_layer", F.lit("Bronze"))
            )

            # Write Parquet to Bronze Parquet Store partitioned by city_name
            output_dir = self.local_bronze_dir / "parquet" / topic
            logger.info(f"Writing Bronze Traffic Parquet data to: {output_dir.relative_to(PROJECT_ROOT)}")
            
            (
                bronze_df.write
                .mode("overwrite")
                .partitionBy("city_name")
                .parquet(str(output_dir))
            )

            logger.info(f"Successfully processed {bronze_df.count()} Bronze Traffic records!")
            return bronze_df

        except Exception as e:
            logger.error(f"Error processing Bronze Traffic stream: {e}")
            raise SparkETLException("Bronze Traffic processing failed", original_exception=e)

    def process_raw_weather_and_aqi_to_bronze(self) -> DataFrame:
        """
        Ingests raw weather/AQI JSON payloads, enriches with Bronze metadata, and writes to Bronze Parquet.

        Returns:
            DataFrame: PySpark Bronze Weather/AQI DataFrame.
        """
        logger.info("--- Starting Bronze Processing for Weather & AQI Stream ---")
        topic = settings.KAFKA_TOPICS.get("weather_raw", "weather-raw-events")
        input_dir = self.local_bronze_dir / topic

        if not input_dir.exists() or not list(input_dir.glob("*.json")):
            logger.warning(f"No Bronze weather JSON files found in {input_dir}. Cannot process Weather Bronze Layer.")
            return self.spark.createDataFrame([], schema=self.get_weather_schema())

        try:
            # Read raw JSON files from Bronze Landing Zone
            raw_df = self.spark.read.option("multiline", "true").json(str(input_dir / "*.json"))

            # Add Bronze Layer Metadata Columns
            bronze_df = (
                raw_df
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("data_layer", F.lit("Bronze"))
            )

            # Write Parquet to Bronze Parquet Store partitioned by city_name
            output_dir = self.local_bronze_dir / "parquet" / topic
            logger.info(f"Writing Bronze Weather Parquet data to: {output_dir.relative_to(PROJECT_ROOT)}")

            (
                bronze_df.write
                .mode("overwrite")
                .partitionBy("city_name")
                .parquet(str(output_dir))
            )

            logger.info(f"Successfully processed {bronze_df.count()} Bronze Weather records!")
            return bronze_df

        except Exception as e:
            logger.error(f"Error processing Bronze Weather stream: {e}")
            raise SparkETLException("Bronze Weather processing failed", original_exception=e)

    @staticmethod
    def get_traffic_schema() -> StructType:
        """Defines explicit PySpark StructType Schema for Raw Traffic events."""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("current_speed", DoubleType(), True),
            StructField("free_flow_speed", DoubleType(), True),
            StructField("current_travel_time", IntegerType(), True),
            StructField("free_flow_travel_time", IntegerType(), True),
            StructField("congestion_ratio", DoubleType(), True),
            StructField("congestion_level", StringType(), True),
            StructField("confidence", DoubleType(), True),
            StructField("road_closure", BooleanType(), True),
            StructField("data_source", StringType(), True)
        ])

    @staticmethod
    def get_weather_schema() -> StructType:
        """Defines explicit PySpark StructType Schema for Raw Weather & AQI events."""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("weather_condition", StringType(), True),
            StructField("aqi", IntegerType(), True),
            StructField("co", DoubleType(), True),
            StructField("no2", DoubleType(), True),
            StructField("o3", DoubleType(), True),
            StructField("so2", DoubleType(), True),
            StructField("pm2_5", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("data_source", StringType(), True)
        ])
