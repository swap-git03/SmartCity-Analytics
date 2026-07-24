"""
Singleton PySpark Session Builder.

Configures PySpark session with Apache Kafka streaming packages, AWS S3 Hadoop connectors,
JVM compatibility options for Java 17/21+, and Windows HADOOP_HOME/PATH native DLL resolution.
"""

import sys
import os
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Setup local HADOOP_HOME & PATH environment variables for native Windows DLL resolution
HADOOP_DIR = (PROJECT_ROOT / "data" / "hadoop").resolve()
HADOOP_BIN = HADOOP_DIR / "bin"
HADOOP_BIN.mkdir(parents=True, exist_ok=True)
HADOOP_PATH_STR = HADOOP_DIR.as_posix()

os.environ["HADOOP_HOME"] = HADOOP_PATH_STR
if str(HADOOP_BIN) not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"{HADOOP_BIN};{os.environ.get('PATH', '')}"

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import SparkETLException

logger = get_logger("SparkSession")


class SparkSessionManager:
    """Singleton manager providing thread-safe PySpark session creation."""
    
    _spark_session: Optional[SparkSession] = None

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        """
        Retrieves or initializes the PySpark Session singleton.

        Returns:
            SparkSession: Active PySpark session configured for Kafka and S3.
        """
        if cls._spark_session is None:
            logger.info("Initializing PySpark Session with Kafka, AWS S3 connectors, and Windows Hadoop compatibility...")
            
            spark_config = settings.SPARK_CONFIG
            app_name = spark_config.get("app_name", "SmartCityStreamingETL")
            master = spark_config.get("master", "local[*]")
            driver_memory = spark_config.get("driver_memory", "2g")
            executor_memory = spark_config.get("executor_memory", "2g")

            # Maven dependencies for Kafka Structured Streaming and AWS S3 Aether/Hadoop
            kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            aws_hadoop_package = "org.apache.hadoop:hadoop-aws:3.3.4"
            aws_sdk_package = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

            packages = f"{kafka_package},{aws_hadoop_package},{aws_sdk_package}"

            hadoop_java_opt = f"-Dhadoop.home.dir={HADOOP_PATH_STR}"
            extra_java_opts = f"-Djava.security.manager=allow {hadoop_java_opt}"

            try:
                builder = (
                    SparkSession.builder
                    .appName(app_name)
                    .master(master)
                    .config("spark.driver.memory", driver_memory)
                    .config("spark.executor.memory", executor_memory)
                    .config("spark.jars.packages", packages)
                    # JVM Java 17/21+ & Windows Compatibility Flags
                    .config("spark.driver.extraJavaOptions", extra_java_opts)
                    .config("spark.executor.extraJavaOptions", extra_java_opts)
                    # AWS S3A Configuration
                    .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
                    .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
                    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.AWS_REGION}.amazonaws.com")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.sql.shuffle.partitions", "4")  # Optimized partition count for local dev
                )

                cls._spark_session = builder.getOrCreate()
                
                # Reduce noisy Spark internal logging
                cls._spark_session.sparkContext.setLogLevel("WARN")
                logger.info(f"PySpark Session '{app_name}' created successfully! Spark Version: {cls._spark_session.version}")

            except Exception as e:
                logger.error(f"Failed to initialize PySpark Session: {e}")
                raise SparkETLException("PySpark Session creation failed", original_exception=e)

        return cls._spark_session

    @classmethod
    def stop_spark_session(cls):
        """Stops the active PySpark session."""
        if cls._spark_session is not None:
            logger.info("Stopping active PySpark Session...")
            cls._spark_session.stop()
            cls._spark_session = None
            logger.info("PySpark Session stopped.")
