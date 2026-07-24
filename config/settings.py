"""
UrbanPulse Configuration Settings Loader Module.

Merges YAML configuration (config/config.yaml) with environment variables (.env)
providing a clean, centralized settings interface for UrbanPulse Mumbai Smart Mobility Platform.
"""

import os
from pathlib import Path
import yaml
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.exceptions import TrafficPlatformException

logger = get_logger("ConfigSettings")

# Resolve Base Project Directory
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
CONFIG_YAML_PATH = BASE_DIR / "config" / "config.yaml"

# Load environment variables from .env if present
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
    logger.info(f"Loaded environment variables from: {ENV_PATH}")
else:
    logger.warning(f".env file not found at {ENV_PATH}. Falling back to system environment variables.")


class Settings:
    """Singleton class managing application settings and environment secrets."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Loads config.yaml and environment variables into memory."""
        if not CONFIG_YAML_PATH.exists():
            raise TrafficPlatformException(f"Configuration file not found at: {CONFIG_YAML_PATH}")

        try:
            with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
                self._yaml_data = yaml.safe_load(f)
        except Exception as e:
            raise TrafficPlatformException("Failed to parse config.yaml", original_exception=e)

        # 1. API Keys & Secrets from .env
        self.TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
        self.OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        self.AWS_REGION = os.getenv("AWS_REGION", self._yaml_data.get("aws", {}).get("region", "ap-south-1"))
        self.AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", self._yaml_data.get("aws", {}).get("s3_bucket", "smartcity-traffic-analytics-swapnil"))

        # 2. Kafka Configuration
        env_kafka = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.KAFKA_BOOTSTRAP_SERVERS = env_kafka if env_kafka else self._yaml_data.get("kafka", {}).get("bootstrap_servers", "13.217.6.185:9092")
        self.KAFKA_TOPICS = self._yaml_data.get("kafka", {}).get("topics", {})

        # 3. Application & Location Specs (UrbanPulse Mumbai Locations)
        self.APP_NAME = self._yaml_data.get("app", {}).get("name", "UrbanPulse")
        self.APP_TITLE = self._yaml_data.get("app", {}).get("title", "Mumbai Smart Mobility & Environmental Analytics Platform")
        self.LOCATIONS = self._yaml_data.get("locations", [])
        # Alias CITIES to LOCATIONS for backward compatibility
        self.CITIES = self.LOCATIONS

        # 4. API Specific Configs
        self.TOMTOM_CONFIG = self._yaml_data.get("api", {}).get("tomtom", {})
        self.OPENWEATHER_CONFIG = self._yaml_data.get("api", {}).get("openweather", {})

        # 5. Spark & Storage Paths
        self.SPARK_CONFIG = self._yaml_data.get("spark", {})
        self.STORAGE_CONFIG = self._yaml_data.get("storage", {})
        self.ML_CONFIG = self._yaml_data.get("ml", {})

    def get_location_coords(self, location_name: str) -> dict:
        """Helper to get coordinates for a given Mumbai location."""
        for loc in self.LOCATIONS:
            if loc["name"].lower() == location_name.lower():
                return loc
        raise TrafficPlatformException(f"Location '{location_name}' not configured in config.yaml")

    def get_city_coords(self, city_name: str) -> dict:
        """Backward compatibility helper mapping city to location."""
        return self.get_location_coords(city_name)


# Instantiate Singleton Settings Object
settings = Settings()
