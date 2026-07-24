"""
Phase 2 Verification Script
Tests Logger, Settings Loader, and Custom Exception system.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import APIException

logger = get_logger("Phase2Test")

def run_verification():
    logger.info("=== Starting Phase 2 Verification ===")
    logger.info(f"App Name: {settings.APP_NAME}")
    logger.info(f"Kafka Bootstrap Servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Configured Cities: {[c['name'] for c in settings.CITIES]}")
    logger.info(f"TomTom Base URL: {settings.TOMTOM_CONFIG.get('base_url')}")
    logger.info(f"OpenWeather Base URL: {settings.OPENWEATHER_CONFIG.get('weather_base_url')}")

    # Verify Custom Exception handling
    try:
        raise APIException("Testing custom exception handling mechanism")
    except APIException as e:
        logger.info(f"Successfully caught custom exception: {e}")

    logger.info("=== Phase 2 Verification Complete & Successful ===")

if __name__ == "__main__":
    run_verification()
