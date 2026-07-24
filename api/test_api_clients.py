"""
Phase 3 API Clients Verification Test Script.

Tests TomTom Traffic Client and OpenWeather Client across configured cities.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from api.tomtom_client import TomTomTrafficClient
from api.openweather_client import OpenWeatherClient

logger = get_logger("Phase3Test")


def run_api_verification():
    logger.info("=== Starting Phase 3 API Clients Verification ===")

    tomtom_client = TomTomTrafficClient()
    openweather_client = OpenWeatherClient()

    cities = settings.CITIES

    for city in cities:
        name = city["name"]
        lat = city["lat"]
        lon = city["lon"]

        logger.info(f"\n--- Testing City: {name} ({lat}, {lon}) ---")

        # 1. Test TomTom Traffic Client
        traffic_data = tomtom_client.fetch_traffic_flow(lat, lon, name)
        logger.info(f"[TomTom Payload] Source: {traffic_data.get('data_source')}")
        logger.info(f"  - Speed: {traffic_data.get('current_speed')} km/h (Free Flow: {traffic_data.get('free_flow_speed')} km/h)")
        logger.info(f"  - Congestion Ratio: {traffic_data.get('congestion_ratio')} -> Level: {traffic_data.get('congestion_level')}")
        logger.info(f"  - Travel Time: {traffic_data.get('current_travel_time')}s")

        # 2. Test OpenWeather Client
        weather_data = openweather_client.fetch_weather_and_aqi(lat, lon, name)
        logger.info(f"[OpenWeather Payload] Source: {weather_data.get('data_source')}")
        logger.info(f"  - Temp: {weather_data.get('temperature')}°C, Humidity: {weather_data.get('humidity')}%, Condition: {weather_data.get('weather_condition')}")
        logger.info(f"  - AQI Index: {weather_data.get('aqi')}, PM2.5: {weather_data.get('pm2_5')}, PM10: {weather_data.get('pm10')}")

    logger.info("\n=== Phase 3 API Verification Complete & Successful ===")


if __name__ == "__main__":
    run_api_verification()
