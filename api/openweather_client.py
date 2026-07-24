"""
OpenWeather REST API Client (Current Weather & Air Quality Pollution).

Fetches meteorological weather features and chemical pollutant air quality metrics.
Includes automated payload standardization and mock fallback generation.
"""

import time
import datetime
import random
import requests
from typing import Dict, Any, Optional

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import APIException

logger = get_logger("OpenWeatherClient")


class OpenWeatherClient:
    """REST Client wrapper for OpenWeather Current Weather and Air Pollution APIs."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize OpenWeather API Client.

        Args:
            api_key (str, optional): OpenWeather API Key. Defaults to settings.OPENWEATHER_API_KEY.
        """
        self.api_key = api_key or settings.OPENWEATHER_API_KEY
        self.weather_url = settings.OPENWEATHER_CONFIG.get(
            "weather_base_url", "https://api.openweathermap.org/data/2.5/weather"
        )
        self.aqi_url = settings.OPENWEATHER_CONFIG.get(
            "aqi_base_url", "https://api.openweathermap.org/data/2.5/air_pollution"
        )
        self.units = settings.OPENWEATHER_CONFIG.get("units", "metric")

    def fetch_weather_and_aqi(self, lat: float, lon: float, city_name: str = "Unknown") -> Dict[str, Any]:
        """
        Fetch real-time weather and air pollution metrics for a given GPS coordinate.

        Args:
            lat (float): Latitude coordinate.
            lon (float): Longitude coordinate.
            city_name (str): Name of the city for payload tagging.

        Returns:
            Dict[str, Any]: Standardized weather and air quality payload.
        """
        if not self.api_key or self.api_key == "your_openweather_api_key_here":
            logger.warning(f"No valid OpenWeather API Key provided. Generating fallback mock data for {city_name}.")
            return self._generate_mock_data(lat, lon, city_name)

        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": self.units
        }

        try:
            logger.info(f"Fetching OpenWeather Weather & AQI data for {city_name} ({lat}, {lon})...")
            
            # 1. Fetch Current Weather
            weather_res = requests.get(self.weather_url, params=params, timeout=10)
            weather_data = weather_res.json() if weather_res.status_code == 200 else {}

            # 2. Fetch Air Pollution (AQI)
            aqi_res = requests.get(self.aqi_url, params=params, timeout=10)
            aqi_data = aqi_res.json() if aqi_res.status_code == 200 else {}

            if weather_res.status_code == 200 and aqi_res.status_code == 200:
                return self._parse_and_standardize(weather_data, aqi_data, lat, lon, city_name)
            else:
                logger.error(f"OpenWeather API returned error status. Weather: {weather_res.status_code}, AQI: {aqi_res.status_code}")
                return self._generate_mock_data(lat, lon, city_name)

        except Exception as e:
            logger.error(f"Failed to fetch OpenWeather data for {city_name}: {e}")
            logger.info(f"Falling back to mock weather generator for {city_name}.")
            return self._generate_mock_data(lat, lon, city_name)

    def _parse_and_standardize(
        self, weather_data: Dict[str, Any], aqi_data: Dict[str, Any], lat: float, lon: float, city_name: str
    ) -> Dict[str, Any]:
        """Parses raw OpenWeather JSON responses into standardized schema."""
        
        # Parse Weather JSON
        main_w = weather_data.get("main", {})
        wind_w = weather_data.get("wind", {})
        weather_desc = weather_data.get("weather", [{}])[0].get("main", "Clear")

        temp = main_w.get("temp", 25.0)
        feels_like = main_w.get("feels_like", 25.0)
        humidity = main_w.get("humidity", 50)
        pressure = main_w.get("pressure", 1013)
        wind_speed = wind_w.get("speed", 3.5)

        # Parse AQI JSON
        list_aqi = aqi_data.get("list", [{}])[0]
        aqi_val = list_aqi.get("main", {}).get("aqi", 2)  # 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor
        components = list_aqi.get("components", {})

        co = components.get("co", 250.0)
        no2 = components.get("no2", 20.0)
        o3 = components.get("o3", 60.0)
        so2 = components.get("so2", 5.0)
        pm2_5 = components.get("pm2_5", 15.0)
        pm10 = components.get("pm10", 30.0)

        timestamp_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return {
            "timestamp": timestamp_utc,
            "city_name": city_name,
            "latitude": lat,
            "longitude": lon,
            "temperature": float(temp),
            "feels_like": float(feels_like),
            "humidity": int(humidity),
            "pressure": int(pressure),
            "wind_speed": float(wind_speed),
            "weather_condition": str(weather_desc),
            "aqi": int(aqi_val),
            "co": float(co),
            "no2": float(no2),
            "o3": float(o3),
            "so2": float(so2),
            "pm2_5": float(pm2_5),
            "pm10": float(pm10),
            "data_source": "OpenWeather_API"
        }

    def _generate_mock_data(self, lat: float, lon: float, city_name: str) -> Dict[str, Any]:
        """Generates realistic synthetic weather and AQI payload for fallback testing."""
        temp = round(random.uniform(15.0, 38.0), 1)
        humidity = random.randint(30, 85)
        pressure = random.randint(1005, 1020)
        wind_speed = round(random.uniform(1.0, 12.0), 1)
        weather_condition = random.choice(["Clear", "Clouds", "Rain", "Haze", "Mist"])

        aqi_val = random.randint(1, 5)
        pm2_5 = round(random.uniform(5.0, 120.0), 1)
        pm10 = round(pm2_5 * random.uniform(1.2, 2.0), 1)

        timestamp_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return {
            "timestamp": timestamp_utc,
            "city_name": city_name,
            "latitude": lat,
            "longitude": lon,
            "temperature": float(temp),
            "feels_like": float(round(temp + random.uniform(-2, 3), 1)),
            "humidity": int(humidity),
            "pressure": int(pressure),
            "wind_speed": float(wind_speed),
            "weather_condition": weather_condition,
            "aqi": int(aqi_val),
            "co": round(random.uniform(200.0, 800.0), 1),
            "no2": round(random.uniform(10.0, 60.0), 1),
            "o3": round(random.uniform(20.0, 90.0), 1),
            "so2": round(random.uniform(2.0, 15.0), 1),
            "pm2_5": float(pm2_5),
            "pm10": float(pm10),
            "data_source": "Mock_Generator"
        }
