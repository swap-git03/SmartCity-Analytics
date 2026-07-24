"""
TomTom Traffic Flow REST API Client.

Fetches real-time traffic speed, free flow speed, travel time, and congestion metrics.
Includes automated schema flattening, derived feature calculation, and mock data fallback.
"""

import time
import datetime
import random
import requests
from typing import Dict, Any, Optional

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import APIException

logger = get_logger("TomTomClient")


class TomTomTrafficClient:
    """REST Client wrapper for TomTom Traffic Flow API."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize TomTom API Client.
        
        Args:
            api_key (str, optional): TomTom API Key. Defaults to settings.TOMTOM_API_KEY.
        """
        self.api_key = api_key or settings.TOMTOM_API_KEY
        self.base_url = settings.TOMTOM_CONFIG.get(
            "base_url", "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        )
        self.unit = settings.TOMTOM_CONFIG.get("unit", "KMPH")

    def fetch_traffic_flow(self, lat: float, lon: float, city_name: str = "Unknown") -> Dict[str, Any]:
        """
        Fetch real-time traffic flow data for a given GPS coordinate.

        Args:
            lat (float): Latitude coordinate.
            lon (float): Longitude coordinate.
            city_name (str): Name of the city for payload tagging.

        Returns:
            Dict[str, Any]: Standardized traffic metric payload.
        """
        if not self.api_key or self.api_key == "your_tomtom_api_key_here":
            logger.warning(f"No valid TomTom API Key provided. Generating fallback mock data for {city_name}.")
            return self._generate_mock_data(lat, lon, city_name)

        params = {
            "point": f"{lat},{lon}",
            "unit": self.unit,
            "key": self.api_key
        }

        try:
            logger.info(f"Fetching TomTom traffic data for {city_name} ({lat}, {lon})...")
            response = requests.get(self.base_url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return self._parse_and_standardize(data, lat, lon, city_name)
            else:
                logger.error(f"TomTom API returned status code {response.status_code}: {response.text}")
                return self._generate_mock_data(lat, lon, city_name)

        except Exception as e:
            logger.error(f"Failed to fetch TomTom traffic data for {city_name}: {e}")
            logger.info(f"Falling back to mock traffic generator for {city_name}.")
            return self._generate_mock_data(lat, lon, city_name)

    def _parse_and_standardize(self, data: Dict[str, Any], lat: float, lon: float, city_name: str) -> Dict[str, Any]:
        """Parses raw TomTom JSON payload into standardized schema."""
        flow = data.get("flowSegmentData", {})

        current_speed = flow.get("currentSpeed", 0.0)
        free_flow_speed = flow.get("freeFlowSpeed", 1.0)
        current_travel_time = flow.get("currentTravelTime", 0)
        free_flow_travel_time = flow.get("freeFlowTravelTime", 0)
        confidence = flow.get("confidence", 1.0)
        road_closure = flow.get("roadClosure", False)

        # Derived Congestion Ratio (Avoid Division by Zero)
        congestion_ratio = round(current_speed / max(free_flow_speed, 1.0), 3)

        # Categorical Congestion Tag
        if congestion_ratio >= 0.85:
            congestion_level = "Low"
        elif congestion_ratio >= 0.55:
            congestion_level = "Medium"
        else:
            congestion_level = "High"

        timestamp_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return {
            "timestamp": timestamp_utc,
            "city_name": city_name,
            "latitude": lat,
            "longitude": lon,
            "current_speed": float(current_speed),
            "free_flow_speed": float(free_flow_speed),
            "current_travel_time": int(current_travel_time),
            "free_flow_travel_time": int(free_flow_travel_time),
            "congestion_ratio": congestion_ratio,
            "congestion_level": congestion_level,
            "confidence": float(confidence),
            "road_closure": bool(road_closure),
            "data_source": "TomTom_API"
        }

    def _generate_mock_data(self, lat: float, lon: float, city_name: str) -> Dict[str, Any]:
        """Generates realistic synthetic traffic flow payload for fallback testing."""
        free_flow_speed = random.choice([40.0, 50.0, 60.0, 80.0])
        congestion_factor = random.uniform(0.3, 1.0)
        current_speed = round(free_flow_speed * congestion_factor, 1)

        congestion_ratio = round(current_speed / free_flow_speed, 3)
        if congestion_ratio >= 0.85:
            congestion_level = "Low"
        elif congestion_ratio >= 0.55:
            congestion_level = "Medium"
        else:
            congestion_level = "High"

        timestamp_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return {
            "timestamp": timestamp_utc,
            "city_name": city_name,
            "latitude": lat,
            "longitude": lon,
            "current_speed": float(current_speed),
            "free_flow_speed": float(free_flow_speed),
            "current_travel_time": int(round(100 / max(current_speed, 1.0) * 60)),
            "free_flow_travel_time": int(round(100 / max(free_flow_speed, 1.0) * 60)),
            "congestion_ratio": congestion_ratio,
            "congestion_level": congestion_level,
            "confidence": 0.95,
            "road_closure": False,
            "data_source": "Mock_Generator"
        }
