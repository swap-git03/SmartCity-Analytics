"""
Apache Kafka Producer Service.

Continuously polls TomTom and OpenWeather APIs and streams JSON events to Kafka topics:
- traffic-raw-events
- weather-raw-events
- aqi-raw-events

Includes delivery callbacks, graceful signal handling, and local JSON buffer fallback.
"""

import sys
import os
import json
import time
import signal
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def _get_third_party_kafka_producer_class():
    """Dynamically imports KafkaProducer from third-party kafka-python package, bypassing local folder namespace shadow."""
    original_path = sys.path[:]
    project_root_str = str(PROJECT_ROOT)
    kafka_folder_str = str(PROJECT_ROOT / "kafka")
    
    # Remove local project paths temporarily to reach site-packages
    sys.path = [p for p in sys.path if p != project_root_str and p != kafka_folder_str and p != ""]
    try:
        from kafka import KafkaProducer
        return KafkaProducer
    except Exception as e:
        return None
    finally:
        sys.path = original_path

KafkaProducer = _get_third_party_kafka_producer_class()

# Restore project root in sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import KafkaException
from api.tomtom_client import TomTomTrafficClient
from api.openweather_client import OpenWeatherClient

logger = get_logger("KafkaProducer")


class CityDataProducer:
    """Enterprise Kafka Producer service for Smart City data streams."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """
        Initialize Kafka Producer and API Clients.
        """
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topics = settings.KAFKA_TOPICS
        
        self.traffic_topic = self.topics.get("traffic_raw", "traffic-raw-events")
        self.weather_topic = self.topics.get("weather_raw", "weather-raw-events")
        self.aqi_topic = self.topics.get("aqi_raw", "aqi-raw-events")

        self.tomtom_client = TomTomTrafficClient()
        self.openweather_client = OpenWeatherClient()

        self.running = True
        self.producer = None
        self._init_kafka_producer()

        # Local fallback directory
        self.local_fallback_dir = Path(settings.STORAGE_CONFIG.get("local", {}).get("bronze_dir", "data/bronze"))
        self.local_fallback_dir.mkdir(parents=True, exist_ok=True)

    def _init_kafka_producer(self):
        """Attempts to initialize connection to Kafka broker."""
        if KafkaProducer is None:
            logger.warning("kafka-python package not installed or unavailable. Producer in Fallback mode.")
            self.producer = None
            return

        try:
            logger.info(f"Connecting Kafka Producer to broker at: {self.bootstrap_servers}...")
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=100,
                max_block_ms=3000  # Timeout connection attempt quickly if broker is offline
            )
            logger.info("Successfully connected to Kafka broker!")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka broker at {self.bootstrap_servers}: {e}")
            logger.info("Producer will operate in Local File Fallback mode until Kafka is online.")
            self.producer = None

    def _on_send_success(self, record_metadata):
        """Callback executed when a message is successfully acknowledged by Kafka."""
        logger.debug(
            f"Message delivered to topic '{record_metadata.topic}' "
            f"[Partition: {record_metadata.partition}, Offset: {record_metadata.offset}]"
        )

    def _on_send_error(self, ex):
        """Callback executed when message delivery fails."""
        logger.error(f"Kafka message delivery failed: {ex}")

    def send_event(self, topic: str, key: str, payload: Dict[str, Any]):
        """
        Publishes a message to Kafka, or dumps to local file fallback if Kafka is offline.
        """
        if self.producer:
            try:
                future = self.producer.send(topic, key=key, value=payload)
                future.add_callback(self._on_send_success)
                future.add_errback(self._on_send_error)
            except Exception as e:
                logger.error(f"Error publishing message to Kafka topic '{topic}': {e}")
                self._save_to_local_fallback(topic, payload)
        else:
            self._save_to_local_fallback(topic, payload)

    def _save_to_local_fallback(self, topic: str, payload: Dict[str, Any]):
        """Saves message as JSON in local data/bronze/ buffer directory."""
        topic_dir = self.local_fallback_dir / topic
        topic_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{payload.get('city_name', 'city')}_{int(time.time()*1000)}.json"
        filepath = topic_dir / filename

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"[Fallback Buffer] Saved event to local file: {filepath.relative_to(PROJECT_ROOT)}")
        except Exception as e:
            logger.error(f"Failed to write fallback event to file: {e}")

    def poll_and_publish_single_burst(self):
        """Polls APIs once for all configured cities and publishes payloads."""
        cities = settings.CITIES
        logger.info(f"--- Triggering single stream burst for {len(cities)} cities ---")

        for city in cities:
            city_name = city["name"]
            lat = city["lat"]
            lon = city["lon"]

            # 1. Traffic Stream
            traffic_payload = self.tomtom_client.fetch_traffic_flow(lat, lon, city_name)
            self.send_event(self.traffic_topic, key=city_name, payload=traffic_payload)

            # 2. Weather & AQI Stream
            weather_payload = self.openweather_client.fetch_weather_and_aqi(lat, lon, city_name)
            self.send_event(self.weather_topic, key=city_name, payload=weather_payload)
            self.send_event(self.aqi_topic, key=city_name, payload=weather_payload)

        if self.producer:
            self.producer.flush()

    def start_continuous_stream(self, interval_seconds: int = 30):
        """Runs an infinite loop polling APIs and publishing to Kafka until interrupted."""
        logger.info(f"Starting continuous streaming loop (Interval: {interval_seconds}s)... Press Ctrl+C to stop.")
        
        try:
            while self.running:
                self.poll_and_publish_single_burst()
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Stopping producer...")
        finally:
            self.close()

    def close(self):
        """Flushes buffered records and closes Kafka Producer connection cleanly."""
        self.running = False
        if self.producer:
            logger.info("Flushing remaining Kafka messages and shutting down producer...")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka Producer shutdown complete.")
