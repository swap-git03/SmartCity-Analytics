"""
Phase 4 & 5 Kafka Producer Verification Test Script.

Executes a single polling burst to test Kafka Producer connection to EC2 broker (13.217.6.185:9092).
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from kafka.city_producer import CityDataProducer

logger = get_logger("Phase5Test")


def run_producer_verification():
    logger.info("=== Starting Kafka Producer Connection Test to EC2 Broker ===")

    producer = CityDataProducer()
    
    # Run a single stream burst
    producer.poll_and_publish_single_burst()

    # Close producer resources cleanly
    producer.close()

    logger.info("=== Kafka Producer Connection Test Complete ===")


if __name__ == "__main__":
    run_producer_verification()
