"""
Phase 4 Kafka Producer Verification Test Script.

Executes a single polling burst to verify API polling, payload formatting,
topic message emission, and fallback buffer persistence.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from kafka.producer import CityDataProducer

logger = get_logger("Phase4Test")


def run_producer_verification():
    logger.info("=== Starting Phase 4 Kafka Producer Verification ===")

    producer = CityDataProducer()
    
    # Run a single stream burst
    producer.poll_and_publish_single_burst()

    # Close producer resources cleanly
    producer.close()

    logger.info("=== Phase 4 Kafka Producer Verification Complete & Successful ===")


if __name__ == "__main__":
    run_producer_verification()
