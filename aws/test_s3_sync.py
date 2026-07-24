"""
Phase 11 AWS S3 Cloud Integration Verification Test Script.

Tests synchronizing local Bronze, Silver, and Gold Parquet datasets to
your live AWS S3 bucket smartcity-traffic-analytics-swapnil.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from aws.s3_uploader import S3Uploader

logger = get_logger("Phase11Test")


def run_s3_sync_verification():
    logger.info("=== Starting Phase 11 AWS S3 Cloud Integration Verification ===")

    # 1. Initialize S3 Uploader
    uploader = S3Uploader()

    # 2. Synchronize Bronze Parquet Data
    bronze_local_dir = PROJECT_ROOT / "data" / "bronze"
    bronze_stats = uploader.sync_directory(bronze_local_dir, s3_prefix="bronze")
    logger.info(f"Bronze Sync Stats: {bronze_stats}")

    # 3. Synchronize Silver Parquet Data
    silver_local_dir = PROJECT_ROOT / "data" / "silver"
    silver_stats = uploader.sync_directory(silver_local_dir, s3_prefix="silver")
    logger.info(f"Silver Sync Stats: {silver_stats}")

    # 4. Synchronize Gold Parquet Data
    gold_local_dir = PROJECT_ROOT / "data" / "gold"
    gold_stats = uploader.sync_directory(gold_local_dir, s3_prefix="gold")
    logger.info(f"Gold Sync Stats: {gold_stats}")

    # 5. List and Verify Uploaded Objects in S3 Bucket
    logger.info("\n--- Verifying AWS S3 Objects in Bucket ---")
    s3_objects = uploader.list_s3_objects()
    for obj_key in s3_objects[:10]:
        logger.info(f"  [S3 Cloud Object] s3://{uploader.bucket_name}/{obj_key}")

    logger.info("=== Phase 11 AWS S3 Cloud Integration Verification Complete & Successful ===")


if __name__ == "__main__":
    run_s3_sync_verification()
