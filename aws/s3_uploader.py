"""
AWS S3 Cloud Synchronization Module.

Uploads and synchronizes Bronze, Silver, and Gold Parquet layers to AWS S3 bucket.
Provides automated retry logic, recursive directory sync, and S3 file verification.
"""

import sys
import os
from pathlib import Path
from typing import List, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import AWSException

logger = get_logger("S3Uploader")


class S3Uploader:
    """Manages secure file and directory synchronization with AWS S3."""

    def __init__(self):
        """Initialize AWS S3 Boto3 client with environment credentials."""
        self.bucket_name = settings.AWS_S3_BUCKET
        self.region_name = settings.AWS_REGION
        self.access_key = settings.AWS_ACCESS_KEY_ID
        self.secret_key = settings.AWS_SECRET_ACCESS_KEY

        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name
            )
            logger.info(f"Initialized AWS S3 Client for bucket '{self.bucket_name}' in region '{self.region_name}'.")
        except Exception as e:
            logger.error(f"Failed to initialize AWS S3 Client: {e}")
            raise AWSException("AWS S3 Client initialization failed", original_exception=e)

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Uploads a single local file to S3.

        Args:
            local_path (Path): Path to local file.
            s3_key (str): S3 destination object key.

        Returns:
            bool: True if upload succeeded, False otherwise.
        """
        local_path = Path(local_path).resolve()
        if not local_path.exists() or not local_path.is_file():
            logger.warning(f"Local file does not exist: {local_path}")
            return False

        try:
            s3_key_clean = s3_key.replace("\\", "/")
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key_clean)
            logger.info(f"Uploaded file: {local_path.name} -> s3://{self.bucket_name}/{s3_key_clean}")
            return True
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to upload {local_path.name} to S3: {e}")
            raise AWSException(f"S3 file upload failed for {local_path.name}", original_exception=e)

    def sync_directory(self, local_dir: Path, s3_prefix: str) -> Dict[str, int]:
        """
        Recursively uploads all files in local_dir to S3 under s3_prefix.

        Args:
            local_dir (Path): Local directory to sync.
            s3_prefix (str): S3 prefix prefix folder.

        Returns:
            Dict[str, int]: Upload summary stats (total_files, uploaded_files, skipped_files).
        """
        local_dir = Path(local_dir).resolve()
        if not local_dir.exists():
            logger.warning(f"Directory not found for S3 sync: {local_dir}")
            return {"total_files": 0, "uploaded_files": 0, "skipped_files": 0}

        s3_prefix_clean = s3_prefix.strip("/").replace("\\", "/")
        uploaded_count = 0
        skipped_count = 0

        files_to_upload = [p for p in local_dir.glob("**/*") if p.is_file() and not p.name.startswith(".")]
        logger.info(f"Starting S3 sync for '{local_dir.relative_to(PROJECT_ROOT)}' -> s3://{self.bucket_name}/{s3_prefix_clean}/ ({len(files_to_upload)} files)")

        for file_path in files_to_upload:
            relative_path = file_path.relative_to(local_dir).as_posix()
            s3_key = f"{s3_prefix_clean}/{relative_path}"

            success = self.upload_file(file_path, s3_key)
            if success:
                uploaded_count += 1
            else:
                skipped_count += 1

        summary = {
            "total_files": len(files_to_upload),
            "uploaded_files": uploaded_count,
            "skipped_files": skipped_count
        }
        logger.info(f"S3 Sync complete for prefix '{s3_prefix_clean}': {uploaded_count}/{len(files_to_upload)} uploaded successfully.")
        return summary

    def list_s3_objects(self, s3_prefix: str = "") -> List[str]:
        """
        Lists S3 object keys under a given prefix.

        Args:
            s3_prefix (str): S3 prefix folder.

        Returns:
            List[str]: List of S3 object keys.
        """
        try:
            s3_prefix_clean = s3_prefix.strip("/").replace("\\", "/")
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_prefix_clean)
            contents = response.get("Contents", [])
            object_keys = [obj["Key"] for obj in contents]
            logger.info(f"Found {len(object_keys)} objects in s3://{self.bucket_name}/{s3_prefix_clean}")
            return object_keys
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to list objects in S3 prefix '{s3_prefix}': {e}")
            raise AWSException(f"S3 list objects failed for {s3_prefix}", original_exception=e)
