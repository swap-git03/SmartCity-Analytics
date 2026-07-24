"""
Power BI Data Exporter.

Exports Gold Analytics and Feature Mart datasets to CSV format in powerbi/data/
for seamless import into Power BI Desktop or web visualization dashboards.
"""

import sys
import os
from pathlib import Path

import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger

logger = get_logger("PowerBIExporter")


def export_powerbi_datasets():
    """Exports Gold Parquet layers to CSV for Power BI Desktop and Web Dashboard."""
    logger.info("--- Starting Power BI Data Export ---")

    gold_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("gold_dir", "data/gold")).resolve()
    pbi_dir = (PROJECT_ROOT / "powerbi" / "data").resolve()
    pbi_dir.mkdir(parents=True, exist_ok=True)

    # 1. Export Gold Analytics Aggregations
    analytics_dir = gold_dir / "analytics"
    if analytics_dir.exists():
        df_analytics = pd.read_parquet(str(analytics_dir))
        analytics_csv = pbi_dir / "gold_analytics.csv"
        df_analytics.to_csv(analytics_csv, index=False)
        logger.info(f"Exported {len(df_analytics)} rows to {analytics_csv.relative_to(PROJECT_ROOT)}")

    # 2. Export Gold Feature Mart
    feature_dir = gold_dir / "feature_mart"
    if feature_dir.exists():
        df_feature = pd.read_parquet(str(feature_dir))
        feature_csv = pbi_dir / "gold_feature_mart.csv"
        df_feature.to_csv(feature_csv, index=False)
        logger.info(f"Exported {len(df_feature)} rows to {feature_csv.relative_to(PROJECT_ROOT)}")

    logger.info("--- Power BI Data Export Completed Successfully ---")


if __name__ == "__main__":
    export_powerbi_datasets()
