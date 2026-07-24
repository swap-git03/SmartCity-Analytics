"""
Phase 12 Machine Learning Pipeline Verification Test Script.

Trains Random Forest Classifier & XGBoost Regressor on Gold Feature Mart,
evaluates accuracy/RMSE metrics, saves model artifacts, and runs sample inference.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger
from ml.train_models import TrafficMLPipeline

logger = get_logger("Phase12Test")


def run_ml_verification():
    logger.info("=== Starting Phase 12 Machine Learning Pipeline Verification ===")

    # 1. Initialize ML Pipeline
    ml_pipeline = TrafficMLPipeline()

    # 2. Load Gold Feature Mart Data
    df = ml_pipeline.load_gold_data()

    # 3. Train Congestion Classifier (Random Forest)
    _, clf_metrics = ml_pipeline.train_congestion_classifier(df)
    logger.info(f"Classifier Metrics: {clf_metrics}")

    # 4. Train Speed Ratio Regressor (XGBoost)
    _, reg_metrics = ml_pipeline.train_speed_ratio_regressor(df)
    logger.info(f"Regressor Metrics: {reg_metrics}")

    # 5. Perform Sample Inference Test
    logger.info("\n--- Running Real-Time Sample ML Inference Test ---")
    sample_input = {
        "temperature": 28.5,
        "humidity": 85,
        "weather_severity_index": 2,  # Rain
        "aqi": 3,
        "pm2_5": 35.0,
        "hour": 18,                   # Evening Peak Traffic Hour
        "day_of_week": 6,             # Friday
        "is_weekend": 0
    }

    prediction = ml_pipeline.predict_realtime_sample(sample_input)
    logger.info(f"Sample Input Features: {sample_input}")
    logger.info(f"ML Model Inference Output: {prediction}")

    logger.info("=== Phase 12 Machine Learning Pipeline Verification Complete & Successful ===")


if __name__ == "__main__":
    run_ml_verification()
