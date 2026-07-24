"""
UrbanPulse Machine Learning Pipeline for Mumbai Smart Mobility & Environmental Analytics.

Model 1: Random Forest Regressor -> Predicts Future Traffic Speed (km/h)
Inputs: current_speed, free_flow_speed, temperature, humidity, aqi, hour, day_of_week
Output: predicted_traffic_speed

Model 2: XGBoost Regressor -> Predicts Future Air Quality Index (AQI)
Inputs: temperature, humidity, pressure, wind_speed, current_speed, hour
Output: predicted_aqi
"""

import sys
import os
from pathlib import Path
from typing import Dict, Tuple, Any

import numpy as np
import pandas as pd
import joblib

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import xgboost as xgb

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from utils.logger import get_logger
from utils.exceptions import MLException

logger = get_logger("UrbanPulseMLPipeline")


class TrafficMLPipeline:
    """Manages model training, evaluation, and serialization for UrbanPulse."""

    def __init__(self):
        """Initialize directory paths and model artifacts storage."""
        self.gold_dir = (PROJECT_ROOT / settings.STORAGE_CONFIG.get("local", {}).get("gold_dir", "data/gold")).resolve()
        self.models_dir = (PROJECT_ROOT / "models").resolve()
        self.models_dir.mkdir(parents=True, exist_ok=True)

        # Model 1 Feature List (Random Forest Regressor -> Speed)
        self.speed_model_features = [
            "avg_current_speed", "avg_free_flow_speed", "avg_temperature",
            "avg_humidity", "avg_aqi", "hour", "day_of_week"
        ]

        # Model 2 Feature List (XGBoost Regressor -> AQI)
        self.aqi_model_features = [
            "avg_temperature", "avg_humidity", "avg_pressure",
            "avg_wind_speed", "avg_current_speed", "hour"
        ]

    def load_gold_data(self) -> pd.DataFrame:
        """
        Loads Gold Feature Mart or Analytics Parquet data into pandas DataFrame.

        Returns:
            pd.DataFrame: Loaded feature dataset.
        """
        analytics_dir = self.gold_dir / "analytics"
        feature_mart_dir = self.gold_dir / "feature_mart"

        target_dir = analytics_dir if analytics_dir.exists() else feature_mart_dir
        if not target_dir.exists():
            raise MLException(f"Gold data directory not found at {target_dir}. Run Gold Pipeline first!")

        logger.info(f"Loading Gold analytics data from {target_dir.relative_to(PROJECT_ROOT)}...")
        df = pd.read_parquet(str(target_dir))

        # Fill missing default columns if necessary
        if "avg_free_flow_speed" not in df.columns:
            df["avg_free_flow_speed"] = df["avg_current_speed"] * 1.3
        if "avg_pressure" not in df.columns:
            df["avg_pressure"] = 1013.0
        if "avg_wind_speed" not in df.columns:
            df["avg_wind_speed"] = 4.2
        if "day_of_week" not in df.columns:
            df["day_of_week"] = 3

        logger.info(f"Loaded {len(df)} feature rows for UrbanPulse ML training.")
        return df

    def train_speed_regressor(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, float]]:
        """
        Model 1: Trains Random Forest Regressor to predict Future Traffic Speed (km/h).
        Inputs: current_speed, free_flow_speed, temperature, humidity, aqi, hour, day_of_week.
        """
        logger.info("--- Training Model 1: Random Forest Regressor (Traffic Speed Prediction) ---")

        X = df[self.speed_model_features].fillna(20.0).copy()
        y = df["avg_current_speed"].fillna(20.0).copy()

        # Train-Test Split (80/20)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Scale Features
        scaler_speed = StandardScaler()
        X_train_scaled = scaler_speed.fit_transform(X_train)
        X_test_scaled = scaler_speed.transform(X_test)

        # Train Random Forest Regressor
        rf_speed_model = RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42)
        rf_speed_model.fit(X_train_scaled, y_train)

        # Predictions & Evaluation Metrics
        y_pred = rf_speed_model.predict(X_test_scaled)
        mse = float(mean_squared_error(y_test, y_pred))
        rmse = float(np.sqrt(mse))
        mae = float(mean_absolute_error(y_test, y_pred))
        r2 = float(r2_score(y_test, y_pred)) if len(y_test) > 1 else 1.0

        metrics = {
            "rmse": round(rmse, 4),
            "mae": round(mae, 4),
            "r2_score": round(r2, 4)
        }

        # Save artifacts
        joblib.dump(rf_speed_model, self.models_dir / "mumbai_traffic_speed_rf.joblib")
        joblib.dump(scaler_speed, self.models_dir / "scaler_speed.joblib")

        logger.info(f"Random Forest Speed Regressor trained! RMSE: {metrics['rmse']} km/h | R2-Score: {metrics['r2_score']}")
        return rf_speed_model, metrics

    def train_aqi_regressor(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, float]]:
        """
        Model 2: Trains XGBoost Regressor to predict Future Air Quality Index (AQI 1-5).
        Inputs: temperature, humidity, pressure, wind_speed, current_speed, hour.
        """
        logger.info("--- Training Model 2: XGBoost Regressor (AQI Prediction) ---")

        X = df[self.aqi_model_features].fillna(2.0).copy()
        y = df["avg_aqi"].fillna(2.0).copy()

        # Train-Test Split (80/20)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Scale Features
        scaler_aqi = StandardScaler()
        X_train_scaled = scaler_aqi.fit_transform(X_train)
        X_test_scaled = scaler_aqi.transform(X_test)

        # Train XGBoost Regressor
        xgb_aqi_model = xgb.XGBRegressor(n_estimators=120, learning_rate=0.05, max_depth=6, random_state=42)
        xgb_aqi_model.fit(X_train_scaled, y_train)

        # Predictions & Evaluation Metrics
        y_pred = xgb_aqi_model.predict(X_test_scaled)
        mse = float(mean_squared_error(y_test, y_pred))
        rmse = float(np.sqrt(mse))
        r2 = float(r2_score(y_test, y_pred)) if len(y_test) > 1 else 1.0

        metrics = {
            "rmse": round(rmse, 4),
            "r2_score": round(r2, 4)
        }

        # Save artifacts
        joblib.dump(xgb_aqi_model, self.models_dir / "mumbai_aqi_xgboost.joblib")
        joblib.dump(scaler_aqi, self.models_dir / "scaler_aqi.joblib")

        logger.info(f"XGBoost AQI Regressor trained! RMSE: {metrics['rmse']} | R2-Score: {metrics['r2_score']}")
        return xgb_aqi_model, metrics

    def predict_mumbai_location_future(
        self,
        current_speed: float,
        free_flow_speed: float,
        temp: float,
        humidity: float,
        aqi: float,
        hour: int,
        pressure: float = 1013.0,
        wind_speed: float = 4.0,
        day_of_week: int = 3
    ) -> Dict[str, Any]:
        """
        Performs real-time ML inference for a Mumbai location.
        Calculates predicted speed, predicted AQI, and derived Congestion Index.
        """
        rf_speed_model = joblib.load(self.models_dir / "mumbai_traffic_speed_rf.joblib")
        scaler_speed = joblib.load(self.models_dir / "scaler_speed.joblib")
        xgb_aqi_model = joblib.load(self.models_dir / "mumbai_aqi_xgboost.joblib")
        scaler_aqi = joblib.load(self.models_dir / "scaler_aqi.joblib")

        # 1. Predict Speed
        df_speed = pd.DataFrame([{
            "avg_current_speed": current_speed,
            "avg_free_flow_speed": free_flow_speed,
            "avg_temperature": temp,
            "avg_humidity": humidity,
            "avg_aqi": aqi,
            "hour": hour,
            "day_of_week": day_of_week
        }])[self.speed_model_features]

        scaled_speed_input = scaler_speed.transform(df_speed)
        pred_speed = float(rf_speed_model.predict(scaled_speed_input)[0])

        # 2. Predict AQI
        df_aqi = pd.DataFrame([{
            "avg_temperature": temp,
            "avg_humidity": humidity,
            "avg_pressure": pressure,
            "avg_wind_speed": wind_speed,
            "avg_current_speed": current_speed,
            "hour": hour
        }])[self.aqi_model_features]

        scaled_aqi_input = scaler_aqi.transform(df_aqi)
        pred_aqi = float(xgb_aqi_model.predict(scaled_aqi_input)[0])

        # 3. Spark-derived Congestion Index (%)
        congestion_index = round(max(0.0, min(100.0, (1.0 - (pred_speed / max(free_flow_speed, 1.0))) * 100)), 1)

        return {
            "predicted_traffic_speed": round(pred_speed, 1),
            "predicted_aqi": round(pred_aqi, 1),
            "derived_congestion_index": congestion_index
        }
