# 🏙️ UrbanPulse: Mumbai Smart Mobility & Environmental Decision Support System

> **Enterprise Big Data Lakehouse & AI Decision Support Platform**  
> Built with Python 3.12, Live REST APIs (TomTom & OpenWeatherMap), Apache Kafka (AWS EC2), PySpark Structured Medallion Lakehouse Engine (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), AWS S3 Cloud Storage, Supervised ML Regressors (Random Forest & XGBoost), Live OSRM Turn-by-Turn Street Routing, and an Enterprise Smart City Decision Support Dashboard.

---

## 📐 High-Level System Architecture

```text
+-------------------------------------------------------------------------------------------------------+
|                                           DATA INGESTION                                              |
|   +------------------------------------+              +------------------------------------+          |
|   |   TomTom Traffic Flow API          |              |   OpenWeather Weather & AQI API    |          |
|   |   (15 Mumbai Junction Corridors)   |              |   (Temperature, AQI, PM2.5, Humidity) |          |
|   +-----------------+------------------+              +-----------------+------------------+          |
+---------------------|---------------------------------------------------|-------------------------------------+
                      |                                                   |
                      v                                                   v
+-------------------------------------------------------------------------------------------------------+
|                                      INGESTION & STREAMING LAYER                                      |
|   +-----------------------------------------------------------------------------------------------+   |
|   |   Apache Kafka Event Broker (AWS EC2 Ubuntu 22.04 LTS @ 13.217.6.185:9092)                    |   |
|   |   Topics: mumbai-traffic-events | mumbai-weather-events | mumbai-aqi-events                 |   |
|   |   Resilience: Zero-downtime Local JSON Fallback Buffering (data/bronze/)                      |   |
|   +-----------------------------------------------+-----------------------------------------------+   |
+---------------------------------------------------|---------------------------------------------------+
                                                    |
                                                    v
+-------------------------------------------------------------------------------------------------------+
|                                   PYSPARK MEDALLION LAKEHOUSE ENGINE                                  |
|                                                                                                       |
|   +-----------------------------------------------------------------------------------------------+   |
|   | 🥉 BRONZE LAYER (Raw Landing Zone)                                                            |   |
|   |    Ingests raw JSON streams, enforces StructType schemas, appends audit metadata               |   |
|   |    Writes partitioned Parquet files (data/bronze/parquet/<topic>/location=<Loc>)              |   |
|   +-----------------------------------------------+-----------------------------------------------+   |
|                                                   |                                                   |
|                                                   v                                                   |
|   +-----------------------------------------------------------------------------------------------+   |
|   | 🥈 SILVER LAYER (Cleansing & Standardization)                                                |   |
|   |    Deduplicates events on (location, timestamp) via .dropDuplicates(), cleans sensor noise     |   |
|   |    Engineers temporal feature columns (hour, day_of_week, day_of_month, is_weekend)           |   |
|   |    Writes clean Parquet files (data/silver/traffic/ and data/silver/weather/)                 |   |
|   +-----------------------------------------------+-----------------------------------------------+   |
|                                                   |                                                   |
|                                                   v                                                   |
|   +-----------------------------------------------------------------------------------------------+   |
|   | 🥇 GOLD LAYER (Analytics & Feature Mart)                                                     |   |
|   |    Joins Silver Traffic & Weather streams on (location, hour, event_date)                     |   |
|   |    Engineers ML features (speed_ratio, delay_time_seconds, congestion_index_pct)              |   |
|   |    Generates location-wise summaries & Feature Mart Parquet tables (data/gold/)               |   |
|   +-----------------------------------------------+-----------------------------------------------+   |
+---------------------------------------------------|---------------------------------------------------+
                                                    |
                                                    v
+-------------------------------------------------------------------------------------------------------+
|                                    CLOUD & CONSUMPTION LAYER                                          |
|   +------------------------------------+  +------------------------------------+  +------------------+|
|   |  AWS S3 Cloud Storage Sync         |  |  Machine Learning Regressors       |  |  Smart City UI   ||
|   |  s3://smartcity-traffic-analytics/ |  |  RF Speed & XGBoost AQI Regressors |  |  7-Section Web   ||
|   |  (ap-south-1 Region)               |  |  (Next 30 Minutes Forecast)        |  |  Decision Room   ||
|   +------------------------------------+  +------------------------------------+  +------------------+|
+-------------------------------------------------------------------------------------------------------+
```

---

## 🧰 Technology Stack

| Layer | Technologies & Tools |
|---|---|
| **Core Language** | Python 3.12, PyYAML, Pydantic |
| **API Data Sources** | TomTom Traffic API, OpenWeatherMap Current Weather & Air Pollution APIs |
| **Messaging & Streaming** | Apache Kafka 3.6.1, ZooKeeper (Deployed on AWS EC2 Ubuntu 22.04 @ `13.217.6.185:9092`) |
| **Distributed Processing** | Apache Spark / PySpark 3.5.0, Hadoop S3A Connectors |
| **Storage & Data Formats** | Apache Parquet, JSON, AWS S3 Cloud Storage (`ap-south-1`) |
| **Machine Learning** | Scikit-Learn 1.9.0, XGBoost 3.3.0, Joblib |
| **Routing & GIS Map** | OpenStreetMap OSRM Routing Service, Leaflet.js |
| **Decision Dashboard** | Vanilla CSS Glassmorphism, Chart.js, HTML5, FontAwesome |

---

## 📍 Covered Mumbai Junction Corridors (15 Locations)

`Dadar` • `Airport T2` • `Andheri` • `Bandra` • `BKC` • `Powai` • `Borivali` • `Marine Drive` • `CST` • `Lower Parel` • `Thane` • `Navi Mumbai` • `Kurla` • `Ghatkopar` • `Chembur`

---

## 🖥️ 7-Section Decision Support Dashboard Architecture

```text
+-------------------------------------------------------------------------------------------------------+
| HEADER: UrbanPulse - Mumbai Smart City Decision Support | Status: Live | Updated: Just Now           |
+-------------------------------------------------------------------------------------------------------+
| SECTION 1: JOURNEY PLANNER                                                                            |
| [FROM: Bandra West] ──► [TO: Lower Parel] ──► [Analyze Journey Button]                                |
+-------------------------------------------------------------------------------------------------------+
| SECTION 2: HERO DECISION SUPPORT RECOMMENDATION CARD                                                  |
| Travel Status: SAFE / MODERATE / AVOID | Dynamic Departure Time | Expected Delay | Current/Pred Time   |
| Reason: Traffic speed expected to drop by 18% in next 30m. AQI moderate (2.1). Low rain probability.  |
+-------------------------------------------------------------------------------------------------------+
| SECTION 3: CURRENT CONDITIONS                | SECTION 4: PREDICTED CONDITIONS (NEXT 30 MINS)        |
| - Current Speed: 18.4 km/h                   | - Predicted Speed: 15.1 km/h (Random Forest)          |
| - Current AQI: 2.1                           | - Predicted AQI: 2.4 (XGBoost)                        |
| - Current Weather: 28.5°C | 68% Humidity     | - Predicted Travel Time: 40 Mins (+12m Delay)         |
| - Congestion Index: 61.7%                    | - Traffic Trend: Worsening (-18% speed drop)          |
+-------------------------------------------------------------------------------------------------------+
| SECTION 5: GEOGRAPHICAL JOURNEY CONTEXT MAP                                                           |
| OpenStreetMap OSRM Live Turn-by-Turn Street Driving Polyline (Start 🟢 Green, Dest 🔴 Red Pins).      |
+-------------------------------------------------------------------------------------------------------+
| SECTION 6: TRAFFIC FORECAST ANALYTICS (DYNAMICALLY REACTIVE)                                         |
| - Speed Trend (Observed vs 30m RF Forecast)  | - Route Travel Time Trend (Mins)                       |
| - Hourly Congestion Pattern                  | - Air Quality Across Major Junctions                   |
+-------------------------------------------------------------------------------------------------------+
| SECTION 7: KEY STRATEGIC INSIGHTS & RANKING TABLES                                                    |
| - Top 5 Most Congested Routes Table          | - Top 5 Fastest Routes Table                          |
+-------------------------------------------------------------------------------------------------------+
```

---

## 📁 Repository Directory Layout

```text
Traffic/
├── api/                        # Live API Data Ingestion Clients
│   ├── tomtom_client.py        # TomTom Traffic API client
│   ├── openweather_client.py    # OpenWeather Weather & AQI API client
│   └── test_api_clients.py     # Live API verification script
├── aws/                        # AWS S3 Cloud Integration
│   ├── s3_uploader.py          # Boto3 recursive directory sync module
│   └── test_s3_sync.py         # AWS S3 bucket sync verification runner
├── config/                     # Centralized System Configuration
│   ├── config.yaml             # 15 Mumbai Locations, API, Kafka, Spark, & S3 settings
│   └── settings.py             # PyYAML / Pydantic environment loader
├── data/                       # Local Medallion Data Storage
│   ├── bronze/                 # Raw JSON events & Bronze Parquet stores
│   ├── silver/                 # Cleaned & enriched Silver Parquet tables
│   ├── gold/                   # Feature Mart & Analytics KPI Parquet tables
│   └── hadoop/                 # Windows PySpark native DLL compatibility
├── docs/                       # Project Documentation
│   └── interview_qa.md         # CDAC Viva & Technical Interview Q&A Guide
├── etl/                        # PySpark Medallion Pipeline Scripts
│   ├── bronze.py               # Bronze Layer ingestion pipeline
│   ├── silver.py               # Silver Layer data cleansing & feature extraction
│   └── gold.py                 # Gold Layer stream join & feature mart generation
├── kafka/                      # Streaming Producer & EC2 Setup
│   ├── city_producer.py        # Kafka event producer with local fallback buffer
│   └── scripts/
│       └── setup_ec2_kafka.sh  # Automated EC2 Kafka deployment bash script
├── ml/                         # Machine Learning Pipeline
│   └── train_models.py         # Random Forest Speed Regressor & XGBoost AQI Regressor
├── models/                     # Serialized Model Artifacts (.joblib)
│   ├── mumbai_traffic_speed_rf.joblib
│   ├── mumbai_aqi_xgboost.joblib
│   ├── scaler_speed.joblib
│   └── scaler_aqi.joblib
├── powerbi/                    # Visual Dashboard
│   └── dashboard.html          # Enterprise Smart City Decision Support UI
├── spark/                      # PySpark Session Infrastructure
│   └── spark_session.py        # Singleton SparkSession builder with JVM flags
├── utils/                      # Core Utilities & Pipeline Orchestration
│   ├── logger.py               # Persistent console & file logging
│   ├── exceptions.py           # Custom exception hierarchy
│   ├── decision_support.py     # Rule-based decision recommendation engine
│   └── run_full_pipeline.py    # End-to-End single command pipeline orchestrator
├── .env                        # Local Environment Keys (API & AWS Keys)
├── .env.example                # Template for environment variables
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

## ⚡ Quick Start Guide

### 1. Environment Setup

```bash
git clone https://github.com/your-repo/SmartCity-Traffic-Analytics.git
cd SmartCity-Traffic-Analytics

python -m venv venv
venv\Scripts\activate      # On Windows
source venv/bin/activate    # On Linux/Mac

pip install -r requirements.txt
```

### 2. Configure Environment Variables (`.env`)

```ini
# API Keys
TOMTOM_API_KEY=your_tomtom_api_key
OPENWEATHER_API_KEY=your_openweather_api_key

# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=ap-south-1
AWS_S3_BUCKET=smartcity-traffic-analytics-swapnil

# Kafka Configuration (AWS EC2 Broker)
KAFKA_BOOTSTRAP_SERVERS=13.217.6.185:9092
```

### 3. Run End-to-End Automated Pipeline

```bash
python utils/run_full_pipeline.py
```

### 4. Launch Decision Support Dashboard

Open `powerbi/dashboard.html` in any modern web browser or run:

```bash
python -c "import webbrowser, os; webbrowser.open('file:///' + os.path.abspath('powerbi/dashboard.html'))"
```

---

## 📊 Machine Learning Model Benchmarks

- **Model 1: Random Forest Traffic Speed Regressor** (`mumbai_traffic_speed_rf.joblib`)
  - **Task**: Predicts Traffic Speed (km/h) for Next 30 Minutes
  - **Root Mean Squared Error (RMSE)**: `5.65 km/h`
  - **Mean Absolute Error (MAE)**: `4.12 km/h`
  - **R² Score**: `0.6635`

- **Model 2: XGBoost AQI Regressor** (`mumbai_aqi_xgboost.joblib`)
  - **Task**: Predicts Air Quality Index (AQI 1-5) for Next 30 Minutes
  - **Root Mean Squared Error (RMSE)**: `0.0000`
  - **R² Score**: `1.0000`

---

## 📜 License & Author

- **Author**: Swapnil Fulpagare
- **CDAC Big Data & Analytics Project**
- **License**: MIT License
#   - U r b a n P u l s e - S m a r t - C i t y - D e c i s i o n - S u p p o r t  
 