# 🎓 SmartCity Real-Time Traffic & Environmental Analytics
## CDAC Viva & Technical Interview Questions & Answers Guide

---

### Q1: Can you give an executive summary of your project?
**Answer**:
Our project is an end-to-end **Smart City Traffic & Environmental Analytics Platform** built using a **Medallion Lakehouse Architecture**.
It ingests real-time traffic flow data from the **TomTom API** and weather/air quality metrics from **OpenWeatherMap APIs** for major cities (Mumbai, London, New York).
The data is streamed via **Apache Kafka** running on an **AWS EC2 instance**, processed using **PySpark** across Bronze (raw landing), Silver (data cleansing & feature enrichment), and Gold (feature mart & analytics aggregation) layers, synchronized to **AWS S3 Cloud Storage**, and utilized to train **Random Forest & XGBoost Machine Learning models** to predict traffic congestion levels and travel delays in real-time. Results are visualized via **Power BI** and an **Interactive Web Dashboard**.

---

### Q2: What is the Medallion Architecture, and why did you use it?
**Answer**:
The Medallion Architecture is a data design pattern that organizes data into three distinct layers to guarantee data quality and auditability:
1. **Bronze Layer (Raw Landing)**: Stores raw, un-altered event streams in JSON/Parquet format with metadata columns (`ingestion_timestamp`, `data_layer="Bronze"`).
2. **Silver Layer (Cleaned & Enriched)**: Deduplicates records on `(city_name, timestamp)`, filters missing values or corrupt sensor outliers, and extracts temporal feature dimensions (`hour`, `day_of_week`, `is_weekend`).
3. **Gold Layer (Curated & Feature Mart)**: Joins Silver Traffic and Weather streams on `(city_name, hour)`, computes business KPI aggregations for dashboards, and builds ML feature tables for model training.

---

### Q3: How does your system handle network failures or Kafka broker downtimes?
**Answer**:
We implemented a **Zero-Downtime Local File Fallback Buffer** inside `kafka/city_producer.py`.
When the Kafka producer attempts to connect to the AWS EC2 broker (`13.217.6.185:9092`), if the broker is unreachable or network times out (`KafkaTimeoutError`), the producer catches the exception, logs a warning, and immediately dumps incoming JSON event streams locally to `data/bronze/<topic>/`.
When PySpark runs, it seamlessly ingests both Kafka topics and local landing directories, ensuring zero data loss.

---

### Q4: Why did you choose Parquet format over CSV or JSON for storage?
**Answer**:
1. **Columnar Compression**: Parquet is a columnar storage format, compressing data by 70-80% compared to JSON/CSV using Snappy compression.
2. **Predicate Pushdown & Column Pruning**: Queries scanning specific columns (e.g. `current_speed`) read only those column blocks rather than parsing full rows.
3. **PySpark Integration**: Parquet preserves explicit schema types (e.g. `TimestampType`, `DoubleType`) natively without needing re-parsing.

---

### Q5: How do your Machine Learning models work, and what are their target outputs?
**Answer**:
We trained two complementary ML models on the **Gold Feature Mart**:
1. **Random Forest Classifier**:
   - *Input Features*: `temperature`, `humidity`, `weather_severity_index`, `aqi`, `pm2_5`, `hour`, `day_of_week`, `is_weekend`.
   - *Target Output*: Categorical `congestion_level` (`Low`, `Medium`, `High`).
   - *Performance*: **88.89% Accuracy**, **87.41% F1-Score**.
2. **XGBoost Regressor**:
   - *Input Features*: Same environmental and temporal features.
   - *Target Output*: Continuous `speed_ratio` ($0.0 - 1.0$) and travel delay times.
   - *Performance*: **0.1288 RMSE**.

---

### Q6: What challenges did you face running PySpark on Windows, and how did you resolve them?
**Answer**:
1. **Java 17/21 Security Manager Issue**:
   - *Error*: `UnsupportedOperationException: getSubject is supported only if a security manager is allowed`.
   - *Fix*: Added JVM flags `.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")` in `SparkSessionManager`.
2. **Hadoop `winutils.exe` & Native DLL Issue**:
   - *Error*: `FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`.
   - *Fix*: Configured local `data/hadoop/bin/winutils.exe` and `hadoop.dll` binaries, formatted `HADOOP_HOME` with forward slashes (`as_posix()`), and updated `os.environ["PATH"]`.

---

### Q7: How does your AWS S3 Cloud Sync work?
**Answer**:
We built `aws/s3_uploader.py` using `boto3`. It recursively scans local `data/bronze/`, `data/silver/`, and `data/gold/` directories and uploads Parquet files to `s3://smartcity-traffic-analytics-swapnil/` in region `ap-south-1`. It tracks upload statistics and verifies S3 object counts.

---

### Q8: What are the key DAX measures you created in Power BI?
**Answer**:
- `Avg Congestion Ratio = AVERAGE(gold_analytics[avg_congestion_ratio])`
- `Peak Hour Congestion = CALCULATE([Avg Congestion Ratio], gold_analytics[hour] IN {8, 9, 17, 18, 19})`
- `Air Quality Index = AVERAGE(gold_analytics[avg_aqi])`
- `Weather Risk Level = SWITCH(TRUE(), AVERAGE(gold_analytics[avg_temperature]) > 35, "Extreme Heat", AVERAGE(gold_analytics[avg_aqi]) >= 4, "Hazardous AQI", "Normal")`

---

### Q9: How is your project structured for production deployment?
**Answer**:
The project adheres to modular Data Engineering best practices:
- **`config/`**: Centralized configuration management with `Pydantic` and `PyYAML`.
- **`utils/`**: Centralized logging (`logger.py`), custom exception hierarchy (`exceptions.py`), and master orchestrator (`run_full_pipeline.py`).
- **`etl/`**: Decoupled Medallion layer scripts (`bronze.py`, `silver.py`, `gold.py`).
- **`ml/`**: Machine Learning pipeline and model serialization (`models/`).

---

### Q10: How can you run the entire project end-to-end with a single command?
**Answer**:
By running:
```bash
python utils/run_full_pipeline.py
```
This automatically fetches live APIs, streams events to Kafka, executes PySpark Bronze/Silver/Gold transformations, syncs datasets to AWS S3, retrains ML models, and exports Power BI CSV reports.
