# Weather-Analysis-Pipeline-Airflow-Snowflake

An automated, production‑style data engineering and forecasting pipeline that ingests historical weather data from the Open‑Meteo API, loads it into Snowflake using an ETL workflow orchestrated by Apache Airflow, and generates 7‑day temperature forecasts using Snowflake ML Forecast. The system processes four U.S. cities: Newport Beach, Boston, Seattle, and Miami.

### Automated ETL · Snowflake ML Forecasting · Apache Airflow Orchestration

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache_Airflow-2.10.1-017CEE?style=flat-square&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Snowflake-ML_Forecast-29B5E8?style=flat-square&logo=snowflake&logoColor=white"/>
  <img src="https://img.shields.io/badge/Open--Meteo-Free_API-00C7B7?style=flat-square"/>
  <img src="https://img.shields.io/badge/Status-Active-22c55e?style=flat-square"/>
</p>

## What This Project Does

This system ingests 60 days of real historical weather data from four US cities, stores it in Snowflake, and uses Snowflake's native machine learning engine to generate a 7-day temperature forecast — all running on a fully automated daily schedule through Apache Airflow.

Two separate pipelines handle the two stages of work. The first pipeline runs at **02:30 UTC** and handles data collection, transformation, and storage. The second runs one hour later at **03:30 UTC**, trains the forecasting model on fresh data, generates predictions, and assembles the final output table that places historical actuals and future forecasts side by side.

# REPO STRUCTURE
    weather-analytics-pipeline-airflow-snowflake/
    │
    ├── dags/
    │   ├── weather_etl_pipeline.py  ← Airflow DAG 1: data collection & storage
    │   ├── weather_prediction.py    ← Airflow DAG 2: ML training & forecasting
    │
    ├── sql/
    │   ├── snowflake.sql            ← All DDL, setup queries & analysis SQL
    │
    ├── docs/
    │   ├── architecture-diagram.png
    │   ├── airflow-dag-graphs/
    │   │     ├── etl-dag.png
    │   │     ├── ml-dag.png
    │   ├── snowflake-screenshots/
    │   │     ├── raw-table.png
    │   │     ├── forecast-table.png
    │   │     ├── final-table.png
    │   ├── report.pdf
    │
    ├── config/
    │   ├── sample_airflow_variables.json
    │   ├── sample_airflow_connection.md
    │
    └── README.md

# SYSTEM ARCHITECTURE
    ┌────────────────────────────────────────────────────────────────┐
    │                        APACHE AIRFLOW                          │
    │                                                                │
    │  ┌──────────────────────────────────────────────────────────┐  │
    │  │  DAG 1: WeatherData_ETL  (Daily @ 02:30 UTC)             │  │
    │  │                                                          │  │
    │  │  Open-Meteo API  →  Extract  →  Transform  →  Load       │  │
    │  │  (4 cities, parallel tasks)        ↓                     │  │
    │  │                              Snowflake RAW.CITY_WEATHER  │  │
    │  └──────────────────────────────────────────────────────────┘  │
    │                              ↓                                 │
    │  ┌──────────────────────────────────────────────────────────┐  │
    │  │  DAG 2: TrainPredict      (Daily @ 03:30 UTC)            │  │
    │  │                                                          │  │
    │  │  RAW.CITY_WEATHER  →  Train Forecast Model               │  │
    │  │                              ↓                           │  │
    │  │                       Predict 7 Days                     │  │
    │  │                              ↓                           │  │
    │  │              ANALYTICS.CITY_WEATHER_FINAL                │  │
    │  │          (Historical UNION Forecast Results)             │  │
    │  └──────────────────────────────────────────────────────────┘  │
    └────────────────────────────────────────────────────────────────┘

# Cities Tracked
| # | City | State | Latitude | Longitude | Climate |
|---|------|-------|----------|-----------|---------|
| 1 | Miami | Florida | 25.7617° N | 80.1918° W | Tropical |
| 2 | Newport Beach | California | 33.6189° N | 117.9289° W | Mediterranean |
| 3 | Seattle | Washington | 47.6062° N | 122.3321° W | Oceanic |
| 4 | Boston | Massachusetts | 42.3601° N | 71.0589° W | Continental |

# Pipeline 1 — Weather ETL DAG
**DAG ID:** WeatherData_ETL
**Schedule:** 30 2 * * * (Daily at 02:30 UTC)
**File:** weather_etl_pipeline.py

**How It Works**
The ETL pipeline runs 4 parallel pipelines (one per city), each consisting of 3 tasks:

    extract  →  transform  →  load

# Task Breakdown
**extract(latitude, longitude)**
Calls the Open-Meteo Forecast API
Fetches past 60 days of daily weather data
Collects: temp_max, temp_min, temp_mean, precipitation, wind_speed, weather_code

    params = {
    "latitude": latitude,
    "longitude": longitude,
    "past_days": 60,
    "forecast_days": 0,
    "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "precipitation_sum", "windspeed_10m_max", "weathercode"],
    "timezone": "America/Los_Angeles"
    }
**transform(raw_data, latitude, longitude, city)**
Flattens the nested API JSON response
Converts data into a list of tuples, one per day
Returns clean records ready for loading
load(records, target_table)
Uses a MERGE (UPSERT) strategy via a temp staging table
Prevents duplicate records on re-runs
Wrapped in SQL transaction with try/except/rollback for data integrity

    MERGE INTO CITY_WEATHER t
    USING CITY_WEATHER_STAGE s
    ON t.CITY = s.CITY AND t.DATE = s.DATE
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...

Airflow Variables Used
City configurations are stored as an Airflow Variable (JSON) — no hardcoded values:

[
  {"city": "Miami",         "lat": 25.7617,  "lon": -80.1918},
  {"city": "Newport Beach", "lat": 33.6189,  "lon": -117.9289},
  {"city": "Seattle",       "lat": 47.6062,  "lon": -122.3321},
  {"city": "Boston",        "lat": 42.3601,  "lon": -71.0589}
]
Set via Admin → Variables → weather_cities in the Airflow UI

Pipeline 2 — TrainPredict DAG
DAG ID: TrainPredict
Schedule: 30 3 * * * (Daily at 03:30 UTC — runs 1 hour after ETL)
File: weather_prediction.py

How It Works
RAW.CITY_WEATHER  →  [train]  →  [predict]  →  ANALYTICS.CITY_WEATHER_FINAL
Task 1 train()
Creates a clean training view (ADHOC.CITY_WEATHER_TRAIN_VIEW) with non-null TEMP_MAX
Trains a native Snowflake ML Forecast Model using SNOWFLAKE.ML.FORECAST
Saves evaluation metrics to ANALYTICS.CITY_WEATHER_MODEL_METRICS
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.CITY_WEATHER_FORECAST_MODEL (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ADHOC.CITY_WEATHER_TRAIN_VIEW'),
    SERIES_COLNAME => 'CITY',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'TEMP_MAX',
    CONFIG_OBJECT => {'ON_ERROR': 'SKIP'}
);
Task 2 predict()
Runs the trained model to generate 7-day forecasts with 95% prediction intervals
Captures results using RESULT_SCAN(LAST_QUERY_ID())
Stores forecast in ADHOC.CITY_WEATHER_FORECAST
Creates the final union table combining historical actuals + forecast predictions:
-- Historical data
SELECT CITY, DATE, TEMP_MAX AS ACTUAL, NULL AS FORECAST, ...
FROM RAW.CITY_WEATHER

UNION ALL

-- ML Forecast
SELECT CITY, TS AS DATE, NULL AS ACTUAL, FORECAST, LOWER_BOUND, UPPER_BOUND
FROM ADHOC.CITY_WEATHER_FORECAST
Snowflake Schema & Tables
Database & Schema Layout
USER_DB_FERRET
├── RAW
│   └── CITY_WEATHER              ← Historical weather data (ETL target)
├── ADHOC
│   ├── CITY_WEATHER_TRAIN_VIEW   ← Clean view for ML training
│   └── CITY_WEATHER_FORECAST     ← Raw ML forecast output
└── ANALYTICS
    ├── CITY_WEATHER_FINAL        ← Historical + Forecast (final table)
    └── CITY_WEATHER_MODEL_METRICS ← Model evaluation metrics

RAW.CITY_WEATHER — Main History Table
Column	Type	Description
CITY	STRING	City name (part of PK)
LATITUDE	FLOAT	Geographic latitude
LONGITUDE	FLOAT	Geographic longitude
DATE	DATE	Observation date (part of PK)
TEMP_MAX	FLOAT	Daily max temperature (°C)
TEMP_MIN	FLOAT	Daily min temperature (°C)
TEMP_MEAN	FLOAT	Daily mean temperature (°C)
PRECIPITATION_MM	FLOAT	Total precipitation (mm)
WIND_SPEED_MAX_KMH	FLOAT	Max wind speed (km/h)
WEATHER_CODE	INTEGER	WMO weather interpretation code
LOAD_TS	TIMESTAMP_NTZ	Auto-set on insert (audit column)
Primary Key: (CITY, DATE) — ensures one record per city per day

ANALYTICS.CITY_WEATHER_FINAL — Final Output Table
Column	Type	Description
CITY	STRING	City name
DATE	DATE	Historical or forecast date
ACTUAL	FLOAT	Historical TEMP_MAX (NULL for forecast rows)
FORECAST	FLOAT	Predicted TEMP_MAX (NULL for historical rows)
LOWER_BOUND	FLOAT	95% prediction interval lower bound
UPPER_BOUND	FLOAT	95% prediction interval upper bound
Airflow Setup
Prerequisites
Apache Airflow 2.10+
apache-airflow-providers-snowflake
Snowflake account with TRAINING_ROLE and ML features enabled

Connections
Configure in Admin → Connections:

Conn ID	Type	Description
snowflake_conn	Snowflake	Points to USER_DB_FERRET, schema RAW
Variables
Configure in Admin → Variables:

Key	Type	Description
weather_cities	JSON	List of city objects with city, lat, lon
DAG Execution Order
02:30 UTC  →  WeatherData_ETL   (fetches + loads 60-day weather for 4 cities)
03:30 UTC  →  TrainPredict      (trains ML model + generates 7-day forecast)

Results & Forecast Output
The ANALYTICS.CITY_WEATHER_FINAL table contains a unified view of:

Historical actuals — 60 days of real weather observations
7-day forecast — ML-predicted max temperature with 95% confidence intervals
Sample Output Structure
CITY          | DATE       | ACTUAL | FORECAST | LOWER_BOUND | UPPER_BOUND
--------------|------------|--------|----------|-------------|------------
Boston        | 2026-03-05 | 8.2    | NULL     | NULL        | NULL
Boston        | 2026-03-06 | NULL   | 9.1      | 6.8         | 11.4
Newport Beach | 2026-03-05 | 21.4   | NULL     | NULL        | NULL
Newport Beach | 2026-03-06 | NULL   | 22.0     | 19.5        | 24.5
...


