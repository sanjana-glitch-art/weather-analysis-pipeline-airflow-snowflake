# weather-analysis-pipeline-airflow-snowflake

An automated, production‑style data engineering and forecasting pipeline that ingests historical weather data from the Open‑Meteo API, loads it into Snowflake using an ETL workflow orchestrated by Apache Airflow, and generates 7‑day temperature forecasts using Snowflake ML Forecast. The system processes four U.S. cities: Newport Beach, Boston, Seattle, and Miami.

Powered by Snowflake ML · Apache Airflow · Open-Meteo API

# Table of Contents
Overview
System Architecture
Cities Tracked
Project Structure
Pipeline 1 — Weather ETL DAG
Pipeline 2 — Train & Predict DAG
Snowflake Schema & Tables
Airflow Setup
Screenshots
Key SQL Queries
Results & Forecast Output
Lessons Learned

# OVERVIEW
This project demonstrates a complete cloud‑based data engineering workflow:
- Automated ETL pipeline (Extract → Transform → Load)
- Automated ML forecasting pipeline
- Snowflake as the central data warehouse
- Airflow for orchestration and scheduling
- Open‑Meteo API as the data source
- Unified analytics table combining historical and forecasted weather metrics
The architecture is modular, scalable, and designed to mirror real‑world data engineering systems.

# REPO STRUCTURE
