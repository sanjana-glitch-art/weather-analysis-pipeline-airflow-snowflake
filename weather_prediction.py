from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Snowflake Connection Helper
def return_snowflake_conn(conn_id="snowflake_conn"):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn, conn.cursor()

# DAG CONFIGURATION
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# inside the WITH block, we define the DAG and its tasks
with DAG(
    dag_id="TrainPredict",
    start_date=datetime(2026, 3, 1), # date from which airflow starts scheduling this DAG
    schedule="30 3 * * *",   # runs after ETL DAG, runs daily at 3:30
    catchup=False, # will not backfill, only run from now on
    default_args=default_args,
    tags=["ML", "Forecast", "Weather", "Snowflake"],
) as dag:
# shared congiguration variables
    train_input_table = "RAW.CITY_WEATHER" # source table with historical weather data
    train_view = "ADHOC.CITY_WEATHER_TRAIN_VIEW" # view that will be created for clean training data
    model_name = "ANALYTICS.CITY_WEATHER_FORECAST_MODEL" # fully qualified name of the snowflake ML forecast model 
    forecast_table = "ADHOC.CITY_WEATHER_FORECAST" # table to store dorecast results
    final_table = "ANALYTICS.CITY_WEATHER_FINAL" # final combined table (historical + forecast)
    metrics_table = "ANALYTICS.CITY_WEATHER_MODEL_METRICS" # table to store model evaluation metrics

# TASK 1 — TRAIN MODEL
    @task # decorator turns this function into an Airflow task
    def train(): 

        conn, cur = return_snowflake_conn() # calls for snowflake connection and cursor

        try:

            cur.execute("USE DATABASE USER_DB_FLAMINGO")
            cur.execute("USE SCHEMA RAW")

            cur.execute("BEGIN") # starts a transaction, all subsequent SQL will be a part of this transaction until COMMIT or ROLLBACK

            # -------------------------------------------
            # Create Clean Training View
            # -------------------------------------------
            create_view_sql = f"""
            CREATE OR REPLACE VIEW {train_view} AS
            SELECT
                CITY,
                DATE,
                TEMP_MAX
            FROM {train_input_table}
            WHERE TEMP_MAX IS NOT NULL
            ORDER BY CITY, DATE;
            """

            cur.execute(create_view_sql)

            # -------------------------------------------
            # Train Snowflake Forecast Model
            # -------------------------------------------
            create_model_sql = f"""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name}
            (
                INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
                SERIES_COLNAME => 'CITY',
                TIMESTAMP_COLNAME => 'DATE',
                TARGET_COLNAME => 'TEMP_MAX',
                CONFIG_OBJECT => {{
                    'ON_ERROR': 'SKIP'
                }}
            );
            """

            cur.execute(create_model_sql)

            # -------------------------------------------
            # Store Evaluation Metrics
            # -------------------------------------------
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {metrics_table}
            AS
            SELECT CURRENT_TIMESTAMP() AS RUN_TS,
                   *
            FROM TABLE({model_name}!SHOW_EVALUATION_METRICS())
            LIMIT 0
            """)

            cur.execute(f"""
            INSERT INTO {metrics_table}
            SELECT CURRENT_TIMESTAMP() AS RUN_TS,
                   *
            FROM TABLE({model_name}!SHOW_EVALUATION_METRICS())
            """)

            cur.execute("COMMIT")

        except Exception as e:

            cur.execute("ROLLBACK")
            raise

        finally:

            cur.close()
            conn.close()


# TASK 2 — GENERATE FORECAST
    @task
    def predict():

        conn, cur = return_snowflake_conn()

        try:
            # run forecast  
            cur.execute(f"""
            CALL {model_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            )
            """)

            # get query ID of the forecast result
            cur.execute("SELECT LAST_QUERY_ID()")
            query_id = cur.fetchone()[0]

            # Store Forecast Results
            cur.execute(f"""
            CREATE OR REPLACE TABLE {forecast_table} AS
            SELECT *
            FROM TABLE(RESULT_SCAN('{query_id}'))
            """)

            # Create Final Table (Historical + Forecast)
            cur.execute(f"""
            CREATE OR REPLACE TABLE {final_table} AS

            SELECT
                CITY,
                DATE,
                TEMP_MAX AS ACTUAL,
                NULL AS FORECAST,
                NULL AS LOWER_BOUND,
                NULL AS UPPER_BOUND
            FROM {train_input_table}

            UNION ALL

            SELECT
                REPLACE(SERIES, '"', '') AS CITY,
                TS AS DATE,
                NULL AS ACTUAL,
                FORECAST,
                LOWER_BOUND,
                UPPER_BOUND
            FROM {forecast_table}
            """)

            print("Forecast + Final Table Created Successfully")

        except Exception as e:
            raise

        finally:
            cur.close()
            conn.close()


# DAG ORDER
    train_task = train()
    predict_task = predict()
    train_task >> predict_task