from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests

# Constants
DBT_PROJECT_DIR = "/opt/airflow/dbt"

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(url):
    data = requests.get(url)
    return data.json()

@task
def transform(stock_1, stock_2, data1, data2):
    results = []
    for d in data1["Time Series (Daily)"]:
        stock_info = data1["Time Series (Daily)"][d]
        stock_info['6. date'] = d
        results.append({'0. stock': stock_1} | stock_info)
        if len(results) > 89:
            break

    for d in data2["Time Series (Daily)"]:
        stock_info = data2["Time Series (Daily)"][d]
        stock_info['6. date'] = d
        results.append({'0. stock': stock_2} | stock_info)
        if len(results) > 179:
            break
    return results

@task
def load(cur, records, target_table):
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {target_table};")
        cur.execute(f"CREATE OR REPLACE TABLE {target_table} (stock string, open float, high float, low float, close float, volume int, date timestamp);")
        for r in records:
            sql = f"INSERT INTO {target_table} (stock, open, high, low, close, volume, date) VALUES ('{r['0. stock']}', {r['1. open']}, {r['2. high']}, {r['3. low']}, {r['4. close']}, {r['5. volume']}, '{r['6. date']}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

@task
def train(cur, train_input_table, train_view, forecast_function_name):
    create_view_sql = f"CREATE OR REPLACE VIEW {train_view} AS SELECT DATE, CLOSE, STOCK FROM {train_input_table};"
    create_model_sql = f"CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'), SERIES_COLNAME => 'STOCK', TIMESTAMP_COLNAME => 'DATE', TARGET_COLNAME => 'CLOSE', CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }});"
    
    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID; 
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT STOCK, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '"', '') AS STOCK, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='combined_stocks_analysis',
    default_args=default_args,
    start_date=datetime(2024, 10, 14),
    catchup=False,
    tags=['ETL', 'ML'],
    schedule_interval='@daily'  # Adjust the schedule according to your need
) as dag:
    
    target_table = "dev.raw_data.market_data"
    url_1 = Variable.get("stock_1")
    url_2 = Variable.get("stock_2")
    stock_1 = Variable.get("symbol_1")
    stock_2 = Variable.get("symbol_2")

    # Task to load data into Snowflake
    cur = return_snowflake_conn()
    
    data1 = extract(url_1)
    data2 = extract(url_2)
    records = transform(stock_1, stock_2, data1, data2)

    load_task = load(cur, records, target_table)

    # DBT Tasks
    dbt_run_lab1 = BashOperator(
        task_id="dbt_run_lab1",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --profiles-dir {DBT_PROJECT_DIR} --select tag:lab1 --full-refresh
        """,
        env={
            'DBT_PROFILE_PATH': f"{DBT_PROJECT_DIR}/profiles.yml",
            'DBT_PROJECT_DIR': DBT_PROJECT_DIR,
        },
    )

    dbt_run_forecast = BashOperator(
        task_id="dbt_run_forecast",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --profiles-dir {DBT_PROJECT_DIR} --select tag:prep_for_training --full-refresh
        """,
        env={
            'DBT_PROFILE_PATH': f"{DBT_PROJECT_DIR}/profiles.yml",
            'DBT_PROJECT_DIR': DBT_PROJECT_DIR,
        },
    )

    # Train and Predict tasks
    train_task = train(cur, target_table, "dev.adhoc.market_data_view", "dev.analytics.predict_stock_price")
    predict_task = predict(cur, "dev.analytics.predict_stock_price", target_table, "dev.adhoc.market_data_forecast", "dev.analytics.market_data")

    # Define task dependencies
    load_task >> dbt_run_lab1 >> dbt_run_forecast >> train_task >> predict_task
