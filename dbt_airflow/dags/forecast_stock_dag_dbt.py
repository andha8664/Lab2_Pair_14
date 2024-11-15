from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import snowflake.connector
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


def extract(url):
    data = requests.get(url)
    return data.json()


def transform(stock_1, stock_2, data1, data2):
    results = []
    for d in data1["Time Series (Daily)"]:
        stock_info = data1["Time Series (Daily)"][d]
        stock_info["6. date"] = d
        results.append({"0. stock": stock_1} | stock_info)
        if len(results) > 89:
            break

    for d in data2["Time Series (Daily)"]:
        stock_info = data2["Time Series (Daily)"][d]
        stock_info["6. date"] = d
        results.append({"0. stock": stock_2} | stock_info)
        if len(results) > 179:
            break
    return results


def load(con, records, target_table):
    try:
        con.execute("BEGIN;")
        con.execute(f"DROP TABLE IF EXISTS {target_table};")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target_table} (
                stock string, 
                open float, 
                high float, 
                low float, 
                close float, 
                volume int, 
                date timestamp
            );
        """
        )
        for r in records:
            sql = f"""
                INSERT INTO {target_table} 
                (stock, open, high, low, close, volume, date) 
                VALUES (
                    '{r["0. stock"]}', 
                    {r["1. open"]}, 
                    {r["2. high"]}, 
                    {r["3. low"]}, 
                    {r["4. close"]}, 
                    {r["5. volume"]}, 
                    '{r["6. date"]}'
                )
            """
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


def call_lab1_dag(**context):
    target_table = "dev.raw_data.market_data"
    url_1 = Variable.get("stock_1")
    url_2 = Variable.get("stock_2")
    stock_1 = Variable.get("symbol_1")
    stock_2 = Variable.get("symbol_2")

    cur = return_snowflake_conn()
    data1 = extract(url_1)
    data2 = extract(url_2)
    records = transform(stock_1, stock_2, data1, data2)
    load(cur, records, target_table)


def train(cur, train_input_table, train_view, forecast_function_name):
    create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS 
        SELECT DATE, CLOSE, STOCK
        FROM {train_input_table};
    """

    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'STOCK',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );
    """

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise


def predict(
    cur, forecast_function_name, train_input_table, forecast_table, final_table
):
    make_prediction_sql = f"""
        BEGIN
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS 
            SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;
    """

    create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {final_table} AS
        SELECT 
            STOCK, 
            DATE, 
            CLOSE AS actual, 
            NULL AS forecast, 
            NULL AS lower_bound, 
            NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT 
            replace(series, '"', '') as STOCK, 
            ts as DATE, 
            NULL AS actual, 
            forecast, 
            lower_bound, 
            upper_bound
        FROM {forecast_table};
    """

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


def call_train_predict_dag(**context):
    train_input_table = "dev.raw_data.market_data"
    train_view = "dev.adhoc.market_data_view"
    forecast_table = "dev.adhoc.market_data_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.market_data"

    cur = return_snowflake_conn()
    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "stock_forecast_dbt_dag",
    default_args=default_args,
    description="Stock forecasting pipeline with DBT integration",
    start_date=datetime(2024, 10, 14),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to find the DBT directory that contains profiles.yml
    find_dbt_directory = BashOperator(
        task_id="find_dbt_directory",
        bash_command='find / -type f -name "profiles.yml" -exec dirname {} \\; | head -n 1',
        do_xcom_push=True,  # Push the output to XCom
    )

    # Task to set the DBT project directory
    set_dbt_directory = BashOperator(
        task_id="set_dbt_directory",
        bash_command='echo $(find / -type f -name "profiles.yml" -exec dirname {} \\; | head -n 1) > /tmp/dbt_project_dir',
        do_xcom_push=False,
    )

    # Task to debug the found directory
    debug_task = BashOperator(
        task_id="debug_task",
        bash_command="cat /tmp/dbt_project_dir",
    )

    # Tasks for running DBT
    dbt_deps = BashOperator(
    task_id="dbt_deps",
    bash_command=f"""
        DBT_PROJECT_DIR=$(cat /tmp/dbt_project_dir) &&
        export DBT_ACCOUNT='{Variable.get("DBT_ACCOUNT")}' &&
        export DBT_USER='{Variable.get("DBT_USER")}' &&
        export DBT_PASSWORD='{Variable.get("DBT_PASSWORD")}' &&
        cd $DBT_PROJECT_DIR &&
        dbt deps --profiles-dir $DBT_PROJECT_DIR || echo "DBT deps failed with exit code $?"
    """,
    retries=2,
    retry_delay=timedelta(minutes=5),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            DBT_PROJECT_DIR=$(cat /tmp/dbt_project_dir) &&
            export DBT_ACCOUNT='{Variable.get("DBT_ACCOUNT")}' &&
            export DBT_USER='{Variable.get("DBT_USER")}' &&
            export DBT_PASSWORD='{Variable.get("DBT_PASSWORD")}' &&
            cd $DBT_PROJECT_DIR &&
            dbt run --profiles-dir $DBT_PROJECT_DIR --full-refresh || echo "DBT run failed with exit code $?"
        """,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            DBT_PROJECT_DIR=$(cat /tmp/dbt_project_dir) &&
            export DBT_ACCOUNT='{Variable.get("DBT_ACCOUNT")}' &&
            export DBT_USER='{Variable.get("DBT_USER")}' &&
            export DBT_PASSWORD='{Variable.get("DBT_PASSWORD")}' &&
            cd $DBT_PROJECT_DIR &&
            dbt test --profiles-dir $DBT_PROJECT_DIR || echo "DBT test failed with exit code $?"
        """,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
            DBT_PROJECT_DIR=$(cat /tmp/dbt_project_dir) &&
            export DBT_ACCOUNT='{Variable.get("DBT_ACCOUNT")}' &&
            export DBT_USER='{Variable.get("DBT_USER")}' &&
            export DBT_PASSWORD='{Variable.get("DBT_PASSWORD")}' &&
            cd $DBT_PROJECT_DIR &&
            dbt snapshot --profiles-dir $DBT_PROJECT_DIR || echo "DBT snapshot failed with exit code $?"
        """,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Define Python Operator tasks to call your functions
    call_extract_transform_load = PythonOperator(
        task_id='call_extract_transform_load',
        python_callable=call_lab1_dag,
        provide_context=True,
        dag=dag
    )

    call_train_predict = PythonOperator(
        task_id='call_train_predict',
        python_callable=call_train_predict_dag,
        provide_context=True,
        dag=dag
    )
    
    # Update your task dependencies to include the new tasks
    (
        find_dbt_directory
        >> set_dbt_directory
        >> debug_task
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> dbt_snapshot
        >> call_extract_transform_load
        >> call_train_predict
    )
