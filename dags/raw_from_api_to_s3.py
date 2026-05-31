import duckdb
import logging
import requests
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER="ud"
DAG_ID="raw_from_api_to_s3"

LAYER="raw"
SOURCE="coin"

ACCESS_KEY=Variable.get("access_key")
SECRET_KEY=Variable.get("secret_key")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

default_args={
    "owner":OWNER,
    "start_date":pendulum.datetime(2026, 5,20, tz="Europe/Moscow"),
    "retries":4,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context):
    """"""
    target_date=context["data_interval_start"].format("YYYY-MM-DD")
    return target_date

def get_and_transfer_api_data_to_s3(**context):
    """"""

    COIN_ID="bitcoin"

    target_date = get_dates(**context)
    logging.info(f"start load for dates: {target_date}")
    
    coingecko_date=pendulum.parse(target_date).format("DD-MM-YYYY")


    url=f"https://api.coingecko.com/api/v3/coins/{COIN_ID}/history"
    params={
        "date": coingecko_date,
        "localization":"false"
    }

    response=requests.get(url, params=params)
    response.raise_for_status()
    data=response.json()

    market_data=data.get("market_data", {})
    row={
        "date":target_date,
        "coin_id":COIN_ID,
        "price_usd": market_data.get("current_price", {}).get("usd"),
        "market_cap_usd":market_data.get("market_cap",{}).get("usd"),
        "total_volume_usd":market_data.get("total_volume",{}).get("usd")
    }

    con=duckdb.connect(database=":memory:")
    con.sql(
        f"""
        SET TIMEZONE='UTC';

        SET memory_limit='2GB';
        SET max_threads=2;

        install httpfs;
        load httpfs;

        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_use_ssl=FALSE;

        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        """
    )    

    con.sql("CREATE TABLE temp_coin AS SELECT * FROM row")

    s3_path= f"s3://raw/coingecko/{COIN_ID}/{target_date}.parquet"

    logging.info(f"saving data to {s3_path}")
    con.sql(f"COPY temp_coin TO '{s3_path}' (FORMAT 'PARQUET')")

    con.close()
    logging.info("load finished successfully!")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup= True,
    description=SHORT_DESCRIPTION,
    max_active_runs=1,
    tags=[LAYER, SOURCE]

) as dag:
    
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    extract_and_load = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )
    
    end=EmptyOperator(task_id="end")

    start >> extract_and_load >> end