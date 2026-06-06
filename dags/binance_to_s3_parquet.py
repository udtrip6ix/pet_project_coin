import logging
import requests
import pendulum
import duckdb
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "ud"
DAG_ID = "binance_to_s3_parquet"
LAYER = "raw"
SOURCE = "binance"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 5, 20, tz="UTC"),
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=30),
}

def get_and_transfer_api_data_to_s3(**context):
    SYMBOL = "BTCUSDT"
    INTERVAL = "1m"
    
    ds = context["data_interval_start"]
    target_date = ds.format("YYYY-MM-DD")
    
    logging.info(f"Start load Binance data for {target_date}")
    
    start_ts = int(ds.start_of('day').timestamp() * 1000)
    end_ts = int(ds.end_of('day').timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "startTime": start_ts,
        "endTime": end_ts,
        "limit": 1500 
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    if not data:
        logging.warning(f"No data received for {target_date}")
        return


    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume', 
        'close_time', 'qav', 'num_trades', 'taker_base', 'taker_quote', 'ignore'
    ])


    cols_to_numeric = ['open_time', 'open', 'high', 'low', 'close', 'volume']
    for col in cols_to_numeric:
        df[col] = pd.to_numeric(df[col])

    con = duckdb.connect(database=":memory:")
    con.sql(f"""
        INSTALL httpfs; LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_use_ssl=FALSE;
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
    """)

    con.register('candles_df', df)

    s3_path = f"s3://{LAYER}/{SOURCE}/{SYMBOL}/{target_date}.parquet"

    logging.info(f"Saving {len(df)} rows to {s3_path}")
    

    con.sql(f"""
        COPY (
            SELECT 
                to_timestamp(open_time / 1000) AS open_time,
                open::DOUBLE AS open,
                high::DOUBLE AS high,
                low::DOUBLE AS low,
                close::DOUBLE AS close,
                volume::DOUBLE AS volume
            FROM candles_df
        ) TO '{s3_path}' (FORMAT 'PARQUET', OVERWRITE TRUE);
    """)

    con.close()
    logging.info("Load finished successfully!")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=True,
    max_active_runs=1,
    tags=[LAYER, SOURCE]
) as dag:

    start = EmptyOperator(task_id="start")

    extract_and_load = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )
    
    end = EmptyOperator(task_id="end")

    start >> extract_and_load >> end