from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from clickhouse_driver import Client
import argparse
import sys
import time

def prepare_and_run_etl():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--coin_id", required=True)
    parser.add_argument("--ch_user", required=True)
    parser.add_argument("--ch_password", required=True)
    args = parser.parse_args()


    client = Client(host='clickhouse', user=args.ch_user, password=args.ch_password)
    client.execute("CREATE DATABASE IF NOT EXISTS stg")
    

    client.execute("""
        CREATE TABLE IF NOT EXISTS stg.coin_history
        (
            date Date,
            coin_id String,
            price_usd Nullable(Float64),
            market_cap_usd Nullable(Float64),
            total_volume_usd Nullable(Float64)
        )
        ENGINE = MergeTree()
        ORDER BY (coin_id, date)
    """)
    

    client.execute("DROP TABLE IF EXISTS stg.coin_history_staging")
    client.execute("""
        CREATE TABLE stg.coin_history_staging 
        AS stg.coin_history 
        ENGINE = Memory
    """)


    spark = SparkSession.builder.appName("ETL_S3_to_CH").getOrCreate()

    try:
        df = spark.read.parquet(f"s3a://raw/coingecko/{args.coin_id}/{args.date}.parquet")
        
        df = df.select(
            col("date").cast("date"),
            col("coin_id"),
            col("price_usd").cast("double"),
            col("market_cap_usd").cast("double"),
            col("total_volume_usd").cast("double")
        )
        

        df.write \
            .format("clickhouse") \
            .option("host", "clickhouse") \
            .option("table", "stg.coin_history_staging") \
            .option("user", args.ch_user) \
            .option("password", args.ch_password) \
            .option("create_table", "false") \
            .mode("append") \
            .save()


        print("--- Перенос данных в финальную таблицу ---")
        client.execute(f"ALTER TABLE stg.coin_history DELETE WHERE date = '{args.date}' AND coin_id = '{args.coin_id}'")
        client.execute("INSERT INTO stg.coin_history SELECT * FROM stg.coin_history_staging")
        client.execute("TRUNCATE TABLE stg.coin_history_staging")
        print("--- Успешно завершено ---")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    prepare_and_run_etl()