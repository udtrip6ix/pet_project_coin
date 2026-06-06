from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from clickhouse_driver import Client
import argparse
import sys
import traceback

def prepare_and_run_etl():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--ch_user", required=True)
    parser.add_argument("--ch_password", required=True)
    args = parser.parse_args()


    client = Client(host='clickhouse', user=args.ch_user, password=args.ch_password)
    client.execute("CREATE DATABASE IF NOT EXISTS stg")
    
    client.execute("""
        CREATE TABLE IF NOT EXISTS stg.binance_candles
        (
            open_time DateTime,
            symbol String,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        PARTITION BY toYYYYMM(open_time)
        ORDER BY (symbol, open_time)
    """)


    spark = SparkSession.builder \
        .appName("ETL_Binance_S3_to_CH") \
        .getOrCreate()

    try:

        path = f"s3a://raw/binance/{args.symbol}/{args.date}.parquet"
        df = spark.read.parquet(path)
        
        print("--- SCHEMA DETECTED ---")
        df.printSchema()
        

        df_final = df.select(
            col("open_time").cast("timestamp").alias("open_time"), 
            lit(args.symbol).alias("symbol"),
            col("open").cast("double"),
            col("high").cast("double"),
            col("low").cast("double"),
            col("close").cast("double"),
            col("volume").cast("double"),
            current_timestamp().alias("updated_at")
        )


        total_rows = df_final.count()
        print(f"--- Подготовлено к записи: {total_rows} строк ---")

        if total_rows > 0:

            df_final.write \
                .format("jdbc") \
                .option("url", "jdbc:clickhouse://clickhouse:8123/stg") \
                .option("dbtable", "stg.binance_candles") \
                .option("user", args.ch_user) \
                .option("password", args.ch_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()
            print("--- Запись в ClickHouse успешно завершена ---")
        else:
            print("--- Данных нет, запись пропущена ---")

    except Exception:
        print("ПОЛНАЯ ОШИБКА:")
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    prepare_and_run_etl()