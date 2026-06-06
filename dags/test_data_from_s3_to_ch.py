from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum
from airflow.sensors.external_task import ExternalTaskSensor

MINIO_ACCESS = Variable.get("access_key")
MINIO_SECRET = Variable.get("secret_key")
CH_USER = Variable.get("ch_user")
CH_PASSWORD = Variable.get("ch_password")

OWNER = "ud"
DAG_ID = 'test_s3_to_clickhouse'

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 5, 20, tz="Europe/Moscow"),
    "retries": 4,
    "retry_delay": pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='0 5 * * *',
    catchup=True
) as dag:

    start = EmptyOperator(task_id='start')

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/spark/apps/test_etl_job.py',
        conn_id='spark_default',
        jars='/jars/clickhouse-spark-runtime.jar,/jars/hadoop-aws-3.3.4.jar,/jars/aws-java-sdk-bundle-1.12.262.jar',
        conf={
            "spark.driver.extraClassPath": "/jars/clickhouse-spark-runtime.jar:/jars/hadoop-aws-3.3.4.jar:/jars/aws-java-sdk-bundle-1.12.262.jar",
            "spark.executor.extraClassPath": "/jars/clickhouse-spark-runtime.jar:/jars/hadoop-aws-3.3.4.jar:/jars/aws-java-sdk-bundle-1.12.262.jar",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS,
            "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        application_args=[
            "--date", "{{ ds }}", 
            "--coin_id", "bitcoin",
            "--ch_user", CH_USER,
            "--ch_password", CH_PASSWORD
        ]
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="test_raw_from_api_to_s3",
        external_task_id="get_and_transfer_api_data_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600, 
        poke_interval=60,
    )
    
    end = EmptyOperator(task_id='end')

    start >> sensor_on_raw_layer >> submit_spark_job >> end