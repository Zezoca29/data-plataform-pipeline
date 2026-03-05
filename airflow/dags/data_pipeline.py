from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from minio import Minio


def ensure_minio_bucket() -> None:
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False,
    )
    bucket_name = "lakehouse"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_data_platform",
    default_args=default_args,
    description="Pipeline end-to-end: ingestão, streaming, lake, dbt e DW",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "streaming", "lakehouse"],
) as dag:
    init_lake = PythonOperator(
        task_id="ensure_minio_bucket",
        python_callable=ensure_minio_bucket,
    )

    start_ingestion = BashOperator(
        task_id="start_ingestion",
        bash_command="python /opt/airflow/ingestion/api_collector.py & sleep 20",
    )

    run_stream_processor = BashOperator(
        task_id="run_spark_streaming_processor",
        bash_command=(
            "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.postgresql:postgresql:42.7.3 /opt/airflow/spark_jobs/process_data.py"
        ),
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
    )

    run_analytics_check = PostgresOperator(
        task_id="run_analytics_check",
        postgres_conn_id="warehouse_postgres",
        sql="""
        insert into public.pipeline_audit (pipeline_name, status, executed_at)
        values ('crypto_data_platform', 'success', now());
        """,
    )

    init_lake >> start_ingestion >> run_stream_processor >> run_dbt_models >> run_analytics_check
