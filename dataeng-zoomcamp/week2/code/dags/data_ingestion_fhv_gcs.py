import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


from utils import upload_to_gcs, format_to_parquet

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

start_month = '{{ execution_date.strftime(\'%Y-%m\') }}'
dataset_file = f"fhv_tripdata_{start_month}.csv"
dataset_url = f"https://nyc-tlc.s3.amazonaws.com/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2019, 1, 1),
    "end_date": datetime(2019, 12, 1),
    "catchup": True,
    "retries" : 1,
    "depends_on_past": False
}

dag = DAG(
    dag_id="ingest_dag_fhv",
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de']
)

with dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/fhv_tripdata/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "fhv_tripdata",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/fhv_tripdata/*.parquet"],
            },
        },
    )

    delete_file_task = BashOperator(
        task_id = "delete_file_task",
        bash_command= f"rm -f {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> delete_file_task

