from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (PostgresToGCSOperator)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (GCSToBigQueryOperator)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from google.cloud import storage


GCS_BUCKET = "ready-d25-postgres-to-gcs"
PROJECT_ID = "ready-de-25"
BQ_DATASET = "olist_amira"

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag1 = DAG(
    'Transfer_tables_to_bq_amira',
    default_args=default_args,
    start_date=datetime(2025, 1, 7),
    description='Transfer postgres to gcs and from gcs to bigquery',
    schedule_interval=None
)

postgres_tables = [
    "customers", "geolocation", "order_items", "orders", "product_category_name_translation", "products"
]

for table in postgres_tables:
    transfer_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id=f'transfer_postgres_to_gcs_{table}',
        postgres_conn_id="postgres_z",
        sql=f"SELECT * FROM {table}",
        bucket=GCS_BUCKET,
        filename=f"amira/{table}/{table}.csv",
        export_format='csv',
        dag=dag1
    )
    load_from_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id=f'load_gcs_to_bigquery_{table}',
        bucket=GCS_BUCKET,
        source_objects=[f"amira/{table}/{table}.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{table}",
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format='csv',
        autodetect=True,
        dag=dag1
    )

    transfer_from_postgres_to_gcs >> load_from_gcs_to_bigquery

api_endpoints = {
    "order_payment": "https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table",
    "sellers": "https://us-central1-ready-de-25.cloudfunctions.net/sellers_table",
}

def fetch_csv_and_upload_to_gcs(api_url, gcs_object_name):
    response = requests.get(api_url)
    response.raise_for_status()
    csv_data = response.text

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_object_name)
    blob.upload_from_string(csv_data, content_type='text/csv')

for table, api_url in api_endpoints.items():
    fetch_api_task = PythonOperator(
        task_id=f'fetch_api_{table}',
        python_callable=fetch_csv_and_upload_to_gcs,
        op_kwargs={
            'api_url': api_url,
            'gcs_object_name': f"amira/{table}/{table}.csv",
        },
        dag=dag1,
    )

    load_from_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id=f'gcs_to_bigquery_{table}',
        bucket=GCS_BUCKET,
        source_objects=[f"amira/{table}/{table}.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{table}",
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        autodetect=True,
        dag=dag1,
    )

    fetch_api_task >> load_from_gcs_to_bigquery
