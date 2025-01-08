from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator)
from datetime import datetime

default_args = {
    'retries': 1
}

dag1 = DAG(
    'modeling_tables_from_bq_amira',
    default_args=default_args,
    description='Model Data in BigQuery by applying facts and dimensions',
    schedule_interval=None,
    start_date=datetime(2025, 1, 8),
    catchup=False,
)

PROJECT_ID = 'ready-de-25'
DATASET_ID = 'olist_amira'
TARGET_DATASET_ID = 'olist_modeling_amira'

sql_dim_customers = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_customers` AS
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM `{PROJECT_ID}.{DATASET_ID}.customers`;
"""

sql_dim_geolocation = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_geolocation` AS
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM `{PROJECT_ID}.{DATASET_ID}.geolocation`;
"""

sql_dim_products = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_products` AS
SELECT
    p.product_id,
    t.string_field_1 AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM 
    `{PROJECT_ID}.{DATASET_ID}.products` p
LEFT JOIN 
    `{PROJECT_ID}.{TARGET_DATASET_ID}.product_category_name_translation` t
    ON p.product_category_name = t.string_field_0;

"""

sql_dim_sellers = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_sellers` AS
SELECT
    seller_id,
    seller_city,
    seller_state,
    seller_zip_code_prefix
FROM `{PROJECT_ID}.{DATASET_ID}.sellers`;
"""

sql_dim_payment = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_payment` AS
SELECT
    order_id,
    payment_type,
    payment_value
FROM `{PROJECT_ID}.{DATASET_ID}.order_payment`;
"""

sql_dim_dates = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_dates` AS
SELECT
    order_id,
    order_purchase_timestamp,
    order_estimated_delivery_date
FROM `{PROJECT_ID}.{DATASET_ID}.orders`;
"""

sql_fact_orders = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET_ID}.fact_orders` AS
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM `{PROJECT_ID}.{DATASET_ID}.order_items`;
"""

create_dim_customers = BigQueryInsertJobOperator(
    task_id='create_dim_customers',
    configuration={
        "query": {
            "query": sql_dim_customers,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_dim_geolocation = BigQueryInsertJobOperator(
    task_id='create_dim_geolocation',
    configuration={
        "query": {
            "query": sql_dim_geolocation,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_dim_products = BigQueryInsertJobOperator(
    task_id='create_dim_products',
    configuration={
        "query": {
            "query": sql_dim_products,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_dim_sellers = BigQueryInsertJobOperator(
    task_id='create_dim_sellers',
    configuration={
        "query": {
            "query": sql_dim_sellers,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_dim_payment = BigQueryInsertJobOperator(
    task_id='create_dim_payment',
    configuration={
        "query": {
            "query": sql_dim_payment,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_dim_dates = BigQueryInsertJobOperator(
    task_id='create_dim_dates',
    configuration={
        "query": {
            "query": sql_dim_dates,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_fact_orders = BigQueryInsertJobOperator(
    task_id='create_fact_orders',
    configuration={
        "query": {
            "query": sql_fact_orders,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_dim_customers >> create_dim_geolocation >> create_dim_products >> create_dim_sellers >> create_dim_payment >> create_dim_dates >> create_fact_orders