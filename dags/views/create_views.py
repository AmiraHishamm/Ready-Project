from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator)
from datetime import datetime

default_args = {
    'retries': 1
}

dag1 = DAG(
    'create_views_from_bq',
    default_args=default_args,
    description='Model Data in BigQuery: Dimensions and Fact Tables',
    schedule_interval=None,
    start_date=datetime(2025, 1, 8),
    catchup=False,
)

PROJECT_ID = 'ready-de-25'
TARGET_DATASET_ID = 'olist_modeling_amira'

sql_view_top_customers = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TARGET_DATASET_ID}.top_customers_view` AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    SUM(oi.price * oi.freight_value) AS total_order_value
FROM
    `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_customers` c
JOIN
    `{PROJECT_ID}.{TARGET_DATASET_ID}.fact_orders` oi ON c.customer_id = oi.order_id
GROUP BY
    c.customer_id, c.customer_unique_id
ORDER BY
    total_order_value DESC;
"""

sql_orders_avg = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TARGET_DATASET_ID}.avg_orders_view` AS
SELECT
    c.customer_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.order_id) / COUNT(DISTINCT c.customer_id) AS avg_orders_per_customer
FROM
    `{PROJECT_ID}.{TARGET_DATASET_ID}.fact_orders` o
JOIN
    `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_customers` c ON o.order_id = c.customer_id  -- Join to get customer_id from dim_customers
GROUP BY
    c.customer_id;

"""


sql_view_top_products = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TARGET_DATASET_ID}.top_selling_products_view` AS
SELECT
    p.product_id,
    t.string_field_1 AS product_category_name_english,
    SUM(oi.quantity) AS total_quantity_sold
FROM
    `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_products` p
JOIN
    `{PROJECT_ID}.{TARGET_DATASET_ID}.fact_orders` oi ON p.product_id = oi.product_id
LEFT JOIN
    `{PROJECT_ID}.{TARGET_DATASET_ID}.product_category_name_translation` t
    ON p.product_category_name = t.string_field_0 
GROUP BY
    p.product_id, t.string_field_1
ORDER BY
    total_quantity_sold DESC;


"""

sql_view_total_number_of_orders = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TARGET_DATASET_ID}.total_orders_per_month_view` AS
SELECT
    EXTRACT(YEAR FROM o.order_purchase_timestamp) AS order_year,
    EXTRACT(MONTH FROM o.order_purchase_timestamp) AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM
    `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_dates` o
GROUP BY
    order_year, order_month
ORDER BY
    order_year, order_month;

"""

sql_view_payment = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TARGET_DATASET_ID}.payment_method_view` AS
SELECT
    p.payment_type,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.order_id) / (SELECT COUNT(DISTINCT order_id) FROM `{PROJECT_ID}.{TARGET_DATASET_ID}.fact_orders`) AS payment_share
FROM
    `{PROJECT_ID}.{TARGET_DATASET_ID}.dim_payment` p
JOIN
    `{PROJECT_ID}.{TARGET_DATASET_ID}.fact_orders` o ON p.order_id = o.order_id
GROUP BY
    p.payment_type;

"""

create_top_customers_by_order_value = BigQueryInsertJobOperator(
    task_id='create_top_customers_by_order_value',
    configuration={
        "query": {
            "query": sql_view_top_customers,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_avg_orders_per_customer = BigQueryInsertJobOperator(
    task_id='create_avg_orders_per_customer',
    configuration={
        "query": {
            "query": sql_orders_avg,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_top_selling_products_by_quantity = BigQueryInsertJobOperator(
    task_id='create_top_selling_products_by_quantity',
    configuration={
        "query": {
            "query": sql_view_top_products,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_orders_per_month = BigQueryInsertJobOperator(
    task_id='create_orders_per_month',
    configuration={
        "query": {
            "query": sql_view_total_number_of_orders,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_payment_method_distribution = BigQueryInsertJobOperator(
    task_id='create_payment_method_distribution',
    configuration={
        "query": {
            "query": sql_view_payment,
            "useLegacySql": False,
        }
    },
    dag=dag1,
)

create_top_customers_by_order_value >> create_avg_orders_per_customer >> create_top_selling_products_by_quantity >> create_orders_per_month >> create_payment_method_distribution
