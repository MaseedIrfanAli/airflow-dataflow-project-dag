from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import uuid

PROJECT_ID = "bamboo-autumn-484913-i0"
DATASET = "retail"

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

def load_dummy_customers():
    from google.cloud import bigquery
    client = bigquery.Client()

    rows = [
        {
            "customer_id": "C101",
            "name": "John",
            "email": "john@email.com",
            "address": "New York",
            "updated_at": datetime.utcnow().isoformat()
        },
        {
            "customer_id": "C102",
            "name": "Ali",
            "email": "ali@email.com",
            "address": "Texas",
            "updated_at": datetime.utcnow().isoformat()
        }
    ]

    table_id = f"{PROJECT_ID}.{DATASET}.customer_staging"
    client.insert_rows_json(table_id, rows)

with DAG(
    dag_id="customer_scd_type2_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "scd", "bigquery"]
) as dag:

    load_staging = PythonOperator(
        task_id="load_dummy_customers",
        python_callable=load_dummy_customers
    )

    scd_merge = BigQueryInsertJobOperator(
        task_id="run_scd_merge",
        configuration={
            "query": {
                "query": f"""
                MERGE `{PROJECT_ID}.{DATASET}.dim_customer` T
                USING `{PROJECT_ID}.{DATASET}.customer_staging` S
                ON T.customer_id = S.customer_id
                AND T.is_current = TRUE

                WHEN MATCHED AND (
                    T.name != S.name OR
                    T.address != S.address OR
                    T.email != S.email
                )
                THEN UPDATE SET
                    end_date = CURRENT_TIMESTAMP(),
                    is_current = FALSE

                WHEN NOT MATCHED THEN
                INSERT (
                    customer_sk,
                    customer_id,
                    name,
                    email,
                    address,
                    start_date,
                    end_date,
                    is_current
                )
                VALUES (
                    GENERATE_UUID(),
                    S.customer_id,
                    S.name,
                    S.email,
                    S.address,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    TRUE
                )
                """,
                "useLegacySql": False,
            }
        },
    )

    data_quality_check = BigQueryInsertJobOperator(
        task_id="data_quality_check",
        configuration={
            "query": {
                "query": f"""
                SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.dim_customer`
                WHERE is_current = TRUE
                """,
                "useLegacySql": False,
            }
        },
    )

    load_staging >> scd_merge >> data_quality_check
