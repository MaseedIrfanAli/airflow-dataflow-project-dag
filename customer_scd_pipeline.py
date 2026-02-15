from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

PROJECT_ID = "bamboo-autumn-484913-i0"
DATASET = "retail"

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="retail_streaming_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "scd", "streaming"]
) as dag:

    # 1️⃣ SCD Type 2 MERGE
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

    # 2️⃣ Load Fact Orders (Now using streaming table)
    load_fact_orders = BigQueryInsertJobOperator(
        task_id="load_fact_orders",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET}.fact_orders`
                (order_id, customer_sk, amount, order_time)

                SELECT
                    o.order_id,
                    d.customer_sk,
                    o.amount,
                    o.order_time
                FROM `{PROJECT_ID}.{DATASET}.orders_raw` o
                JOIN `{PROJECT_ID}.{DATASET}.dim_customer` d
                  ON o.customer_id = d.customer_id
                 AND d.is_current = TRUE
                WHERE DATE(o.order_time) = CURRENT_DATE()
                """,
                "useLegacySql": False,
            }
        },
    )

    # 3️⃣ Data Quality Check
    dq_check = BigQueryInsertJobOperator(
        task_id="dq_check",
        configuration={
            "query": {
                "query": f"""
                SELECT COUNT(*) 
                FROM `{PROJECT_ID}.{DATASET}.fact_orders`
                WHERE customer_sk IS NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    scd_merge >> load_fact_orders >> dq_check
