from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from datetime import datetime
from airflow.models import Variable

PROJECT_ID = "bamboo-autumn-484913-i0"
REGION = "us-central1"

BUILD_VERSION = Variable.get("dataflow_build_version")

TEMPLATE_GCS_PATH = f"gs://interview-dataflow-templates/bq_to_bq_build-{BUILD_VERSION}.json"

with DAG(
    dag_id="bq_to_bq_flex_template",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_flex = DataflowStartFlexTemplateOperator(
        task_id="run_bq_to_bq_flex",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": f"bq-to-bq-build-{BUILD_VERSION}",
                "containerSpecGcsPath": TEMPLATE_GCS_PATH,
                "parameters": {
                    "input_table": "bamboo-autumn-484913-i0:dataset.sales_raw",
                    "output_table": "bamboo-autumn-484913-i0:dataset.sales"
                },
                "environment": {
                    "tempLocation": "gs://interview-dataflow-templates/temp",
                    "stagingLocation": "gs://interview-dataflow-templates/staging"
                }
            }
        }
    )
