from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.models import Variable


BUCKET_NAME = Variable.get("gcp_bucket_name")
PROJECT_ID = Variable.get("gcp_project_id")
DATASET_NAME = Variable.get("gcp_dataset_name")
TABLE_TRANSFORMED = Variable.get("gcp_table_transformed")
TABLE_AGGREGATED = "breweries_aggregated"  

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@daily',
}

with DAG(
    dag_id='gcp_gold_layer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['gcp', 'gold'],
) as dag:

    create_aggregated_table = BigQueryInsertJobOperator(
        task_id='create_aggregated_table',
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{TABLE_AGGREGATED}` AS
                SELECT
                    brewery_type,
                    state,
                    COUNT(*) AS total_breweries
                FROM
                    `{PROJECT_ID}.{DATASET_NAME}.{TABLE_TRANSFORMED}`
                GROUP BY
                    brewery_type, state
                ORDER BY
                    total_breweries DESC;
                """,
                "useLegacySql": False,
            }
        },
    )

    export_aggregated_data = BigQueryToGCSOperator(
        task_id='export_aggregated_data',
        source_project_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_AGGREGATED}',
        destination_cloud_storage_uris=[f'gs://{BUCKET_NAME}/gold/breweries_aggregated.parquet'],
        export_format='PARQUET',
    )

    create_aggregated_table >> export_aggregated_data