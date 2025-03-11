from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@daily',
}

BUCKET_NAME = Variable.get("gcp_bucket_name")
PROJECT_ID = Variable.get("gcp_project_id")
DATASET_NAME = Variable.get("gcp_dataset_name")
TABLE_RAW = Variable.get("gcp_table_raw")
TABLE_TRANSFORMED = Variable.get("gcp_table_transformed")
REGION = Variable.get("gcp_region")


SCHEMA_RAW = [
    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "brewery_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "address_1", "type": "STRING", "mode": "NULLABLE"},
    {"name": "address_2", "type": "STRING", "mode": "NULLABLE"},
    {"name": "address_3", "type": "STRING", "mode": "NULLABLE"},
    {"name": "city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state_province", "type": "STRING", "mode": "NULLABLE"},
    {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "website_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "street", "type": "STRING", "mode": "NULLABLE"},
]

#PEGA ARQUIVO ESPECIFICO
#DEIXAR MAIS DINAMICO PARA PEGAR O ARQUIVO MAIS NOVO E FAZER VALIDAÇÃO DOS DADOS PARA EVITAR DUPLICADAS
def transform_json_to_newline_delimited(**kwargs):
    bucket_name = "jvq-test"
    source_object = "bronze/breweries_20250306T113623.json"  
    destination_object = "bronze/breweries_transformed.json"  

    hook = GCSHook(gcp_conn_id='google_cloud_default')
    file_content = hook.download(bucket_name=bucket_name, object_name=source_object)
    
    try:
        data = json.loads(file_content) 
        ndjson = '\n'.join([json.dumps(record) for record in data]) 
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao decodificar JSON: {str(e)}")


    hook.upload(
        bucket_name=bucket_name,
        object_name=destination_object,
        data=ndjson.encode('utf-8'),  
        mime_type='application/json'
    )

    return f"gs://{bucket_name}/{destination_object}"


with DAG(
    dag_id='gcp_silver_layer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['gcp', 'full'],
) as dag:


    transform_json = PythonOperator(
        task_id='transform_json',
        python_callable=transform_json_to_newline_delimited,
    )


    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id=DATASET_NAME,
        location='US',
    )


    create_raw_table = BigQueryCreateEmptyTableOperator(
        task_id='create_raw_table',
        dataset_id=DATASET_NAME,
        table_id=TABLE_RAW,
        schema_fields=SCHEMA_RAW,
    )


    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=['bronze/breweries_transformed.json'],  
        destination_project_dataset_table=f'{DATASET_NAME}.{TABLE_RAW}',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=SCHEMA_RAW,
        ignore_unknown_values=True,  
    )

    transform_data = BigQueryInsertJobOperator(
        task_id='transform_data',
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{DATASET_NAME}.{TABLE_TRANSFORMED}` AS
                SELECT
                    DISTINCT id,
                    name,
                    brewery_type,
                    address_1 AS address,
                    city,
                    state_province AS state,
                    postal_code,
                    country,
                    IFNULL(longitude, 0.0) AS longitude,
                    IFNULL(latitude, 0.0) AS latitude,
                    phone,
                    website_url,
                    street
                FROM
                    `{DATASET_NAME}.{TABLE_RAW}`
                WHERE
                    id IS NOT NULL
                    AND name IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
    )

    export_to_gcs = BigQueryToGCSOperator(
        task_id='export_to_gcs',
        source_project_dataset_table=f'{DATASET_NAME}.{TABLE_TRANSFORMED}',
        destination_cloud_storage_uris=[f'gs://{BUCKET_NAME}/silver/breweries_transformed.parquet'],
        export_format='PARQUET',
    )

    transform_json >> create_dataset >> create_raw_table >> load_gcs_to_bigquery >> transform_data >> export_to_gcs