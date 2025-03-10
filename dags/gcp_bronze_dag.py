from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from google.cloud import storage
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@daily',
}

def test_gcp_connection():
    try:
        hook = GCSHook(gcp_conn_id='google_cloud_default')
        client = hook.get_conn()
        buckets = list(client.list_buckets())
        print(f"Conexão GCP OK. Buckets disponíveis: {len(buckets)}")
        return True
    except Exception as e:
        raise Exception(f"Falha na conexão GCP: {str(e)}")

def test_api_connection():
    try:
        hook = HttpHook(method='GET', http_conn_id='brewery_api')
        response = hook.run('/breweries')
        if response.status_code == 200:
            print("Conexão API OK")
            return True
        raise Exception(f"Status code inválido: {response.status_code}")
    except Exception as e:
        raise Exception(f"Falha na API: {str(e)}")

@task
def extract_load_bronze(**context):
   
    bucket_name = "jvq-test"
    endpoint = "/breweries"
    execution_date = context['logical_date'].strftime("%Y%m%dT%H%M%S")
    blob_path = f"bronze/breweries_{execution_date}.json"


    http_hook = HttpHook(method='GET', http_conn_id='brewery_api')
    response = http_hook.run(endpoint)
    
    if response.status_code != 200:
        raise ValueError(f"Falha na API: Status {response.status_code}")
    
    data = response.json()
    print(f"Dados extraídos: {len(data)} registros")


    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=blob_path,
        data=json.dumps(data, indent=2),
        mime_type='application/json'
    )
    
    print(f"Dados carregados em gs://{bucket_name}/{blob_path}")
    return blob_path

with DAG(
    dag_id='gcp_bronze_layer',
    default_args=default_args,
    catchup=False,
    tags=['gcp', 'bronze'],
) as dag:

    test_gcp = PythonOperator(
        task_id='testar_conexao_gcp',
        python_callable=test_gcp_connection
    )

    test_api = PythonOperator(
        task_id='testar_conexao_api',
        python_callable=test_api_connection
    )
    
    bronze_task = extract_load_bronze()

    [test_gcp, test_api] >> bronze_task