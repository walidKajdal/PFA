from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_sensor_data(**kwargs):
    """Traite les données des capteurs"""
    # Import ici pour éviter les problèmes de dépendances circulaires
    from src.etl.extract import extract_batch_from_kafka
    from src.etl.transform import clean_and_transform_batch
    from src.etl.load import load_to_elasticsearch
    
    # Extraction
    print("Extraction des données depuis Kafka...")
    raw_data = extract_batch_from_kafka()
    
    # Transformation
    print(f"Transformation de {len(raw_data)} enregistrements...")
    transformed_data = clean_and_transform_batch(raw_data)
    
    # Chargement
    print("Chargement des données dans Elasticsearch...")
    load_to_elasticsearch(transformed_data)
    
    return f"Processed {len(transformed_data)} records"

with DAG(
    'iot_sensor_etl',
    default_args=default_args,
    description='ETL pipeline for IoT sensor data',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    process_task = PythonOperator(
        task_id='process_sensor_data',
        python_callable=process_sensor_data,
    )