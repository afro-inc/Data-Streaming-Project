from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from include.init_streaming import KafkaProducer


api_endpoint = 'https://random-data-api.com/api/users/random_user'
bootstrap_servers = 'kafka:9093'
client_id = 'producer_instance'
PAUSE_INTERVAL = 10  
STREAMING_DURATION = 120
topic = 'new_user'
producer = KafkaProducer(api_endpoint,bootstrap_servers, client_id, PAUSE_INTERVAL, STREAMING_DURATION, topic)

start_date = datetime(2024, 12, 3)

default_args = {
    'owner': 'neiko',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id = 'streaming_dag_v9',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup=False,
    description='stream random users to kafka topic',
    max_active_runs=1

) as dag:
    stream_task = PythonOperator(
        task_id = 'stream_to_kafka_topic',
        python_callable=producer.init_stream
    )
    stream_task