from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'abhay',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'payment_ingestion_pipeline',
    default_args=default_args,
    description='High-throughput payment ingestion pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 15),
    catchup=False,
)

def start_producer():
    print("Starting payment producer...")
    # Start producer in background

def run_flink_job():
    print("Running Flink streaming job...")
    # Execute Flink job

def check_health():
    print("Checking pipeline health...")
    # Check metrics

# Define tasks
task_producer = PythonOperator(
    task_id='start_producer',
    python_callable=start_producer,
    dag=dag,
)

task_flink = PythonOperator(
    task_id='run_flink_job',
    python_callable=run_flink_job,
    dag=dag,
)

task_health = PythonOperator(
    task_id='health_check',
    python_callable=check_health,
    dag=dag,
)

# Define dependencies
task_producer >> task_flink >> task_health
