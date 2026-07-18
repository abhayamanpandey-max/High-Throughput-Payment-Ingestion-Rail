from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Default parameters applied across all operational stages
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@yourcompany.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='payment_pipeline_orchestration',
    default_args=default_args,
    description='Triggers Medallion processing routines across Databricks workspaces',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['payment', 'databricks', 'production']
) as dag:

    # Cluster parameters configuration footprint
    new_cluster_config = {
        'spark_version': '14.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2
    }

    # Task 1: Ingest streaming logs to raw database paths
    run_bronze = DatabricksSubmitRunOperator(
        task_id='ingest_bronze_layer',
        databricks_conn_id='databricks_default',
        new_cluster=new_cluster_config,
        notebook_task={'notebook_path': '/Production/PaymentPipeline/01_Bronze'}
    )

    # Task 2: Validate structure and drop malformed data payloads
    run_silver = DatabricksSubmitRunOperator(
        task_id='clean_silver_layer',
        databricks_conn_id='databricks_default',
        new_cluster=new_cluster_config,
        notebook_task={'notebook_path': '/Production/PaymentPipeline/02_Silver'}
    )

    # Task 3: Calculate security flags for Streamlit metrics
    run_gold = DatabricksSubmitRunOperator(
        task_id='aggregate_gold_layer',
        databricks_conn_id='databricks_default',
        new_cluster=new_cluster_config,
        notebook_task={'notebook_path': '/Production/PaymentPipeline/03_Gold'}
    )

    # Sequential dependencies setting directional flow paths
    run_bronze >> run_silver >> run_gold
