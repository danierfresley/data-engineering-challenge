import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.append('/opt/airflow/etl_src')

from processors.log_processor import LogProcessor

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=1),
}

def process_logs():
    """Process large log files"""
    processor = LogProcessor()
    processor.process(
        input_path='/opt/airflow/data/sample.log.gz',
        output_path='/opt/airflow/data/processed_logs',
        chunksize=50000
    )

with DAG(
    'etl_logs',
    default_args=default_args,
    description='ETL Pipeline for Log Files',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['etl', 'logs'],
) as dag:

    process_logs_task = PythonOperator(
        task_id='process_log_files',
        python_callable=process_logs,
    )