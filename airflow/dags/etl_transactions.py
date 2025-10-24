import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Add ETL source to path
sys.path.append('/opt/airflow/etl_src')

from processors.transaction_processor import TransactionProcessor
from utils.database import DatabaseManager
from utils.data_quality import DataQualityChecker

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def process_transactions():
    """Task to process transactions data"""
    processor = TransactionProcessor()
    processor.process(
        input_path=Variable.get('transactions_file_path', '/opt/airflow/data/sample_transactions.csv'),
        chunksize=10000
    )

def validate_data_quality():
    """Task to validate data quality"""
    checker = DataQualityChecker()
    results = checker.validate_transactions()
    
    if not results['is_valid']:
        raise ValueError(f"Data quality validation failed: {results['errors']}")

def load_to_data_warehouse():
    """Task to load data to data warehouse"""
    db_manager = DatabaseManager()
    db_manager.load_transactions_to_dw()

with DAG(
    'etl_transactions',
    default_args=default_args,
    description='ETL Pipeline for Transactions Data',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'transactions'],
) as dag:

    # Sensors
    wait_for_file = FileSensor(
        task_id='wait_for_transactions_file',
        filepath=Variable.get('transactions_file_path', '/opt/airflow/data/sample_transactions.csv'),
        poke_interval=30,
        timeout=3600,
        mode='poke',
    )

    # ETL Tasks
    process_transactions_task = PythonOperator(
        task_id='process_transactions',
        python_callable=process_transactions,
    )

    validate_quality_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )

    load_warehouse_task = PythonOperator(
        task_id='load_to_data_warehouse',
        python_callable=load_to_data_warehouse,
    )

    # Notifications
    success_email = EmailOperator(
        task_id='send_success_email',
        to='data-team@company.com',
        subject='ETL Transactions - Success',
        html_content='The transactions ETL pipeline completed successfully.',
        trigger_rule='all_success'
    )

    failure_email = EmailOperator(
        task_id='send_failure_email',
        to='data-team@company.com',
        subject='ETL Transactions - Failed',
        html_content='The transactions ETL pipeline failed.',
        trigger_rule='one_failed'
    )

    # Dependencies
    wait_for_file >> process_transactions_task >> validate_quality_task >> load_warehouse_task
    load_warehouse_task >> success_email
    [process_transactions_task, validate_quality_task, load_warehouse_task] >> failure_email