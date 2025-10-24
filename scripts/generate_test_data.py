import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def generate_transactions_data(n_records: int = 1000000) -> None:
    """
    Genera un archivo CSV de transacciones de ejemplo.
    """
    logger.info(f"Generating {n_records} transaction records...")
    
    # Generar datos sintéticos
    dates = pd.date_range(
        start=datetime.now() - timedelta(days=365),
        end=datetime.now(),
        periods=n_records
    )
    
    data = {
        'transaction_id': range(1, n_records + 1),
        'user_id': np.random.randint(1, 10000, n_records),
        'amount': np.random.uniform(1, 1000, n_records).round(2),
        'currency': 'USD',
        'transaction_date': dates,
        'status': np.random.choice(
            ['completed', 'failed', 'pending'],
            n_records,
            p=[0.85, 0.1, 0.05]
        )
    }
    
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/data/sample_transactions.csv', index=False)
    logger.info("Transactions data generated successfully")

def generate_logs_data(n_records: int = 5000000) -> None:
    """
    Genera un archivo de logs en formato JSONL comprimido.
    """
    logger.info(f"Generating {n_records} log records...")
    
    endpoints = ['/api/users', '/api/transactions', '/api/products', '/api/orders', '/api/auth']
    status_codes = [200, 201, 400, 401, 403, 404, 500, 502, 503]
    
    data = []
    for i in range(n_records):
        log_entry = {
            'timestamp': (datetime.now() - timedelta(seconds=np.random.randint(0, 86400))).strftime('%Y-%m-%d %H:%M:%S'),
            'endpoint': np.random.choice(endpoints),
            'status_code': np.random.choice(status_codes, p=[0.3, 0.1, 0.1, 0.05, 0.05, 0.1, 0.1, 0.1, 0.1]),
            'response_time': np.random.uniform(0.1, 5.0),
            'user_agent': f"user_agent_{np.random.randint(1, 100)}"
        }
        data.append(log_entry)
    
    # Convertir a DataFrame y guardar como JSONL comprimido
    df = pd.DataFrame(data)
    df.to_json(
        '/opt/airflow/data/sample.log.gz',
        orient='records',
        lines=True,
        compression='gzip'
    )
    logger.info("Logs data generated successfully")

if __name__ == '__main__':
    generate_transactions_data(1000000)
    generate_logs_data(5000000)