import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Optional, List, Dict, Any
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manager for database operations"""
    
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.getenv(
            'DATABASE_URL', 
            'postgresql://airflow:airflow@postgres:5432/data_warehouse'
        )
        self.engine = create_engine(self.connection_string)
        
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> None:
        """Insert DataFrame into database"""
        try:
            with self.engine.begin() as connection:
                df.to_sql(
                    table_name,
                    connection,
                    if_exists=if_exists,
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
            
        except SQLAlchemyError as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            raise

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            with self.engine.connect() as connection:
                result = pd.read_sql(text(query), connection, params=params)
            return result
            
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table_from_df(self, df: pd.DataFrame, table_name: str, primary_key: Optional[str] = None) -> None:
        """Create table from DataFrame structure"""
        try:
            with self.engine.begin() as connection:
                # Create table with inferred types
                df.head(0).to_sql(table_name, connection, if_exists='fail', index=False)
                
                # Add primary key if specified
                if primary_key and primary_key in df.columns:
                    connection.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})"))
                    
            logger.info(f"Successfully created table {table_name}")
            
        except SQLAlchemyError as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def load_transactions_to_dw(self) -> None:
        """Load transactions to data warehouse following star schema"""
        try:
            # Create fact table
            fact_query = """
            INSERT INTO fact_transactions (transaction_id, user_id, date_id, amount, status)
            SELECT 
                transaction_id,
                user_id,
                TO_CHAR(transaction_date, 'YYYYMMDD')::INT as date_id,
                amount,
                status
            FROM raw_transactions
            WHERE processed_at >= (SELECT COALESCE(MAX(processed_at), '1900-01-01') FROM fact_transactions)
            """
            
            with self.engine.begin() as connection:
                result = connection.execute(text(fact_query))
                logger.info(f"Loaded {result.rowcount} rows into fact_transactions")
                
        except SQLAlchemyError as e:
            logger.error(f"Error loading data to warehouse: {e}")
            raise