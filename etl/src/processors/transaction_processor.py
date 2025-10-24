import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from utils.database import DatabaseManager
from utils.file_utils import FileHandler

logger = logging.getLogger(__name__)

class TransactionProcessor:
    """Processor for transaction data"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.file_handler = FileHandler()
        
    def process(self, input_path: str, chunksize: int = 10000) -> None:
        """Process transactions data in chunks"""
        try:
            logger.info(f"Starting transaction processing from {input_path}")
            
            for chunk_num, chunk in enumerate(pd.read_csv(input_path, chunksize=chunksize)):
                logger.info(f"Processing chunk {chunk_num + 1}")
                
                # Transform data
                transformed_chunk = self._transform_chunk(chunk)
                
                # Validate data
                if self._validate_chunk(transformed_chunk):
                    # Load to database
                    self.db_manager.insert_dataframe(
                        transformed_chunk, 
                        'raw_transactions', 
                        if_exists='append'
                    )
                else:
                    logger.warning(f"Chunk {chunk_num + 1} failed validation")
                    
            logger.info("Transaction processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing transactions: {e}")
            raise

    def _transform_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to a chunk of data"""
        # Copy to avoid SettingWithCopyWarning
        transformed = chunk.copy()
        
        # Convert date columns
        date_columns = ['transaction_date', 'created_at']
        for col in date_columns:
            if col in transformed.columns:
                transformed[col] = pd.to_datetime(transformed[col], errors='coerce')
        
        # Clean numeric columns
        if 'amount' in transformed.columns:
            transformed['amount'] = pd.to_numeric(transformed['amount'], errors='coerce')
            # Remove negative amounts
            transformed = transformed[transformed['amount'] > 0]
        
        # Standardize status
        if 'status' in transformed.columns:
            transformed['status'] = transformed['status'].str.lower().str.strip()
            valid_statuses = ['completed', 'failed', 'pending', 'cancelled']
            transformed = transformed[transformed['status'].isin(valid_statuses)]
        
        # Add processing metadata
        transformed['processed_at'] = datetime.now()
        transformed['batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return transformed

    def _validate_chunk(self, chunk: pd.DataFrame) -> bool:
        """Validate chunk data quality"""
        if chunk.empty:
            logger.warning("Empty chunk received")
            return False
            
        # Check for required columns
        required_columns = ['user_id', 'amount', 'transaction_date']
        missing_columns = [col for col in required_columns if col not in chunk.columns]
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for null values in critical columns
        critical_columns = ['user_id', 'amount', 'transaction_date']
        null_counts = chunk[critical_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values found in critical columns: {null_counts.to_dict()}")
            return False
            
        return True