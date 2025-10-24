import gzip
import json
import polars as pl
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
from typing import Iterator, Dict, Any
from utils.file_utils import FileHandler

logger = logging.getLogger(__name__)

class LogProcessor:
    """Processor for large log files"""
    
    def __init__(self):
        self.file_handler = FileHandler()
        
    def process(self, input_path: str, output_path: str, chunksize: int = 50000) -> None:
        """Process log files in streaming mode"""
        try:
            logger.info(f"Starting log processing from {input_path}")
            
            # Create output directory
            Path(output_path).mkdir(parents=True, exist_ok=True)
            
            # Process in chunks using polars for better performance
            self._process_with_polars(input_path, output_path, chunksize)
            
            logger.info("Log processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing logs: {e}")
            raise

    def _process_with_polars(self, input_path: str, output_path: str, chunksize: int) -> None:
        """Process logs using Polars for better performance"""
        # Read JSONL gzip file
        df = pl.read_ndjson(input_path)
        
        # Filter status_code >= 500
        df_filtered = df.filter(pl.col('status_code') >= 500)
        
        # Clean and parse fields
        df_clean = self._clean_log_data(df_filtered)
        
        # Group by hour and endpoint
        df_grouped = df_clean.groupby([
            pl.col('timestamp').dt.truncate('1h').alias('hour'),
            'endpoint'
        ]).agg([
            pl.count().alias('error_count'),
            pl.col('response_time').mean().alias('avg_response_time'),
            pl.col('response_time').max().alias('max_response_time'),
            pl.col('response_time').min().alias('min_response_time'),
            pl.col('user_id').n_unique().alias('unique_users')
        ])
        
        # Export to Parquet with partitioning
        output_file = Path(output_path) / "error_logs_summary.parquet"
        df_grouped.write_parquet(
            str(output_file),
            compression='snappy',
            use_pyarrow=True
        )

    def _clean_log_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and parse log data"""
        # Convert timestamp
        df = df.with_columns([
            pl.col('timestamp').str.strptime(pl.Datetime, fmt='%Y-%m-%d %H:%M:%S').alias('timestamp')
        ])
        
        # Extract endpoint path
        df = df.with_columns([
            pl.col('endpoint')
            .str.extract(r'(\/[a-zA-Z0-9_\-\.\/]+)')
            .alias('endpoint_clean')
        ])
        
        # Clean user agent
        df = df.with_columns([
            pl.col('user_agent')
            .str.slice(0, 100)
            .alias('user_agent_clean')
        ])
        
        return df

    def _process_with_pandas(self, input_path: str, output_path: str, chunksize: int) -> None:
        """Alternative processing with pandas (for comparison)"""
        chunks = []
        
        with gzip.open(input_path, 'rt') as f:
            for i, line in enumerate(f):
                if i % chunksize == 0 and i > 0:
                    # Process chunk
                    chunk_df = pd.DataFrame(chunks)
                    processed_chunk = self._process_chunk(chunk_df)
                    self._save_chunk(processed_chunk, output_path, i // chunksize)
                    chunks = []
                    
                try:
                    data = json.loads(line.strip())
                    chunks.append(data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON at line {i}")
                    
        # Process remaining chunks
        if chunks:
            chunk_df = pd.DataFrame(chunks)
            processed_chunk = self._process_chunk(chunk_df)
            self._save_chunk(processed_chunk, output_path, 'final')

    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of log data"""
        # Filter errors
        error_chunk = chunk[chunk['status_code'] >= 500].copy()
        
        if error_chunk.empty:
            return error_chunk
            
        # Parse timestamp
        error_chunk['timestamp'] = pd.to_datetime(error_chunk['timestamp'])
        error_chunk['hour'] = error_chunk['timestamp'].dt.floor('h')
        
        return error_chunk

    def _save_chunk(self, chunk: pd.DataFrame, output_path: str, chunk_id: int) -> None:
        """Save processed chunk"""
        if not chunk.empty:
            chunk_file = Path(output_path) / f"error_logs_chunk_{chunk_id}.parquet"
            chunk.to_parquet(
                chunk_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )