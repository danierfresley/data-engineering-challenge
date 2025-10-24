from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import os
import logging

logger = logging.getLogger(__name__)

class FileSizeSensor(BaseSensorOperator):
    """
    Sensor that checks if file exists and meets minimum size requirement
    """
    
    @apply_defaults
    def __init__(
        self,
        filepath: str,
        min_size_mb: float = 1.0,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.min_size_bytes = min_size_mb * 1024 * 1024

    def poke(self, context):
        if not os.path.exists(self.filepath):
            logger.info(f"File {self.filepath} does not exist yet")
            return False
            
        file_size = os.path.getsize(self.filepath)
        
        if file_size < self.min_size_bytes:
            logger.info(f"File {self.filepath} is too small: {file_size} bytes")
            return False
            
        logger.info(f"File {self.filepath} is ready: {file_size} bytes")
        return True