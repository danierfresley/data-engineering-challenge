from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

class DataQualityOperator(BaseOperator):
    """
    Custom operator for data quality checks
    """
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        checks: list,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.checks = checks

    def execute(self, context):
        from utils.data_quality import DataQualityChecker
        
        checker = DataQualityChecker()
        results = checker.run_checks(self.table_name, self.checks)
        
        if not results['success']:
            error_msg = f"Data quality checks failed for {self.table_name}: {results['errors']}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
            
        logger.info(f"Data quality checks passed for {self.table_name}")

class FileProcessingOperator(BaseOperator):
    """
    Custom operator for file processing with chunking
    """
    
    @apply_defaults
    def __init__(
        self,
        input_path: str,
        output_path: str,
        processor_class: str,
        chunksize: int = 10000,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.output_path = output_path
        self.processor_class = processor_class
        self.chunksize = chunksize

    def execute(self, context):
        # Dynamic import based on processor_class
        module = __import__('processors', fromlist=[self.processor_class])
        processor_class = getattr(module, self.processor_class)
        processor = processor_class()
        
        processor.process(
            input_path=self.input_path,
            output_path=self.output_path,
            chunksize=self.chunksize
        )