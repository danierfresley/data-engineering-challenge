import pytest
import pandas as pd
from datetime import datetime
from etl.src.processors.transaction_processor import TransactionProcessor
from etl.src.utils.data_quality import DataQualityChecker

class TestTransactionProcessor:
    def test_transform_chunk(self):
        """Test chunk transformation"""
        processor = TransactionProcessor()
        
        # Create test data
        test_data = pd.DataFrame({
            'user_id': [1, 2, 3],
            'amount': [100.50, 200.75, -50.00],
            'transaction_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'status': ['COMPLETED', 'failed', 'invalid_status']
        })
        
        transformed = processor._transform_chunk(test_data)
        
        # Assertions
        assert len(transformed) == 2  # Negative amount and invalid status filtered
        assert transformed['status'].str.islower().all()
        assert 'processed_at' in transformed.columns

    def test_validate_chunk(self):
        """Test chunk validation"""
        processor = TransactionProcessor()
        
        # Valid data
        valid_data = pd.DataFrame({
            'user_id': [1, 2],
            'amount': [100.50, 200.75],
            'transaction_date': [datetime.now(), datetime.now()]
        })
        
        assert processor._validate_chunk(valid_data) == True
        
        # Invalid data (missing required column)
        invalid_data = pd.DataFrame({
            'user_id': [1, 2],
            'amount': [100.50, 200.75]
            # Missing transaction_date
        })
        
        assert processor._validate_chunk(invalid_data) == False

class TestDataQualityChecker:
    def test_validation_structure(self):
        """Test data quality validation structure"""
        checker = DataQualityChecker()
        
        # Mock database response
        # In a real test, you'd use a test database or mocking
        result = checker.validate_transactions()
        
        assert 'is_valid' in result
        assert 'checks' in result
        assert 'errors' in result
        assert isinstance(result['checks'], list)