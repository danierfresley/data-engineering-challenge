import pandas as pd
from sqlalchemy import text
import logging
from typing import Dict, List, Any, Optional
from utils.database import DatabaseManager

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Data quality validation class"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def validate_transactions(self) -> Dict[str, Any]:
        """Validate transactions data quality"""
        checks = [
            self._check_table_not_empty('raw_transactions'),
            self._check_no_negative_amounts(),
            self._check_valid_statuses(),
            self._check_no_duplicate_transactions(),
        ]
        
        results = {
            'is_valid': all(check['passed'] for check in checks),
            'checks': checks,
            'errors': [check['error'] for check in checks if not check['passed']]
        }
        
        return results

    def _check_table_not_empty(self, table_name: str) -> Dict[str, Any]:
        """Check if table is not empty"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.db_manager.execute_query(query)
            count = result.iloc[0]['count']
            
            return {
                'check': f'{table_name} not empty',
                'passed': count > 0,
                'error': f'Table {table_name} is empty' if count == 0 else None,
                'details': f'Row count: {count}'
            }
            
        except Exception as e:
            return {
                'check': f'{table_name} not empty',
                'passed': False,
                'error': str(e),
                'details': None
            }

    def _check_no_negative_amounts(self) -> Dict[str, Any]:
        """Check for negative amounts"""
        try:
            query = "SELECT COUNT(*) as count FROM raw_transactions WHERE amount < 0"
            result = self.db_manager.execute_query(query)
            count = result.iloc[0]['count']
            
            return {
                'check': 'No negative amounts',
                'passed': count == 0,
                'error': f'Found {count} transactions with negative amounts' if count > 0 else None,
                'details': f'Negative amount count: {count}'
            }
            
        except Exception as e:
            return {
                'check': 'No negative amounts',
                'passed': False,
                'error': str(e),
                'details': None
            }

    def _check_valid_statuses(self) -> Dict[str, Any]:
        """Check for valid status values"""
        try:
            query = """
            SELECT status, COUNT(*) as count 
            FROM raw_transactions 
            WHERE status NOT IN ('completed', 'failed', 'pending', 'cancelled')
            GROUP BY status
            """
            result = self.db_manager.execute_query(query)
            
            invalid_statuses = result.to_dict('records')
            
            return {
                'check': 'Valid statuses',
                'passed': len(invalid_statuses) == 0,
                'error': f'Found invalid statuses: {invalid_statuses}' if invalid_statuses else None,
                'details': f'Invalid status count: {len(invalid_statuses)}'
            }
            
        except Exception as e:
            return {
                'check': 'Valid statuses',
                'passed': False,
                'error': str(e),
                'details': None
            }

    def _check_no_duplicate_transactions(self) -> Dict[str, Any]:
        """Check for duplicate transactions"""
        try:
            query = """
            SELECT transaction_id, COUNT(*) as count
            FROM raw_transactions 
            GROUP BY transaction_id 
            HAVING COUNT(*) > 1
            """
            result = self.db_manager.execute_query(query)
            duplicates = result.to_dict('records')
            
            return {
                'check': 'No duplicate transactions',
                'passed': len(duplicates) == 0,
                'error': f'Found {len(duplicates)} duplicate transactions' if duplicates else None,
                'details': f'Duplicate count: {len(duplicates)}'
            }
            
        except Exception as e:
            return {
                'check': 'No duplicate transactions',
                'passed': False,
                'error': str(e),
                'details': None
            }