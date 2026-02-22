"""Tests for customer validation with PySpark"""

import pytest
from pyspark.sql import functions as F
from src.validations.validate_customers import validate_customers
from chispa.dataframe_comparer import assert_df_equality


@pytest.mark.spark
class TestValidateCustomers:
    """Test suite for customer validation"""
    
    def test_validate_all_valid_customers(self, spark):
        """Test with all valid customer records"""
        data = [
            ("a"*32, "b"*32, "12345", "São Paulo", "SP"),
            ("c"*32, "d"*32, "67890", "Rio de Janeiro", "RJ")
        ]
        
        df = spark.createDataFrame(
            data,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )
        
        df_valid, df_invalid, stats = validate_customers(df)
        
        assert stats['valid_rows'] == 2
        assert stats['invalid_rows'] == 0
        assert stats['valid_percentage'] == 100.0
    
    def test_validate_invalid_customer_id(self, spark):
        """Test with invalid customer_id (wrong length)"""
        data = [
            ("invalid", "b"*32, "12345", "São Paulo", "SP")
        ]
        
        df = spark.createDataFrame(
            data,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )
        
        df_valid, df_invalid, stats = validate_customers(df)
        
        assert stats['valid_rows'] == 0
        assert stats['invalid_rows'] == 1
        assert stats['validation_details']['customer_id_invalid'] == 1
    
    def test_validate_invalid_state(self, spark):
        """Test with invalid state code"""
        data = [
            ("a"*32, "b"*32, "12345", "Invalid City", "XX")
        ]
        
        df = spark.createDataFrame(
            data,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )
        
        df_valid, df_invalid, stats = validate_customers(df)
        
        assert stats['invalid_rows'] == 1
        assert stats['validation_details']['customer_state_invalid'] == 1
    
    def test_validate_invalid_zip_code(self, spark):
        """Test with invalid zip code (wrong length)"""
        data = [
            ("a"*32, "b"*32, "123", "São Paulo", "SP")  # Only 3 digits
        ]
        
        df = spark.createDataFrame(
            data,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )
        
        df_valid, df_invalid, stats = validate_customers(df)
        
        assert stats['invalid_rows'] == 1
        assert stats['validation_details']['customer_zip_code_invalid'] == 1
    
    def test_validate_null_values(self, spark):
        """Test with null values"""
        data = [
            (None, "b"*32, "12345", "São Paulo", "SP")
        ]
        
        df = spark.createDataFrame(
            data,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )
        
        df_valid, df_invalid, stats = validate_customers(df)
        
        assert stats['invalid_rows'] == 1
        assert stats['validation_details']['customer_id_invalid'] == 1
    
    def test_validate_mixed_valid_invalid(self, spark):
        """Test with mix of valid and invalid records"""
        data = [
            ("a"*32, "b"*32, "12345", "São Paulo", "SP"),     # valid
            ("invalid", "c"*32, "67890", "Rio", "RJ"),        # invalid id
            ("d"*32, "e"*32, "123", "BH", "MG"),             # invalid zip
            ("f"*32, "g"*32, "99999", "Porto Alegre", "RS")  # valid
        ]
        
        df = spark.createDataFrame(
            data,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )
        
        df_valid, df_invalid, stats = validate_customers(df)
        
        assert stats['valid_rows'] == 2
        assert stats['invalid_rows'] == 2
        assert stats['valid_percentage'] == 50.0