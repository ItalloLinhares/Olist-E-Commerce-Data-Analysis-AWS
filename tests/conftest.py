"""Pytest configuration and fixtures"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("PyTestSession") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_customers_data():
    """Sample customers data for testing"""
    return [
        ("a"*32, "b"*32, "12345", "São Paulo", "SP"),
        ("c"*32, "d"*32, "67890", "Rio de Janeiro", "RJ"),
        ("e"*32, "f"*32, "11111", "Belo Horizonte", "MG")
    ]


@pytest.fixture
def sample_orders_data():
    """Sample orders data for testing"""
    return [
        ("order1"*6 + "aa", "cust1"*6 + "aa", "delivered", 
         "2023-01-15 10:30:00", "2023-01-15 11:00:00"),
        ("order2"*6 + "bb", "cust2"*6 + "bb", "shipped", 
         "2023-02-20 14:15:00", "2023-02-20 15:00:00"),
        ("order3"*6 + "cc", "cust3"*6 + "cc", "canceled", 
         "2023-03-10 09:00:00", None)
    ]