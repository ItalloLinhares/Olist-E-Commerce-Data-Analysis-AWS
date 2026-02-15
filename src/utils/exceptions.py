"""
Custom exceptions for ETL pipeline

This module defines custom exception classes for better error handling
and debugging in the ETL process.
"""


class ETLException(Exception):
    """
    Base exception for all ETL-related errors
    
    All custom exceptions should inherit from this class to allow
    catching all ETL errors with a single except clause.
    
    Example:
        >>> raise ETLException("Generic ETL error occurred")
    """
    pass


class DataValidationError(ETLException):
    """
    Raised when data validation fails
    
    Use this when data doesn't meet expected quality standards,
    format requirements, or business rules.
    
    Args:
        message (str): Description of the validation failure
        invalid_count (int, optional): Number of invalid records
        field_name (str, optional): Name of the field that failed validation
    
    Example:
        >>> if invalid_records > threshold:
        ...     raise DataValidationError(
        ...         "Too many invalid customer IDs",
        ...         invalid_count=150,
        ...         field_name="customer_id"
        ...     )
    """
    
    def __init__(self, message: str, invalid_count: int = None, field_name: str = None):
        self.invalid_count = invalid_count
        self.field_name = field_name
        
        # Enrich message with details
        if invalid_count is not None:
            message = f"{message} (Invalid records: {invalid_count})"
        if field_name is not None:
            message = f"{message} (Field: {field_name})"
        
        super().__init__(message)


class S3AccessError(ETLException):
    """
    Raised when there's an error accessing S3
    
    Use this for S3-specific errors like permission issues,
    bucket not found, or network problems.
    
    Args:
        message (str): Description of the S3 error
        bucket (str, optional): Name of the bucket
        key (str, optional): S3 object key
        operation (str, optional): Operation that failed (read/write/list)
    
    Example:
        >>> raise S3AccessError(
        ...     "Failed to read from S3",
        ...     bucket="my-bucket",
        ...     key="data/file.csv",
        ...     operation="read"
        ... )
    """
    
    def __init__(self, message: str, bucket: str = None, key: str = None, operation: str = None):
        self.bucket = bucket
        self.key = key
        self.operation = operation
        
        # Build detailed message
        details = []
        if bucket:
            details.append(f"Bucket: {bucket}")
        if key:
            details.append(f"Key: {key}")
        if operation:
            details.append(f"Operation: {operation}")
        
        if details:
            message = f"{message} ({', '.join(details)})"
        
        super().__init__(message)


class TransformationError(ETLException):
    """
    Raised when data transformation fails
    
    Use this when operations like filtering, aggregation, or
    column transformations encounter errors.
    
    Args:
        message (str): Description of the transformation error
        transformation_type (str, optional): Type of transformation (filter/aggregate/join)
        dataframe_name (str, optional): Name/identifier of the DataFrame
    
    Example:
        >>> raise TransformationError(
        ...     "Failed to join customers with orders",
        ...     transformation_type="join",
        ...     dataframe_name="customers_orders"
        ... )
    """
    
    def __init__(self, message: str, transformation_type: str = None,dataframe_name: str = None):
        self.transformation_type = transformation_type
        self.dataframe_name = dataframe_name
        
        if transformation_type:
            message = f"{message} (Type: {transformation_type})"
        if dataframe_name:
            message = f"{message} (DataFrame: {dataframe_name})"
        
        super().__init__(message)


class DataQualityError(ETLException):
    """
    Raised when data quality checks fail
    
    Use this when data quality thresholds are not met
    (e.g., too many nulls, duplicate keys, referential integrity).
    
    Args:
        message (str): Description of the quality issue
        metric_name (str, optional): Name of the quality metric
        expected_value (float, optional): Expected threshold
        actual_value (float, optional): Actual value measured
    
    Example:
        >>> raise DataQualityError(
        ...     "Null percentage exceeds threshold",
        ...     metric_name="null_percentage",
        ...     expected_value=5.0,
        ...     actual_value=12.5
        ... )
    """
    
    def __init__(self, message: str, metric_name: str = None, expected_value: float = None, actual_value: float = None):
        self.metric_name = metric_name
        self.expected_value = expected_value
        self.actual_value = actual_value
        
        if metric_name:
            message = f"{message} (Metric: {metric_name})"
        if expected_value is not None and actual_value is not None:
            message = f"{message} (Expected: {expected_value}, Actual: {actual_value})"
        
        super().__init__(message)


class ConfigurationError(ETLException):
    """
    Raised when there's a configuration error
    
    Use this for missing environment variables, invalid config values,
    or configuration validation failures.
    
    Args:
        message (str): Description of the configuration error
        config_key (str, optional): Configuration key that's problematic
    
    Example:
        >>> raise ConfigurationError(
        ...     "Missing required environment variable",
        ...     config_key="S3_INPUT_BUCKET"
        ... )
    """
    
    def __init__(self, message: str, config_key: str = None):
        self.config_key = config_key
        
        if config_key:
            message = f"{message} (Config key: {config_key})"
        
        super().__init__(message)


class SparkSessionError(ETLException):
    """
    Raised when there's an error with Spark session
    
    Use this for Spark initialization, configuration, or execution errors.
    
    Example:
        >>> raise SparkSessionError("Failed to create Spark session")
    """
    pass


# Convenience function for validation threshold checks
def validate_threshold(
    actual_value: float,
    threshold: float,
    metric_name: str,
    should_be_above: bool = True
) -> None:
    """
    Validate that a metric meets a threshold
    
    Args:
        actual_value: The actual measured value
        threshold: The threshold value
        metric_name: Name of the metric being checked
        should_be_above: If True, actual should be >= threshold.
                        If False, actual should be <= threshold.
    
    Raises:
        DataQualityError: If threshold is not met
    
    Example:
        >>> # Check that valid percentage is at least 95%
        >>> validate_threshold(
        ...     actual_value=92.5,
        ...     threshold=95.0,
        ...     metric_name="valid_percentage",
        ...     should_be_above=True
        ... )
        DataQualityError: Quality threshold not met (Metric: valid_percentage) 
                         (Expected: >= 95.0, Actual: 92.5)
    """
    if should_be_above:
        if actual_value < threshold:
            raise DataQualityError(
                f"Quality threshold not met (Expected: >= {threshold})",
                metric_name=metric_name,
                expected_value=threshold,
                actual_value=actual_value
            )
    else:
        if actual_value > threshold:
            raise DataQualityError(
                f"Quality threshold exceeded (Expected: <= {threshold})",
                metric_name=metric_name,
                expected_value=threshold,
                actual_value=actual_value
            )