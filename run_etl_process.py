import pandas as pd
import logging
import boto3
from urllib.parse import urlparse
import sys
import validations
import validations.validate_customers as validate_customers
import validations.validate_orders as validate_orders
import validate_sellers
import validate_products
import validate_order_items
import validate_orders_reviews
import validate_orders_payments
import validate_geolocation
import time
from utils.logger import setup_logging
from utils.s3_helpers import get_file_size, list_s3_files, list_files_with_metadata
from src.utils.exceptions import (
    ETLException,
    DataValidationError,
    S3AccessError,
    TransformationError,
    DataQualityError,
    ConfigurationError
)

logger = setup_logging()

FILE_PROCESSOR_MAP = {
"olist_customers_dataset.csv": {
    "function": validate_customers.validate_customers_and_get_clean_data,
    "output_name": "olist_customers_dataset.parquet",
    "output_folder": "customers_dataset/",
    "quality_threshold": 95.0 
},
"olist_orders_dataset.csv": {
    "function": validate_orders.validate_orders,
    "output_name": "olist_orders_dataset.parquet",
    "output_folder": "orders_dataset/",
    "quality_threshold": 95.0 
},
#"olist_sellers_dataset.csv": {
#    "function": validate_sellers.validate_sellers,
#    "output_name": "olist_sellers_dataset.parquet",
#    "output_folder": "sellers_dataset/"
#    "quality_threshold": 95.0 
#},
"olist_products_dataset.csv": {
    "function": validate_products.validate_products,
    "output_name": "olist_products_dataset.parquet",
    "output_folder": "products_dataset/",
    "quality_threshold": 95.0 
},
"olist_order_items_dataset.csv": {
    "function": validate_order_items.validate_order_items,
    "output_name": "olist_order_items_dataset.parquet",
    "output_folder": "order_items_dataset/",
    "quality_threshold": 95.0 
},
"olist_order_reviews_dataset.csv": {
    "function": validate_orders_reviews.validate_orders_reviews,
    "output_name": "olist_orders_reviews_dataset.parquet",
    "output_folder": "orders_reviews_dataset/",
    "quality_threshold": 95.0 
},
"olist_order_payments_dataset.csv": {
    "function": validate_orders_payments.validate_orders_payments,
    "output_name": "olist_order_payments_dataset.parquet",
    "output_folder": "order_payments_dataset/",
    "quality_threshold": 95.0 
},
"olist_geolocation_dataset.csv": {
    "function": validate_geolocation.validate_geolocation,
    "output_name": "olist_geolocation_dataset.parquet",
    "output_folder": "order_geolocation_dataset/",
    "quality_threshold": 95.0 
}
}

def validate_configuration(input_path: str, output_path: str) -> None:
    """
    Validate ETL configuration before starting
    
    Args:
        input_path: S3 input path
        output_path: S3 output path
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    if not input_path:
        raise ConfigurationError(
            "Input path cannot be empty",
            config_key="input_path"
        )
    
    if not output_path:
        raise ConfigurationError(
            "Output path cannot be empty",
            config_key="output_path"
        )
    
    if not input_path.startswith('s3://'):
        raise ConfigurationError(
            f"Input path must start with 's3://': {input_path}",
            config_key="input_path"
        )
    
    if not output_path.startswith('s3://'):
        raise ConfigurationError(
            f"Output path must start with 's3://': {output_path}",
            config_key="output_path"
        )
    
    logger.info("configuration_validated", input_path=input_path, output_path=output_path)

def check_data_quality(
    rows_input: int,
    rows_output: int,
    file_name: str,
    threshold: float = 95.0
) -> None:
    """
    Check if data quality meets minimum threshold
    
    Args:
        rows_input: Number of input rows
        rows_output: Number of output rows (after validation)
        file_name: Name of the file being processed
        threshold: Minimum percentage of valid records (default: 95%)
        
    Raises:
        DataQualityError: If valid percentage is below threshold
    """
    if rows_input == 0:
        raise DataQualityError(
            f"Input file is empty: {file_name}",
            metric_name="input_rows",
            expected_value=1,
            actual_value=0
        )
    
    valid_percentage = (rows_output / rows_input) * 100
    
    if valid_percentage < threshold:
        raise DataQualityError(
            f"Data quality below threshold for {file_name}",
            metric_name="valid_percentage",
            expected_value=threshold,
            actual_value=valid_percentage
        )
    
    logger.info(
        "data_quality_check_passed",
        file_name=file_name,
        valid_percentage=round(valid_percentage, 2),
        threshold=threshold
    )

def run_etl_process(
    spark_session: SparkSession,
    input_path: str,
    output_path: str
) -> Dict:
    """
    Execute complete ETL process using PySpark
    
    Args:
        spark_session: Active Spark session
        input_path: S3 path for raw data
        output_path: S3 path for processed data
        
    Returns:
        Dictionary with processing statistics
        
    Raises:
        ConfigurationError: If configuration is invalid
        S3AccessError: If unable to access S3
        ETLException: For general ETL errors
    """
    
    # Validate configuration first
    try:
        validate_configuration(input_path, output_path)
    except ConfigurationError as e:
        logger.critical("configuration_error", error=str(e))
        raise
    
    # Ensure paths end with /
    if not input_path.endswith('/'):
        input_path += '/'
    if not output_path.endswith('/'):
        output_path += '/'
    
    stats = {
        'total_files_found': 0,
        'total_files_processed': 0,
        'total_files_failed': 0,
        'total_rows_input': 0,
        'total_rows_output': 0,
        'files_processed': []
    }
    
    etl_start_time = time.time()
    
    logger.info("etl_process_started", input_path=input_path, output_path=output_path)
    
    try:
        # List files from S3
        logger.info("listing_s3_files", path=input_path)
        
        try:
            all_files = list_files_with_metadata(input_path, extension='.csv')
        except Exception as e:
            raise S3AccessError(
                "Failed to list files from S3",
                bucket=input_path.split('/')[2],
                operation="list"
            ) from e
        
        stats['total_files_found'] = len(all_files)
        
        if not all_files:
            logger.warning("no_files_found", path=input_path)
            return stats
        
        logger.info(
            "files_found",
            total_files=len(all_files),
            file_names=[f['name'] for f in all_files]
        )
        
        # Process each file
        for file_metadata in all_files:
            file_uri = file_metadata['path']
            file_name = file_metadata['name']
            file_size_mb = file_metadata['size_mb']
            
            file_start_time = time.time()
            
            logger.info(
                "processing_file_started",
                file_name=file_name,
                file_size_mb=file_size_mb
            )
            
            try:
                # Check if file is in processor map
                if file_name not in FILE_PROCESSOR_MAP:
                    logger.warning("file_not_in_processor_map", file_name=file_name)
                    continue
                
                config = FILE_PROCESSOR_MAP[file_name]
                processor_function = config["function"]
                final_output_path = f'{output_path}{config["output_folder"]}{config["output_name"]}'
                quality_threshold = config.get("quality_threshold", 95.0)
                
                # Read CSV with Spark
                logger.info("reading_csv", file_name=file_name)
                
                try:
                    df_spark = spark_session.read \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv(file_uri)
                except Exception as e:
                    raise S3AccessError(
                        f"Failed to read CSV from S3: {file_name}",
                        key=file_uri,
                        operation="read"
                    ) from e
                
                # Count rows BEFORE validation (using Spark action)
                try:
                    rows_input = df_spark.count()
                except Exception as e:
                    raise TransformationError(
                        f"Failed to count rows in Spark DataFrame: {file_name}",
                        transformation_type="count",
                        dataframe_name=file_name
                    ) from e
                
                logger.info("csv_read_complete", file_name=file_name, rows_read=rows_input)
                
                # Check if input is empty
                if rows_input == 0:
                    raise DataValidationError(
                        f"Input file is empty: {file_name}",
                        invalid_count=0,
                        field_name="all"
                    )
                
                logger.info("applying_validations", file_name=file_name)
                
                try:
                    df_clean = processor_function(df_spark)
                except Exception as e:
                    raise DataValidationError(
                        f"Validation function failed for {file_name}",
                        field_name="multiple"
                    ) from e
                
                # Count rows AFTER validation (using Spark action)
                try:
                    rows_output = df_clean.count()
                except Exception as e:
                    raise TransformationError(
                        f"Failed to count rows in cleaned DataFrame: {file_name}",
                        transformation_type="count",
                        dataframe_name=file_name
                    ) from e
                
                rows_dropped = rows_input - rows_output
                
                # Check if result is empty
                if rows_output == 0:
                    raise DataValidationError(
                        f"All records were invalid for {file_name}",
                        invalid_count=rows_input,
                        field_name="all"
                    )
                
                # Check data quality threshold
                try:
                    check_data_quality(
                        rows_input=rows_input,
                        rows_output=rows_output,
                        file_name=file_name,
                        threshold=quality_threshold
                    )
                except DataQualityError as e:
                    logger.error(
                        "data_quality_threshold_not_met",
                        file_name=file_name,
                        error=str(e)
                    )

                logger.info("saving_parquet", file_name=file_name, output_path=final_output_path)
                
                try:
                    df_clean.write \
                        .mode("overwrite") \
                        .parquet(final_output_path, compression="snappy")
                except Exception as e:
                    raise S3AccessError(
                        f"Failed to write Parquet to S3: {file_name}",
                        key=final_output_path,
                        operation="write"
                    ) from e
                
                processing_time = time.time() - file_start_time
                
                file_stats = {
                    'file_name': file_name,
                    'file_size_mb': file_size_mb,
                    'rows_input': rows_input,
                    'rows_output': rows_output,
                    'rows_dropped': rows_dropped,
                    'processing_time_sec': round(processing_time, 2),
                    'output_path': final_output_path,
                    'status': 'success'
                }
                
                stats['files_processed'].append(file_stats)
                stats['total_files_processed'] += 1
                stats['total_rows_input'] += rows_input
                stats['total_rows_output'] += rows_output
                
                logger.info("file_processed_successfully", **file_stats)
                
            except S3AccessError as e:
                logger.error(
                    "s3_access_error",
                    file_name=file_name,
                    bucket=e.bucket,
                    key=e.key,
                    operation=e.operation,
                    error=str(e)
                )
                stats['total_files_failed'] += 1
                stats['files_processed'].append({
                    'file_name': file_name,
                    'status': 'failed',
                    'error': f'S3 access error: {str(e)}'
                })
                # Re-raise S3 errors as they're likely systemic
                raise
                
            except DataValidationError as e:
                logger.error(
                    "data_validation_error",
                    file_name=file_name,
                    invalid_count=e.invalid_count,
                    field_name=e.field_name,
                    error=str(e)
                )
                stats['total_files_failed'] += 1
                stats['files_processed'].append({
                    'file_name': file_name,
                    'status': 'failed',
                    'error': f'Validation error: {str(e)}'
                })
                # Continue processing other files
                continue
                
            except DataQualityError as e:
                logger.error(
                    "data_quality_error",
                    file_name=file_name,
                    metric_name=e.metric_name,
                    expected_value=e.expected_value,
                    actual_value=e.actual_value,
                    error=str(e)
                )
                stats['total_files_failed'] += 1
                stats['files_processed'].append({
                    'file_name': file_name,
                    'status': 'failed',
                    'error': f'Quality error: {str(e)}'
                })
                # Continue (quality issues logged but not fatal)
                continue
                
            except TransformationError as e:
                logger.error(
                    "transformation_error",
                    file_name=file_name,
                    transformation_type=e.transformation_type,
                    dataframe_name=e.dataframe_name,
                    error=str(e)
                )
                stats['total_files_failed'] += 1
                stats['files_processed'].append({
                    'file_name': file_name,
                    'status': 'failed',
                    'error': f'Transformation error: {str(e)}'
                })
                continue
                
            except Exception as e:
                logger.critical(
                    "unexpected_error_processing_file",
                    file_name=file_name,
                    error=str(e),
                    error_type=type(e).__name__,
                    exc_info=True
                )
                stats['total_files_failed'] += 1
                stats['files_processed'].append({
                    'file_name': file_name,
                    'status': 'failed',
                    'error': f'Unexpected error: {str(e)}'
                })
                # For unexpected errors, re-raise
                raise ETLException(
                    f"Unexpected error processing file: {file_name}"
                ) from e
        
        # Calculate final statistics
        stats['total_processing_time_sec'] = round(time.time() - etl_start_time, 2)
        
        logger.info("etl_process_completed", **stats)
        
        return stats
        
    except ConfigurationError:
        # Already logged, just re-raise
        raise
        
    except S3AccessError:
        # Already logged, just re-raise
        raise
        
    except Exception as e:
        logger.critical(
            "etl_process_failed",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        raise ETLException(f"ETL process failed: {str(e)}") from e