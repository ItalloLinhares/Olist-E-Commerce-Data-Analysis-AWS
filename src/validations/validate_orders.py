from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from functools import reduce

# Importing custom modules (Assuming these are also renamed/migrated)
import validations as validations
import clean_df as clean_df

# Logger configuration
logger = logging.getLogger(__name__)

def validate_customers_and_get_clean_data(orders_df: DataFrame) -> DataFrame:
    """
    Validates the orders DataFrame and returns a cleaned version.
    """
    
    # Create a sequential ID for tracking rowss.
    orders_df = orders_df.withColumn("order_seq_id", F.monotonically_increasing_id())

    # Dictionary to store invalid records for each column
    invalid_orders_records = {}

    # --- VALIDATION STEPS ---
    
    # Validate order_id format
    invalid_orders_records['order_id'] = validations.validate_alphanumeric_id_32_format(
        orders_df, 'order_id'
    )
    
    # Validate customer_id format
    invalid_orders_records['customer_id'] = validations.validate_alphanumeric_id_32_format(
        orders_df, 'customer_id'
    )
    
    # Validate order_status format
    invalid_orders_records['order_status'] = validations.validate_order_status_format(
        orders_df, 'order_status'
    )
    
    # Validate datetime columns
    datetime_columns = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]

    for col_name in datetime_columns:
        invalid_orders_records[col_name] = validations.validate_datetime_format(
            orders_df, col_name
        )

    # --- CONSOLIDATION STEPS ---

    # Convert dictionary values (DataFrames) to a list
    invalid_orders_list = list(invalid_orders_records.values())
    
    # Check if there are any invalid DataFrames to combine
    if invalid_orders_list:
        # Combine all invalid DataFrames into one using reduce and unionByName
        combined_invalid_orders_df = reduce(
            DataFrame.unionByName, 
            invalid_orders_list
        )
        
        # Remove duplicates based on 'order_id' to get a unique list of invalid rows
        unique_invalid_orders_df = combined_invalid_orders_df.dropDuplicates(['order_seq_id'])
    else:
        # If no errors found, create an empty DataFrame with the same schema
        unique_invalid_orders_df = orders_df.limit(0)
    
    # --- CLEANING STEP ---
    
    # Remove the invalid rows from the original DataFrame
    cleaned_df = clean_df.clean_df(
        orders_df, 
        unique_invalid_orders_df, 
        'order_seq_id'
    )

    return cleaned_df