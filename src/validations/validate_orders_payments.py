from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import logging
import validations.validations as validations
import clean_df as clean_df

logger = logging.getLogger(__name__)

def validate_orders_payments(orders_payments_df: DataFrame) -> DataFrame:

    orders_payments_df = orders_payments_df.withColumn("seq_id", F.monotonically_increasing_id())

    invalid_payments_records = {}

    # Validations
    invalid_payments_records['order_id'] = validations.validate_alphanumeric_id_32_format(
        orders_payments_df, 'order_id'
    )
    
    invalid_payments_records['payment_type'] = validations.validate_payment_type_format(
        orders_payments_df, 'payment_type'
    )
    
    invalid_payments_records['payment_sequential'] = validations.validate_integer_format(
        orders_payments_df, 'payment_sequential'
    )
    
    invalid_payments_records['payment_installments'] = validations.validate_integer_format(
        orders_payments_df, 'payment_installments'
    )
    
    invalid_payments_records['payment_value'] = validations.validate_monetary_format(
        orders_payments_df, 'payment_value'
    )
    
    # Consolidate errors
    invalid_dfs_list = list(invalid_payments_records.values())
    
    if invalid_dfs_list:
        combined_invalid_payments_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        # Note: Switched to seq_id for deduplication as order_id is not unique in payments
        unique_invalid_payments_df = combined_invalid_payments_df.dropDuplicates(['seq_id'])
    else:
        unique_invalid_payments_df = orders_payments_df.limit(0)
    
    # Clean Data
    # Note: Using 'seq_id' for the join is safer than 'order_id'
    cleaned_df = clean_df.clean_df(
        orders_payments_df, 
        unique_invalid_payments_df, 
        'seq_id'
    )
    
    return cleaned_df