from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import logging
import validations.validations as validations
import clean_df as clean_df

logger = logging.getLogger(__name__)

def validate_order_items(order_items_df: DataFrame) -> DataFrame:

    # Create sequential ID
    order_items_df = order_items_df.withColumn("seq_id", F.monotonically_increasing_id())
    
    invalid_items_records = {}

    # Validate IDs
    ids_to_check = ['order_id', 'product_id', 'seller_id']
    for col in ids_to_check:
        invalid_items_records[col] = validations.validate_alphanumeric_id_32_format(
            order_items_df, col
        )

    # Validate Date
    invalid_items_records['shipping_limit_date'] = validations.validate_datetime_format(
        order_items_df, 'shipping_limit_date'
    )
    
    # Validate Monetary Values
    invalid_items_records['price'] = validations.validate_monetary_format(
        order_items_df, 'price'
    )
    invalid_items_records['freight_value'] = validations.validate_monetary_format(
        order_items_df, 'freight_value'
    )
    
    # Consolidate errors
    invalid_dfs_list = list(invalid_items_records.values())
    
    if invalid_dfs_list:
        combined_invalid_items_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        # Using seq_id for safer deduplication in Spark
        unique_invalid_items_df = combined_invalid_items_df.dropDuplicates(['seq_id'])
    else:
        unique_invalid_items_df = order_items_df.limit(0)
    
    # Clean Data
    cleaned_df = clean_df.clean_df(
        order_items_df, 
        unique_invalid_items_df, 
        'seq_id'
    )
    
    return cleaned_df