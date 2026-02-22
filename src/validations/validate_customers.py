from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from functools import reduce
import validations_module 
import clean_data_module

# Logger Configuration
logger = logging.getLogger(__name__)

def validate_customers_and_get_clean_data(customers_df: DataFrame) -> DataFrame:
    """
    Validates customer data and returns a clean DataFrame using PySpark.
    """
    
    customers_df = customers_df.withColumn("customer_seq_id", F.monotonically_increasing_id())

    invalid_customer_records = {}


    invalid_customer_records['customer_id'] = validations_module.validate_alphanumeric_id_32_format(
        customers_df, 'customer_id'
    )
    
    invalid_customer_records['customer_unique_id'] = validations_module.validate_alphanumeric_id_32_format(
        customers_df, 'customer_unique_id'
    )
    
    invalid_customer_records['customer_state'] = validations_module.validate_state_format(
        customers_df, 'customer_state'
    )

    invalid_dfs_list = list(invalid_customer_records.values())
    
    if invalid_dfs_list:
        combined_invalid_customers_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        unique_invalid_customers_df = combined_invalid_customers_df.dropDuplicates(['customer_seq_id'])
    else:
        unique_invalid_customers_df = customers_df.limit(0)

    cleaned_df = clean_data_module.clean_df(
        customers_df, 
        unique_invalid_customers_df, 
        'customer_seq_id'
    )

    return cleaned_df