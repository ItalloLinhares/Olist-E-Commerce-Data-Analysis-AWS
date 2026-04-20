from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import structlog
from functools import reduce
import validations 
import clean_df

# Logger Configuration
logger = structlog.getLogger(__name__)

def validate_customers(customers_df: DataFrame) -> DataFrame:
    """
    Validates customer data and returns a clean DataFrame using PySpark.
    """
    customers_df = customers_df.withColumn("customer_seq_id", F.monotonically_increasing_id())

    invalid_customer_records = {}
    valid_customer_records = {}

    valid_customer_records['customer_id'], invalid_customer_records['customer_id'] = validations.validate_alphanumeric_id_32(
        customers_df, 'customer_id'
    )
    
    valid_customer_records['customer_unique_id'], invalid_customer_records['customer_unique_id'] = validations.validate_alphanumeric_id_32(
        customers_df, 'customer_unique_id'
    )
    
    valid_customer_records['customer_state'], invalid_customer_records['customer_state'] = validations.validate_state_uf_format(
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

    cleaned_df = clean_df.clean_df(
        customers_df, 
        unique_invalid_customers_df, 
        'customer_seq_id'
    )

    return cleaned_df