from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import logging
import validations.validations as validations 
import clean_df as clean_df

logger = logging.getLogger(__name__)

def validate_sellers(sellers_df: DataFrame) -> DataFrame:

    sellers_df = sellers_df.withColumn("seller_seq_id", F.monotonically_increasing_id())

    invalid_sellers_records = {}
    
    # Validate Seller ID
    invalid_sellers_records['seller_id'] = validations.validate_alphanumeric_id_32_format(
        sellers_df, 'seller_id'
    )
    
    # Commented out in original, kept commented here
    # invalid_sellers_records['seller_city'] = validations.validate_city_format(sellers_df, 'seller_city')
    # invalid_sellers_records['seller_state'] = validations.validate_state_format(sellers_df, 'seller_state')

    # Consolidate errors
    invalid_dfs_list = list(invalid_sellers_records.values())
    
    if invalid_dfs_list:
        combined_invalid_sellers_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        unique_invalid_sellers_df = combined_invalid_sellers_df.dropDuplicates(['seller_seq_id'])
    else:
        unique_invalid_sellers_df = sellers_df.limit(0)

    cleaned_df = clean_df.clean_df(
        sellers_df, 
        unique_invalid_sellers_df, 
        'seller_seq_id'
    )
    
    return cleaned_df