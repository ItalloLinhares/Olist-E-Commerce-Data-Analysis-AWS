from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import logging
import validations.validations as validations
import clean_df as clean_df

logger = logging.getLogger(__name__)

def validate_orders_reviews(orders_reviews_df: DataFrame) -> DataFrame:

    orders_reviews_df = orders_reviews_df.withColumn("order_review_seq_id", F.monotonically_increasing_id())

    invalid_reviews_records = {}
    
    # Validate IDs
    invalid_reviews_records['review_id'] = validations.validate_alphanumeric_id_32_format(
        orders_reviews_df, 'review_id'
    )
    invalid_reviews_records['order_id'] = validations.validate_alphanumeric_id_32_format(
        orders_reviews_df, 'order_id'
    )
    
    # Validate Score
    invalid_reviews_records['review_score'] = validations.validate_score_format(
        orders_reviews_df, 'review_score'
    )
    
    # Validate Dates
    invalid_reviews_records['review_creation_date'] = validations.validate_datetime_format(
        orders_reviews_df, 'review_creation_date'
    )
    invalid_reviews_records['review_answer_timestamp'] = validations.validate_datetime_format(
        orders_reviews_df, 'review_answer_timestamp'
    )
    
    # Consolidate errors
    invalid_dfs_list = list(invalid_reviews_records.values())
    
    if invalid_dfs_list:
        combined_invalid_reviews_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        unique_invalid_reviews_df = combined_invalid_reviews_df.dropDuplicates(['order_review_seq_id'])
    else:
        unique_invalid_reviews_df = orders_reviews_df.limit(0)
    
    cleaned_df = clean_df.clean_df(
        orders_reviews_df, 
        unique_invalid_reviews_df, 
        'order_review_seq_id'
    )
    
    return cleaned_df