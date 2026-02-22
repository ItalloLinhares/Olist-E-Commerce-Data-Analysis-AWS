from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import logging
import validations.validations as validations
import clean_df as clean_df

logger = logging.getLogger(__name__)

def validate_products(products_df: DataFrame) -> DataFrame:

    products_df = products_df.withColumn("product_seq_id", F.monotonically_increasing_id())

    invalid_products_records = {}
    
    # Validate ID and Category
    invalid_products_records['product_id'] = validations.validate_alphanumeric_id_32_format(
        products_df, 'product_id'
    )
    invalid_products_records['product_category_name'] = validations.validate_product_category_format(
        products_df, 'product_category_name'
    )
    
    # Validate Integers (Lengths, Dimensions, Weights)
    integer_columns = [
        'product_name_lenght',
        'product_description_lenght',
        'product_photos_qty',
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    ]
    
    for col in integer_columns:
        invalid_products_records[col] = validations.validate_integer_format(products_df, col)

    # Consolidate errors
    invalid_dfs_list = list(invalid_products_records.values())
    
    if invalid_dfs_list:
        combined_invalid_products_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        unique_invalid_products_df = combined_invalid_products_df.dropDuplicates(['product_seq_id'])
    else:
        unique_invalid_products_df = products_df.limit(0)

    cleaned_df = clean_df.clean_df(
        products_df, 
        unique_invalid_products_df, 
        'product_seq_id'
    )
    
    return cleaned_df