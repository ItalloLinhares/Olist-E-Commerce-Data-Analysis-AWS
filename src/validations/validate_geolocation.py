from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
import logging

# Assuming modules are renamed
import validations.validations as validations
import clean_df as clean_df

logger = logging.getLogger(__name__)

def validate_geolocation(geolocation_df: DataFrame) -> DataFrame:
    
    # Create sequential ID
    geolocation_df = geolocation_df.withColumn("geolocation_seq_id", F.monotonically_increasing_id())

    # Dictionary to store invalid records
    invalid_geolocation_records = {}
    
    # Validate Coordinates
    invalid_geolocation_records['geolocation_lat'] = validations.validate_coordinate_format(
        geolocation_df, 'geolocation_lat'
    )
    invalid_geolocation_records['geolocation_lng'] = validations.validate_coordinate_format(
        geolocation_df, 'geolocation_lng'
    )
    
    # Validate City
    invalid_geolocation_records['geolocation_city'] = validations.validate_city_format(
        geolocation_df, 'geolocation_city'
    )
    
    # Validate State (UF)
    invalid_geolocation_records['geolocation_state'] = validations.validate_state_format(
        geolocation_df, 'geolocation_state'
    )
    
    # Consolidate errors
    invalid_dfs_list = list(invalid_geolocation_records.values())
    
    if invalid_dfs_list:
        combined_invalid_geolocation_df = reduce(
            DataFrame.unionByName, 
            invalid_dfs_list
        )
        unique_invalid_geolocation_df = combined_invalid_geolocation_df.dropDuplicates(['geolocation_seq_id'])
    else:
        unique_invalid_geolocation_df = geolocation_df.limit(0)

    # Clean Data
    cleaned_df = clean_df.clean_df(
        geolocation_df, 
        unique_invalid_geolocation_df, 
        'geolocation_seq_id'
    )
    
    return cleaned_df