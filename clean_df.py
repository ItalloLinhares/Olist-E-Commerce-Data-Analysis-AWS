def clean_df(original_df, invalid_df, join_column):
    """
    Removes invalid rows from the original dataframe.
    """
    return original_df.join(invalid_df, on=join_column, how="left_anti")