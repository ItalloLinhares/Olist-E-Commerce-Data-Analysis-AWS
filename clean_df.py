import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_df(df_to_clean, dataframe_registros_invalidos, primary_key):
    logger.info(f"Encontrados {len(dataframe_registros_invalidos)} clientes invalidos.")

    if not dataframe_registros_invalidos.empty:
        invalid_unique_ids = dataframe_registros_invalidos[primary_key].unique()
        cleaned_df = df_to_clean[~df_to_clean[primary_key].isin(invalid_unique_ids)]
    else:
        cleaned_df = df_to_clean

    return cleaned_df