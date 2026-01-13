
# Nome do arquivo: customer_processor.py
import pandas as pd
import logging
import validations

# Cria uma instância do logger para este módulo
logger = logging.getLogger(__name__)

def validate_customers_and_get_clean_data(customers_df):
    registros_invalidos_cutomers = {column: pd.DataFrame() for column in customers_df.columns}
    
    registros_invalidos_cutomers['customer_id'] = validations.validar_formato_id_alfanumerico_32(customers_df, 'customer_id')
    registros_invalidos_cutomers['customer_unique_id'] = validations.validar_formato_id_alfanumerico_32(customers_df, 'customer_unique_id')
    #registros_invalidos_cutomers['customer_city'] = validations.validar_formato_cidade(customers_df, 'customer_city')
    registros_invalidos_cutomers['customer_state'] = validations.validar_formato_uf(customers_df, 'customer_state')
    
    lista_registros_invalidos_customers = list(registros_invalidos_cutomers.values())
    df_registros_invalidos_customers_combinado = pd.concat(lista_registros_invalidos_customers, ignore_index=True)
    dataframe_registros_customers_invalidos = df_registros_invalidos_customers_combinado.drop_duplicates()
    
    logger.info(f"Encontrados {len(dataframe_registros_customers_invalidos)} clientes invalidos.")

    if not dataframe_registros_customers_invalidos.empty:
        invalid_unique_ids = dataframe_registros_customers_invalidos['customer_unique_id'].unique()
        cleaned_customers_df = customers_df[~customers_df['customer_unique_id'].isin(invalid_unique_ids)]
    else:
        cleaned_customers_df = customers_df

    return cleaned_customers_df

