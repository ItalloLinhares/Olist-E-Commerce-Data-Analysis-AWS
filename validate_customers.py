
import pandas as pd
import logging
import validations
import clean_df as clean_df

# Cria uma instância do logger para este módulo
logger = logging.getLogger(__name__)

def validate_customers_and_get_clean_data(customers_df):

    customers_df['customer_seq_id'] = customers_df.index + 1

    registros_invalidos_cutomers = {column: pd.DataFrame() for column in customers_df.columns}
    
    registros_invalidos_cutomers['customer_id'] = validations.validar_formato_id_alfanumerico_32(customers_df, 'customer_id')
    registros_invalidos_cutomers['customer_unique_id'] = validations.validar_formato_id_alfanumerico_32(customers_df, 'customer_unique_id')
    #registros_invalidos_cutomers['customer_city'] = validations.validar_formato_cidade(customers_df, 'customer_city')
    registros_invalidos_cutomers['customer_state'] = validations.validar_formato_uf(customers_df, 'customer_state')
    
    lista_registros_invalidos_customers = list(registros_invalidos_cutomers.values())
    df_registros_invalidos_customers_combinado = pd.concat(lista_registros_invalidos_customers, ignore_index=True)
    dataframe_registros_customers_invalidos = df_registros_invalidos_customers_combinado.drop_duplicates()
    
    cleaned_df = clean_df.clean_df(customers_df, dataframe_registros_customers_invalidos, 'customer_seq_id')
    return cleaned_df


