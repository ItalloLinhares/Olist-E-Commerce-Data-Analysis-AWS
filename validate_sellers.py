import pandas as pd
import validations as validations 
import clean_df as clean_df
import logging

logger = logging.getLogger(__name__)

def validate_sellers(sellers):
    #Cria um dicionario para guardar todas os registros inválidos de cada coluna
    registros_invalidos_sellers = {column: pd.DataFrame() for column in sellers.columns}
    #Verifica se a coluna seller_id é válida
    registros_invalidos_sellers['seller_id'] = validations.validar_formato_id_alfanumerico_32(sellers, 'seller_id')
    #Verifica se a coluna seller_city é válida
    registros_invalidos_sellers['seller_city'] = validations.validar_formato_cidade(sellers, 'seller_city')
    #Verifica se a coluna seller_state é válida
    registros_invalidos_sellers['seller_state'] = validations.validar_formato_uf(sellers, 'seller_state')

    cleaned_df = clean_df.clean_df(sellers, registros_invalidos_sellers, 'seller_id')
    return cleaned_df