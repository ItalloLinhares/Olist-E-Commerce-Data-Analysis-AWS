import pandas as pd
import validations as validations
import clean_df

def validate_geolocation(geolocation):

    geolocation['geolocation_seq_id'] = geolocation.index + 1

    #Cria um dicionario para guardar todas os registros inválidos de cada coluna
    registros_invalidos_geolocation = {column: pd.DataFrame() for column in geolocation.columns}
    #Verifica se a coluna geolocation_lat é válida
    registros_invalidos_geolocation['geolocation_lat'] = validations.validar_formato_coordenada(geolocation, 'geolocation_lat')
    #Verifica se a coluna geolocation_lng é válida
    registros_invalidos_geolocation['geolocation_lng'] = validations.validar_formato_coordenada(geolocation, 'geolocation_lng')
    #Verifica se a coluna geolocation_city é válida
    registros_invalidos_geolocation['geolocation_city'] = validations.validar_formato_cidade(geolocation, 'geolocation_city')
    #Verifica se a coluna geolocation_state é válida
    registros_invalidos_geolocation['geolocation_state'] = validations.validar_formato_uf(geolocation, 'geolocation_state')
    
    lista_registros_invalidos_geolocation = list(registros_invalidos_geolocation.values())
    df_registros_invalidos_geolocation_combinado = pd.concat(lista_registros_invalidos_geolocation, ignore_index=True)
    dataframe_registros_geolocation_invalidos = df_registros_invalidos_geolocation_combinado.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')
    
    clean_df = clean_df.clean_df(geolocation, dataframe_registros_geolocation_invalidos, 'geolocation_seq_id')
    return clean_df