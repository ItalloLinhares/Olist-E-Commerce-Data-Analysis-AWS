import pandas as pd
import validations as validations
import clean_df as clean_df
import logging

def validate_products(products):
    #Cria um dicionario para guardar todas os registros inválidos de cada coluna
    registros_invalidos_products = {column: pd.DataFrame() for column in products.columns}
    #Verifica se a coluna order_id é válida
    registros_invalidos_products['product_id'] = validations.validar_formato_id_alfanumerico_32(products, 'product_id')
    #Verifica se a coluna product_category_name é válida
    registros_invalidos_products['product_category_name'] = validations.validar_formato_products_category(products, 'product_category_name')
    #Verifica se a coluna product_name_lenght é válida
    registros_invalidos_products['product_name_lenght'] = validations.validar_formato_numero_inteiro(products, 'product_name_lenght')
    #Verifica se a coluna product_description_lenght é válida
    registros_invalidos_products['product_description_lenght'] = validations.validar_formato_numero_inteiro(products, 'product_description_lenght')
    #Verifica se a coluna product_category_name é válida
    registros_invalidos_products['product_photos_qty'] = validations.validar_formato_numero_inteiro(products, 'product_photos_qty')
    #Verifica se a coluna product_category_name é válida
    registros_invalidos_products['product_weight_g'] = validations.validar_formato_numero_inteiro(products, 'product_weight_g')
    #Verifica se a coluna product_category_name é válida
    registros_invalidos_products['product_length_cm'] = validations.validar_formato_numero_inteiro(products, 'product_length_cm')
    #Verifica se a coluna product_height_cm é válida
    registros_invalidos_products['product_height_cm'] = validations.validar_formato_numero_inteiro(products, 'product_height_cm')
    #Verifica se a coluna product_width_cm é válida
    registros_invalidos_products['product_width_cm'] = validations.validar_formato_numero_inteiro(products, 'product_width_cm')
    #Junta todas os dicionarios em um Dataframe e exclui as cópias para termos um Dataframe final com todos os valores inválidos
    lista_registros_invalidos_products = list(registros_invalidos_products.values())
    df_registros_invalidos_customers_combinado = pd.concat(lista_registros_invalidos_products, ignore_index=True)
    dataframe_registros_products_invalidos = df_registros_invalidos_customers_combinado.drop_duplicates()

    cleaned_df = clean_df.clean_df(products, dataframe_registros_products_invalidos, 'product_id')
    return cleaned_df
    




