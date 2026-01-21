import pandas as pd
import validations as validations
import clean_df
import logging

logger = logging.getLogger(__name__)

def validate_orders_payments(orders_payments):

    orders_payments['seq_id'] = orders_payments.index + 1

    #Cria um dicionario para guardar todas os registros inválidos de cada coluna
    registros_invalidos_orders_payments = {column: pd.DataFrame() for column in orders_payments.columns}
    #Verifica se a coluna order_id é válida
    registros_invalidos_orders_payments['order_id'] = validations.validar_formato_id_alfanumerico_32(orders_payments, 'order_id')
    #Verifica se a coluna order_id é válida
    registros_invalidos_orders_payments['payment_type'] = validations.validar_formato_forma_de_pagamento(orders_payments, 'payment_type')
    #Verifica se a coluna payment_sequential é válida
    registros_invalidos_orders_payments['payment_sequential'] = validations.validar_formato_numero_inteiro(orders_payments, 'payment_sequential')
    #Verifica se a coluna payment_installments é válida
    registros_invalidos_orders_payments['payment_installments'] = validations.validar_formato_numero_inteiro(orders_payments, 'payment_installments')
    #Verifica se a coluna payment_value é válida
    registros_invalidos_orders_payments['payment_value'] = validations.validar_formato_monetario(orders_payments, 'payment_value')
    lista_registros_invalidos_orders_payments = list(registros_invalidos_orders_payments.values())
    df_registros_invalidos_orders_payments_combinado = pd.concat(lista_registros_invalidos_orders_payments, ignore_index=True)
    dataframe_registros_payments_invalidos = df_registros_invalidos_orders_payments_combinado.drop_duplicates(subset=['order_id'], keep='first')
    
    cleaned_df = clean_df.clean_df(orders_payments, dataframe_registros_payments_invalidos, 'order_id')
    return cleaned_df