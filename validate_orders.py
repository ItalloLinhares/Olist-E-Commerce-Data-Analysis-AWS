import validations as validations
import clean_df as clean_df
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def validate_orders(orders):
    orders['order_seq_id'] = orders.index + 1

    #Cria um dicionario para guardar todas os registros inválidos de cada coluna
    registros_invalidos_orders = {coluna: pd.DataFrame() for coluna in orders.columns}
    #Verifica se a coluna order_id é válida
    registros_invalidos_orders['order_id'] = validations.validar_formato_id_alfanumerico_32(orders, 'order_id')
    #Verifica se a coluna customer_id é válida
    registros_invalidos_orders['customer_id'] = validations.validar_formato_id_alfanumerico_32(orders, 'customer_id')
    #Verifica se a coluna order_status é válida
    registros_invalidos_orders['order_status'] = validations.validar_formato_order_status(orders, 'order_status')
    #Verifica se a coluna order_purchase_timestamp é válida
    registros_invalidos_orders['order_purchase_timestamp'] = validations.validar_formato_data_hora(orders, 'order_purchase_timestamp')
    #Verifica se a coluna order_approved_at é válida
    registros_invalidos_orders['order_approved_at'] = valores_invalidos_order_approved_at = validations.validar_formato_data_hora(orders, 'order_approved_at')
    #Verifica se a coluna order_delivered_carrier_date é válida
    registros_invalidos_orders['order_delivered_carrier_date'] = valores_invalidos_order_delivered_carrier_date = validations.validar_formato_data_hora(orders, 'order_delivered_carrier_date')
    #Verifica se a coluna order_delivered_customer_date é válida
    registros_invalidos_orders['order_delivered_customer_date'] = valores_invalidos_order_delivered_customer_date = validations.validar_formato_data_hora(orders, 'order_delivered_customer_date')
    #Verifica se a coluna order_estimated_delivery_date é válida
    registros_invalidos_orders['order_estimated_delivery_date'] = valores_invalidos_order_estimated_delivery_date = validations.validar_formato_data_hora(orders, 'order_estimated_delivery_date')
    #Junta todas os dicionarios em um Dataframe e exclui as cópias para termos um Dataframe final com todos os valores inválidos
    lista_registros_invalidos_orders = list(registros_invalidos_orders.values())
    df_registros_invalidos_orders_combinado = pd.concat(lista_registros_invalidos_orders, ignore_index=True)
    dataframe_registros_orders_invalidos = df_registros_invalidos_orders_combinado.drop_duplicates(subset=['order_id'], keep='first')
    
    cleaned_df = clean_df.clean_df(orders, dataframe_registros_orders_invalidos, 'order_seq_id')

    return cleaned_df