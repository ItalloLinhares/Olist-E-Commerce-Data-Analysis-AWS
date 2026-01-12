#Test
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

def run_etl_process(spark_session, input_path, output_path):
    # Verifica se o input_path termina com barra, se não, adiciona
    if not input_path.endswith('/'):
        input_path += '/'
        
    specific_file_path = f"{input_path}olist_customers_dataset.csv"
    
    logger.info(f"Iniciando processamento. Lendo arquivo especifico: {specific_file_path}")
    
    try:
        # 1. Ler CSV
        df_spark_input = spark_session.read.option("header", "true") \
                                           .option("inferSchema", "true") \
                                           .csv(specific_file_path)
        logger.info("Leitura do S3 (Spark) concluida.")

        # 2. Converter para Pandas
        df_pandas_input = df_spark_input.toPandas()
        logger.info(f"Conversao para Pandas concluida. Total linhas: {len(df_pandas_input)}")

        # 3. Aplicar validação
        df_pandas_clean = validate_customers_and_get_clean_data(df_pandas_input)
        logger.info("Regras de validacao aplicadas.")

        # 4. Salvar resultado (USANDO PANDAS DIRETO)
        if not df_pandas_clean.empty:
            
            # Garante que o output_path não termine com barra para montarmos o nome do arquivo
            base_output_path = output_path.rstrip('/')
            
            # Monta o caminho completo com o nome exato do arquivo desejado
            final_file_path = f"{base_output_path}/olist_customer_dataset.parquet"
            
            logger.info(f"Salvando arquivo unico via Pandas em: {final_file_path}")
            
            # Salva direto. Como já instalamos 's3fs' e 'pyarrow', isso funciona nativamente no S3
            df_pandas_clean.to_parquet(final_file_path, index=False)
            
            logger.info("Sucesso! Arquivo salvo.")
        else:
            logger.warning("O DataFrame resultante esta vazio. Nada foi gravado.")
            
    except Exception as e:
        logger.error(f"Erro durante o processamento ETL: {str(e)}")
        raise e