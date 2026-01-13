import pandas as pd
import logging
import boto3
from urllib.parse import urlparse
import sys
import validations
import validate_customers


FILE_PROCESSOR_MAP = {
    "olist_customers_dataset.csv": {
        "function": validate_customers.validate_customers_and_get_clean_data,
        "output_name": "olist_customers_dataset.parquet"
    }}

logger = logging.getLogger(__name__)

def list_s3_files(s3_path):
    """Lista todos os arquivos CSV dentro de uma pasta S3"""
    s3 = boto3.client('s3')
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            # Filtra apenas CSV e ignora pastas
            if key.endswith('.csv') and not key.endswith('/'):
                # Reconstrói o caminho completo s3://...
                full_path = f"s3://{bucket}/{key}"
                files.append(full_path)
    return files

def run_etl_process(spark_session, input_path, output_path):
    # Verifica se o input_path termina com barra, se não, adiciona
    if not input_path.endswith('/'): input_path += '/'
    if not output_path.endswith('/'): output_path += '/'

    logger.info(f"Listando arquivos na pasta: {input_path}")
    all_s3_files = list_s3_files(input_path)


    for file_uri in all_s3_files:
        try:
            file_name = file_uri.split('/')[-1]

            logger.info(f"--- Iniciando arquivo: {file_name} ---")

            if file_name in FILE_PROCESSOR_MAP:
                config = FILE_PROCESSOR_MAP[file_name]
                processor_function = config["function"]
                final_output_name = config["output_name"]

                df_spark = spark_session.read.option("header", "true").option("inferSchema", "true").csv(file_uri)
                df_pandas = df_spark.toPandas()
                df_clean = processor_function(df_pandas)

                if not df_clean.empty:
                    final_path = f"{output_path}{final_output_name}"
                    logger.info(f"Salvando resultado em: {final_path}")
                    df_clean.to_parquet(final_path, index=False)
                else:
                    logger.warning(f"Arquivo '{file_name}' ignorado pois não está no mapa de processamento.")
        except Exception as e:
            logger.error(f"Erro ao processar o arquivo {file_uri}: {str(e)}")
            # O 'continue' faz o loop ir para o próximo arquivo mesmo se este falhar
            continue           