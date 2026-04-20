from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple, List, Optional

def show_results(column: str, total: int, valid: int, invalid: int):
    """Helper function to print validation statistics."""
    valid_pct = (valid / total * 100) if total > 0 else 0.0
    invalid_pct = (invalid / total * 100) if total > 0 else 0.0
    
    print(f"{'='*5} RESULTS FOR COLUMN {column.upper()} {'='*5}")
    print(f"Valid rows for '{column}': {valid} of {total} ({valid_pct:.2f}%)")
    print(f"Invalid rows for '{column}': {invalid} of {total} ({invalid_pct:.2f}%) \n")

def validate_alphanumeric_id_32(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates if the ID is alphanumeric and has exactly 32 characters."""
    
    # Define validation condition
    validation_condition = (
        (F.col(column).isNotNull()) &
        (F.length(F.col(column)) == 32) &
        (F.col(column).rlike("^[a-zA-Z0-9]{32}$"))
    )
    
    # Create temp column
    df_checked = df.withColumn("is_valid", validation_condition)
    
    # Split DataFrames
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    # Statistics
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_datetime_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates if the column matches the format YYYY-MM-DD HH:MM:SS."""
    
    expected_format = "yyyy-MM-dd HH:mm:ss"
    
    # Try to cast to timestamp, if null it failed
    validation_condition = (
        F.to_timestamp(F.col(column), expected_format).isNotNull()
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_monetary_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates positive numbers with up to 2 decimal places."""
    
    # Convert to float/double just in case
    col_val = F.col(column).cast("double")
    
    validation_condition = (
        col_val.isNotNull() &
        (col_val > 0) &
        # Check decimals: multiply by 100 and check if it's an integer
        ((F.round(col_val * 100, 5) % 1) == 0)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_integer_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates if the value is a positive integer."""
    
    col_val = F.col(column).cast("double")
    
    validation_condition = (
        col_val.isNotNull() &
        (col_val > 0) &
        ((col_val % 1) == 0)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_zip_code_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates ZIP codes (5 digits string)."""
    
    validation_condition = (
        F.col(column).isNotNull() &
        (F.length(F.col(column).cast("string")) == 5) &
        (F.col(column).cast("string").rlike("^[0-9]{5}$"))
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_state_uf_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates Brazilian State codes (UF)."""
    
    valid_ufs = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
        'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR',
        'SC', 'SP', 'SE', 'TO'
    ]
    
    validation_condition = (
        F.col(column).isNotNull() &
        F.upper(F.col(column)).isin(valid_ufs)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_coordinate_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates coordinates (latitude/longitude between -90 and 90)."""
    
    col_val = F.col(column).cast("double")
    
    validation_condition = (
        col_val.isNotNull() &
        (col_val >= -90) & 
        (col_val <= 90)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_product_category_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates product categories against a fixed list."""
    
    valid_options = [
        'perfumaria', 'artes', 'esporte_lazer', 'bebes',
        'utilidades_domesticas', 'instrumentos_musicais', 'cool_stuff',
        'moveis_decoracao', 'eletrodomesticos', 'brinquedos',
        'cama_mesa_banho', 'construcao_ferramentas_seguranca',
        'informatica_acessorios', 'beleza_saude', 'malas_acessorios',
        'ferramentas_jardim', 'moveis_escritorio', 'automotivo',
        'eletronicos', 'fashion_calcados', 'telefonia', 'papelaria',
        'fashion_bolsas_e_acessorios', 'pcs', 'casa_construcao',
        'relogios_presentes', 'construcao_ferramentas_construcao',
        'pet_shop', 'eletroportateis', 'agro_industria_e_comercio',
        'moveis_sala', 'sinalizacao_e_seguranca', 'climatizacao',
        'consoles_games', 'livros_interesse_geral',
        'construcao_ferramentas_ferramentas',
        'fashion_underwear_e_moda_praia', 'fashion_roupa_masculina',
        'moveis_cozinha_area_de_servico_jantar_e_jardim',
        'industria_comercio_e_negocios', 'telefonia_fixa',
        'construcao_ferramentas_iluminacao', 'livros_tecnicos',
        'eletrodomesticos_2', 'artigos_de_festas', 'bebidas',
        'market_place', 'la_cuisine', 'construcao_ferramentas_jardim',
        'fashion_roupa_feminina', 'casa_conforto', 'audio',
        'alimentos_bebidas', 'musica', 'alimentos',
        'tablets_impressao_imagem', 'livros_importados',
        'portateis_casa_forno_e_cafe', 'fashion_esporte',
        'artigos_de_natal', 'fashion_roupa_infanto_juvenil',
        'dvds_blu_ray', 'artes_e_artesanato', 'pc_gamer', 'moveis_quarto',
        'cine_foto', 'fraldas_higiene', 'flores', 'casa_conforto_2',
        'portateis_cozinha_e_preparadores_de_alimentos',
        'seguros_e_servicos', 'moveis_colchao_e_estofado',
        'cds_dvds_musicais'
    ]
    
    validation_condition = (
        F.lower(F.col(column)).isin(valid_options)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_order_status_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates order status."""
    
    valid_options = [
        "delivered", "shipped", "canceled", "unavailable",
        "invoiced", "processing", "created", "approved"
    ]
    
    validation_condition = (
        F.lower(F.col(column)).isin(valid_options)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_payment_method_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates payment methods."""
    
    valid_options = [
        "credit_card", "boleto", "voucher", "debit_card", "not_defined"
    ]
    
    validation_condition = (
        F.lower(F.col(column)).isin(valid_options)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid

def validate_score_format(df: DataFrame, column: str) -> Tuple[DataFrame, DataFrame]:
    """Validates scores (integers between 0 and 5)."""
    
    col_val = F.col(column).cast("double")
    
    validation_condition = (
        col_val.isNotNull() &
        (col_val >= 0) &
        (col_val <= 5) &
        ((col_val % 1) == 0)
    )

    df_checked = df.withColumn("is_valid", validation_condition)
    
    df_valid = df_checked.filter(F.col("is_valid") == True).drop("is_valid")
    df_invalid = df_checked.filter(F.col("is_valid") == False).drop("is_valid")
    
    show_results(column, df.count(), df_valid.count(), df_invalid.count())
    
    return df_valid, df_invalid