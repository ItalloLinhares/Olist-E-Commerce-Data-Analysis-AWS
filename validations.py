def validar_formato_id_alfanumerico_32(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        print(f"ERRO: A coluna '{review_coluna}' não foi encontrada no DataFrame fornecido.")
        return pd.DataFrame()

    mascara_validos = (
        review_dataframe[review_coluna].notna() &
        (review_dataframe[review_coluna].astype(str).str.len() == 32) &
        (review_dataframe[review_coluna].astype(str).str.isalnum())
    )

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = total_validos/total_linhas*100
    percentual_invalidos = total_invalidos/total_linhas*100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos

    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_data_hora(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        print(f"ERRO: A coluna '{review_coluna}' não foi encontrada no DataFrame fornecido.")
        return pd.DataFrame()

    formato_esperado = '%Y-%m-%d %H:%M:%S'

    datas_convertidas = pd.to_datetime(
        review_dataframe[review_coluna],
        format=formato_esperado,
        errors='coerce'
    )
    mascara_validos = datas_convertidas.notna()

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = total_validos/total_linhas*100
    percentual_invalidos = total_invalidos/total_linhas*100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos

    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_monetario(review_dataframe, review_coluna):
    import pandas as pd
    import numpy as np

    numeric_values = pd.to_numeric(review_dataframe[review_coluna], errors='coerce')

    is_positive = numeric_values > 0

    has_up_to_two_decimals = np.isclose((numeric_values * 100) % 1, 0)

    mascara_validos = numeric_values.notna() & is_positive & has_up_to_two_decimals

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100

    percentual_validos = total_validos/total_linhas*100
    percentual_invalidos = total_invalidos/total_linhas*100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_numero_inteiro(review_dataframe, review_coluna):
    import pandas as pd

    numeric_values = pd.to_numeric(review_dataframe[review_coluna], errors='coerce')

    is_positive = numeric_values > 0

    is_integer = numeric_values % 1 == 0

    mascara_validos = numeric_values.notna() & is_positive & is_integer

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = total_validos/total_linhas*100
    percentual_invalidos = total_invalidos/total_linhas*100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_cep(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    mascara_validos = review_dataframe[review_coluna].apply(
        lambda x: len(str(x)) == 5 and str(x).isdigit() if pd.notna(x) else False
    )

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = total_validos/total_linhas*100
    percentual_invalidos = total_invalidos/total_linhas*100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def _normalize_text_for_city_validation(text):
    import unicodedata

    if not isinstance(text, str):
        return ""
    return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode('utf-8').lower()

def validar_formato_cidade(review_dataframe, review_coluna, _cache={'valid_municipios_set': None}):
    # import requests
    # import pandas as pd

    # if _cache['valid_municipios_set'] is None:
    #     try:
    #         response = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios")
    #         response.raise_for_status()
    #         municipios_data = response.json()
    #         _cache['valid_municipios_set'] = {
    #             _normalize_text_for_city_validation(municipio['nome']) for municipio in municipios_data
    #         }
    #     except requests.exceptions.RequestException:
    #         _cache['valid_municipios_set'] = set()

    # valid_municipios_set = _cache['valid_municipios_set']

    # if review_coluna not in review_dataframe.columns:
    #     return pd.DataFrame()

    # mascara_validos = review_dataframe[review_coluna].apply(
    #     lambda x: _normalize_text_for_city_validation(x) in valid_municipios_set
    # )

    # total_linhas = len(review_dataframe)
    # total_validos = mascara_validos.sum()
    # total_invalidos = total_linhas - total_validos
    # percentual_validos = (total_validos / total_linhas) * 100
    # percentual_invalidos = (total_invalidos / total_linhas) * 100

    # show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    # mascara_invalidos = ~mascara_validos
    # dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return review_dataframe

def validar_formato_uf(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    valid_ufs_set = {
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
        'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR',
        'SC', 'SP', 'SE', 'TO'
    }
    mascara_validos = review_dataframe[review_coluna].apply(
        lambda x: str(x).upper() if pd.notna(x) else ''
    ).isin(valid_ufs_set)

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_coordenada(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    numeric_values = pd.to_numeric(review_dataframe[review_coluna], errors='coerce')

    mascara_validos = numeric_values.notna() & (numeric_values >= -90) & (numeric_values <= 90)

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos

    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos



def validar_formato_products_category(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    valid_options_set = {
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
    }

    mascara_validos = review_dataframe[review_coluna].astype(str).str.lower().isin(valid_options_set)
    #is_nan = review_dataframe[review_coluna].isna()
    #mascara_validos = is_in_set | is_nan

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100
    
    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_order_status(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    valid_options_set = {
        "delivered", "shipped", "canceled", "unavailable",
        "invoiced", "processing", "created", "approved"
    }

    mascara_validos = review_dataframe[review_coluna].astype(str).str.lower().isin(valid_options_set)

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100
    
    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_forma_de_pagamento(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    valid_options_set = {
        "credit_card", "boleto", "voucher", "debit_card", "not_defined"
    }

    mascara_validos = review_dataframe[review_coluna].astype(str).str.lower().isin(valid_options_set)

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos

    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_monetario(review_dataframe, review_coluna):
    import pandas as pd
    import numpy as np

    numeric_values = pd.to_numeric(review_dataframe[review_coluna], errors='coerce')

    is_positive = numeric_values > 0

    multiplicado_e_arredondado = np.round(numeric_values * 100, decimals=5)
    has_up_to_two_decimals = (multiplicado_e_arredondado  % 1) == 0

    mascara_validos = numeric_values.notna() & is_positive & has_up_to_two_decimals

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def validar_formato_score(review_dataframe, review_coluna):
    import pandas as pd

    if review_coluna not in review_dataframe.columns:
        return pd.DataFrame()

    numeric_values = pd.to_numeric(review_dataframe[review_coluna], errors='coerce')

    mascara_validos = (
        numeric_values.notna() &
        (numeric_values >= 0) &
        (numeric_values <= 5) &
        (numeric_values % 1 == 0)
    )

    total_linhas = len(review_dataframe)
    total_validos = mascara_validos.sum()
    total_invalidos = total_linhas - total_validos
    percentual_validos = (total_validos / total_linhas) * 100
    percentual_invalidos = (total_invalidos / total_linhas) * 100

    show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos)

    mascara_invalidos = ~mascara_validos
    dataframe_invalidos = review_dataframe[mascara_invalidos].copy()

    return dataframe_invalidos

def show_results(review_coluna, total_linhas, total_validos, total_invalidos, percentual_validos, percentual_invalidos):
    print(f"="*5 + f"RESULTADOS PARA A COLUNA {review_coluna.upper()}" + f"="*5)
    print(f"Linhas Válidas da Coluna {review_coluna} no dataframe: {total_validos} de {total_linhas} ({percentual_validos:.2f}%)")
    print(f"Linhas Inválidas da Coluna {review_coluna} no dataframe: {total_invalidos} de {total_linhas} ({percentual_invalidos:.2f}%) \n")

def test():
    return null