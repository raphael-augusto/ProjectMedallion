import requests
import pandas as pd

def fetch_api_to_spark(
    spark, 
    api_url, 
    params=None, 
    headers=None, 
    json_path=None, 
    pages=1, 
    pagination_key=None
):
    """
    spark: SparkSession ativa
    api_url: URL base do endpoint da API
    params: Dicionário de parâmetros iniciais da requisição
    headers: Headers HTTP opcionais (ex: autenticação)
    json_path: Lista com o caminho até o campo desejado do JSON
    pages: Quantidade de páginas a buscar (default: 1)
    pagination_key: Nome do parâmetro de paginação
    return:Spark DataFrame
    """
    all_data = []
    cur_params = params.copy() if params else {}
    next_pagination_value = None

    for _ in range(pages):
        if pagination_key and next_pagination_value is not None:
            cur_params[pagination_key] = next_pagination_value

        resp = requests.get(api_url, params=cur_params, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        # Se precisar navegar por um caminho dentro do JSON (ex: ["items"])
        if json_path:
            for key in json_path:
                data = data[key]

        if not data:
            break

        all_data.extend(data)

        # Atualiza paginação para próxima iteração
        if pagination_key:
            # Exemplo GitHub: since = id do último repo
            if isinstance(data, list) and data:
                next_pagination_value = data[-1].get('id')
            elif isinstance(data, dict) and pagination_key in data:
                next_pagination_value = data[pagination_key]
            else:
                break

    df_pd = pd.json_normalize(all_data)
    return spark.createDataFrame(df_pd)
