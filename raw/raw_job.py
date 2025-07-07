from config.config import API_URL
from utils.apiIngest import fetch_api_to_spark
from utils.deltaUtils import log, write_table

# from config.config import API_URL, TABLES

# Test the import by printing the values
# print("API_URL:", API_URL)
# print("TABLES:", TABLES)


# log("RAW", "Iniciando ingestão API")
df = fetch_api_to_spark(
    spark,
    api_url=API_URL,
    params=None,
    headers=None,
    json_path=None,      # Não precisa, já retorna lista direto
    pages=2,             # Quantas páginas buscar
    pagination_key="since"
)
df.show()

# # write_table(df_raw, TABLES["raw"])
# log("RAW", "RAW finalizada")

