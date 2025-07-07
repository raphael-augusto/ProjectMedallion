DATABASE = "medallion_db"
API_URL = "https://api.github.com/repositories"

TABLES = {
    "raw":     f"{DATABASE}.raw_api_data",
    "bronze":  f"{DATABASE}.bronze_api_data",
    "silver":  f"{DATABASE}.silver_api_data",
    "gold":    f"{DATABASE}.gold_grouped_api"
}
