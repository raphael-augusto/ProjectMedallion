

# silver/silver_job.py

from config.config import TABLES
from utils.delta_utils import log, read_table, write_table

log("SILVER", "Lendo BRONZE")
df_bronze = read_table(spark, TABLES["bronze"])

log("SILVER", "Tratando nulos, deduplicando")
df_silver = df_bronze.dropDuplicates(["OFFENSE_CODE", "OFFENSE_DESCRIPTION"])\
                     .dropna(subset=["OFFENSE_DESCRIPTION"])

write_table(df_silver, TABLES["silver"])
log("SILVER", "SILVER finalizada")

     