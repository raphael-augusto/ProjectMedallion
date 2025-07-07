from config.config import TABLES
from utils.delta_utils import log, read_table, write_table
from pyspark.sql.functions import current_timestamp

log("BRONZE", "Lendo RAW")
df_raw = read_table(spark, TABLES["raw"])

log("BRONZE", "Deduplicando e marcando timestamp")
df_bronze = df_raw.dropDuplicates(["OFFENSE_CODE", "OFFENSE_DESCRIPTION"])\
                  .withColumn("bronze_ingestion_ts", current_timestamp())

write_table(df_bronze, TABLES["bronze"])
log("BRONZE", "BRONZE finalizada")
