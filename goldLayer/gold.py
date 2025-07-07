from config.config import TABLES
from utils.delta_utils import log, read_table, write_table
from pyspark.sql.functions import count

log("GOLD", "Lendo SILVER")
df_silver = read_table(spark, TABLES["silver"])

log("GOLD", "Gerando agregados")
df_gold = df_silver.groupBy("OFFENSE_CODE_GROUP")\
                   .agg(count("*").alias("ocorrencias"))\
                   .orderBy("ocorrencias", ascending=False)

write_table(df_gold, TABLES["gold"])
log("GOLD", "GOLD finalizada")