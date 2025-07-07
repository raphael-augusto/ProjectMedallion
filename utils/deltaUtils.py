def log(layer, msg):
    from datetime import datetime
    print(f"[{layer.upper()}][{datetime.now().isoformat()}] {msg}")

def read_table(spark, table):
    log("UTILS", f"Lendo tabela {table}")
    return spark.table(table)

def write_table(df, table, mode="overwrite"):
    log("UTILS", f"Gravando tabela {table} (registros: {df.count()})")
    df.write.format("delta").mode(mode).saveAsTable(table)
