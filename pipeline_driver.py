def run_notebook(notebook_path):
    print(f"Executando notebook: {notebook_path}")
    dbutils.notebook.run(notebook_path, timeout_seconds=0)

# Caminhos relativos a partir do pipeline principal
layers = [
    "./raw/raw_job",
    "./bronze/bronze_job",
    "./silver/silver_job",
    "./gold/gold_job"
]

for layer in layers:
    run_notebook(layer)
