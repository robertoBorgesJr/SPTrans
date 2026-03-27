import sys
import os
import datetime
from pyspark.sql import functions as F
import pandas as pd

sys.path.append(os.path.abspath('..'))

# 2. Imports dos módulos (arquivos .py)
from utils.config import catalog, schema, volume_path, gtfs_files
from utils.data_quality import validar_ingestao_bronze

# 3. Configuração de Widgets (O Databricks reconhece isso mesmo em .py)
dbutils.widgets.text("data_processamento", datetime.datetime.now().strftime("%Y-%m-%d"))
data_alvo = dbutils.widgets.get("data_processamento")

path_today = f"{volume_path}{data_alvo}/"

# 4. Lógica de Auditoria
print("="*50)
print(f"AUDITORIA BRONZE - DATA: {data_alvo}")
print("="*50)

results = []
for file in gtfs_files:
    is_valid, r_count, t_count = validar_ingestao_bronze(file, catalog, schema, path_today)
    
    status = "✅" if is_valid else "❌"
    print(f"{status} {file.ljust(15)} | Origem: {r_count} | Destino: {t_count}")
    
    results.append({
        "arquivo": file, 
        "status": "SUCESSO" if is_valid else "FALHA", 
        "origem": r_count, 
        "destino": t_count
    })

# Exibição
display(pd.DataFrame(results))