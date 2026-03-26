# Script para auditoria de dados na camada Bronze

%run "../utils/config"

from utils.data_quality import validar_ingestao_bronze
import datetime
from pyspark.sql import functions as F

results = []

print("\n" + "="*40)
print("RELATÓRIO DE RECONCILIAÇÃO (BRONZE)")
print("="*40)

dbutils.widgets.text("data_processamento", datetime.datetime.now().strftime("%Y-%m-%d"))
data_alvo = dbutils.widgets.get("data_processamento")

path_today = f"{volume_path}{data_alvo}/"

for file in gtfs_files:
    
    # Chamada da função de validação
    # path_today deve ser o caminho da pasta landing_zone do dia
    is_valid, r_count, t_count = validar_ingestao_bronze(file, catalog, schema, path_today)
    
    status = "✅" if is_valid else "❌"
    print(f"{status} {file.ljust(15)} | Origem: {r_count} | Destino: {t_count}")

    if not is_valid:
        print(f"   ⚠️ ALERTA: Divergência detectada em {file}!")