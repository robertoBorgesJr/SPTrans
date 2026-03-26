# Script para auditoria de dados na camada Bronze
from utils.data_quality import validar_ingestao_bronze

results = []

catalog = "sptrans_catalog"
schema = "gtfs_data"

print("\n" + "="*40)
print("RELATÓRIO DE RECONCILIAÇÃO (BRONZE)")
print("="*40)

for file in gtfs_files:
    
    # Chamada da função de validação
    # path_today deve ser o caminho da pasta landing_zone do dia
    is_valid, r_count, t_count = validar_ingestao_bronze(file, catalog, schema, path_today)
    
    status = "✅" if is_valid else "❌"
    print(f"{status} {file.ljust(15)} | Origem: {r_count} | Destino: {t_count}")

    if not is_valid:
        print(f"   ⚠️ ALERTA: Divergência detectada em {file}!")