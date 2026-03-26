# Script para auditoria de dados na camada Bronze
from utils.data_quality import validar_ingestao_bronze

results = []

print("\n" + "="*40)
print("RELATÓRIO DE RECONCILIAÇÃO (BRONZE)")
print("="*40)

for file in gtfs_files:
    target_table = f"{catalog}.{schema}.bronze_{file}"
    
    # Chamada da função de validação
    # path_today deve ser o caminho da pasta landing_zone do dia
    success = validar_ingestao_bronze(file, target_table, path_today)
    
    results.append({"tabela": file, "status": "✅ OK" if success else "❌ ERRO"})

# Exibe um resumo rápido no final
print("\nRESUMO FINAL:")
for res in results:
    print(f"{res['tabela']}: {res['status']}")

# Opcional: Interromper o pipeline se algo falhar
if any(r['status'] == "❌ ERRO" for r in results):
    raise Exception("Falha na reconciliação de dados. Verifique os logs acima.")