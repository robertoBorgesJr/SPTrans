def validar_ingestao_bronze(file_name, target_table, path_today):
    # 1. Conta registros no arquivo bruto (CSV)
    raw_count = spark.read.option("header", "true").csv(f"{path_today}{file_name}.txt").count()
    
    # 2. Conta registros na tabela Bronze que entraram HOJE
    # (Considerando que você tem uma coluna 'data_processamento' ou similar)
    from pyspark.sql.functions import current_date
    
    table_count = spark.table(target_table) \
        .filter(F.col("data_ingestao").cast("date") == F.current_date()) \
        .count()
    
    # 3. Comparação
    if raw_count == table_count:
        print(f"✅ Sucesso: {file_name} reconciliado ({raw_count} linhas).")
        return True
    else:
        diff = raw_count - table_count
        print(f"❌ Erro: Divergência no arquivo {file_name}! Diferença de {diff} linhas.")
        return False