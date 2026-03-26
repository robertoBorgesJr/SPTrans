def validar_ingestao_bronze(file_name, catalog, schema, current_path):
    # Compara o arquivo físico na landing_zone com a tabela Delta.    

    target_table = f"{catalog}.{schema}.bronze_{file_name}"

    try:
        # 1. Lê o arquivo bruto da pasta identificada (CSV/TXT)
        file_path = f"{current_path}{file_name}.txt"
        raw_df = spark.read.option("header", "true").csv(file_path)
        raw_count = raw_df.count()
        
        # 2. Conta na tabela Delta o que entrou especificamente deste arquivo
        # Usamos o input_file_name() para garantir que contamos apenas o lote atual
        table_count = spark.table(target_table) \
            .filter(F.input_file_name().contains(file_name)) \
            .count()
            
        if raw_count == table_count:
            return True, raw_count, table_count
        else:
            return False, raw_count, table_count
            
    except Exception as e:
        print(f"Erro ao validar {file_name}: {str(e)}")
        return False, 0, 0