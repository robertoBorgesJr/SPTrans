-- Se retornar alguma linha, o teste FALHA.

SELECT
    route_id,
    periodo_dia,
    headway_minimo_min,
    headway_maximo_min
FROM `sptrans_catalog`.`gtfs_data`.`gold_frequencia_por_linha`
WHERE headway_minimo_min > headway_maximo_min