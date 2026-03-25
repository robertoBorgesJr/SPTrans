



select
    1
from `sptrans_catalog`.`gtfs_data`.`gold_frequencia_por_linha`

where not(headway_medio_min >= 0)

