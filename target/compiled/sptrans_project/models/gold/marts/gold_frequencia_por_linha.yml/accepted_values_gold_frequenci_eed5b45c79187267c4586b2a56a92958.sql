
    
    

with all_values as (

    select
        periodo_dia as value_field,
        count(*) as n_records

    from `sptrans_catalog`.`gtfs_data`.`gold_frequencia_por_linha`
    group by periodo_dia

)

select *
from all_values
where value_field not in (
    'madrugada','pico_manha','manha','tarde','pico_tarde','noite','noite_tardia'
)


