

WITH tarifa_rota AS (
  SELECT
    r.route_id,
    r.route_short_name                            AS numero_linha,
    r.route_long_name                             AS nome_linha,
    r.route_type,
    fa.fare_id,
    fa.price                                      AS preco_brl,
    fa.currency_type                              AS moeda,
    fa.payment_method,
    CASE fa.payment_method
      WHEN 0  THEN 'pagamento_a_bordo'
      WHEN 1  THEN 'pagamento_antecipado'
      ELSE        'nao_informado'
    END                                           AS descricao_pagamento,
    -- Transferências permitidas
    CASE
      WHEN fa.transfers IS NULL
        OR fa.transfers = ''                      THEN 'ilimitadas'
      WHEN fa.transfers = '0'                     THEN 'sem_transferencia'
      WHEN fa.transfers = '1'                     THEN 'uma_transferencia'
      WHEN fa.transfers = '2'                     THEN 'duas_transferencias'
      ELSE fa.transfers
    END                                           AS transferencias_permitidas,
    fa.transfer_duration                          AS duracao_transferencia_seg,
    ROUND(fa.transfer_duration / 3600.0, 1)      AS duracao_transferencia_horas,
    fr.origin_id,
    fr.destination_id,
    fr.contains_id,
    -- Classificação do operador pela tarifa
    CASE
      WHEN fa.fare_id LIKE 'CPTM%'               THEN 'CPTM'
      WHEN fa.fare_id LIKE 'METRO%'              THEN 'Metro'
      ELSE                                            'SPTrans'
    END                                           AS operador
  FROM `sptrans_catalog`.`gtfs_data`.`silver_fare_rules` fr
  JOIN `sptrans_catalog`.`gtfs_data`.`silver_fare_attributes` fa ON fr.fare_id  = fa.fare_id
  JOIN `sptrans_catalog`.`gtfs_data`.`silver_routes`           r  ON fr.route_id = r.route_id
)
SELECT
  *,
  current_timestamp()                             AS dt_processamento
FROM tarifa_rota