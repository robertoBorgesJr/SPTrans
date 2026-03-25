

WITH classificacao_servico AS (
  SELECT
    c.service_id,
    -- Inferência do tipo de serviço a partir dos dias ativos
    CASE
      WHEN c.monday = 1
       AND c.saturday = 0
       AND c.sunday = 0                           THEN 'dia_util'
      WHEN c.saturday = 1
       AND c.monday = 0
       AND c.sunday = 0                           THEN 'sabado'
      WHEN c.sunday = 1
       AND c.monday = 0
       AND c.saturday = 0                         THEN 'domingo'
      WHEN c.monday = 1
       AND c.saturday = 1
       AND c.sunday = 1                           THEN 'todos_os_dias'
      ELSE                                            'servico_especial'
    END                                           AS tipo_servico,
    c.start_date,
    c.end_date
  FROM `sptrans_catalog`.`gtfs_data`.`silver_calendar`  c
),
viagens_agg AS (
  SELECT
    t.service_id,
    cs.tipo_servico,
    cs.start_date,
    cs.end_date,
    t.route_id,
    r.route_short_name                            AS numero_linha,
    r.route_long_name                             AS nome_linha,
    t.direction_id,
    CASE t.direction_id
      WHEN 0  THEN 'ida'
      WHEN 1  THEN 'volta'
      ELSE        'circular'
    END                                           AS sentido,
    COUNT(t.trip_id)                              AS total_viagens,
    COUNT(DISTINCT t.shape_id)                    AS total_shapes_distintos
  FROM `sptrans_catalog`.`gtfs_data`.`silver_trips` t
  JOIN `sptrans_catalog`.`gtfs_data`.`silver_routes` r  ON t.route_id   = r.route_id
  JOIN classificacao_servico        cs ON t.service_id = cs.service_id
  GROUP BY
    t.service_id, cs.tipo_servico, cs.start_date, cs.end_date,
    t.route_id, r.route_short_name, r.route_long_name,
    t.direction_id
)
SELECT
  service_id,
  tipo_servico,
  start_date,
  end_date,
  route_id,
  numero_linha,
  nome_linha,
  direction_id,
  sentido,
  total_viagens,
  total_shapes_distintos,
  -- Total bidirecional da linha naquele serviço (window function)
  SUM(total_viagens) OVER (
    PARTITION BY service_id, route_id
  )                                               AS total_viagens_linha_servico,
  current_timestamp()                             AS dt_processamento
FROM viagens_agg