-- =============================================================================
-- gold_frequencia_por_linha
--    Headway (intervalo entre ônibus) médio, mínimo e máximo por rota,
--    segmentado por período do dia (madrugada, manhã, tarde, noite).
-- =============================================================================

CREATE OR REPLACE TABLE sptrans_catalog.gtfs_data.gold_frequencia_por_linha
USING DELTA
COMMENT 'Frequência operacional (headway) por linha e período do dia'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
AS
WITH freq_enriquecida AS (
  SELECT
    t.route_id,
    r.route_short_name                          AS numero_linha,
    r.route_long_name                           AS nome_linha,
    r.route_type,
    f.trip_id,
    f.start_time,
    f.end_time,
    f.headway_secs,
    ROUND(f.headway_secs / 60.0, 1)            AS headway_min,
    -- Classificação por período do dia
    CASE
      WHEN f.start_time < '05:00:00'            THEN 'madrugada'
      WHEN f.start_time < '09:00:00'            THEN 'pico_manha'
      WHEN f.start_time < '12:00:00'            THEN 'manha'
      WHEN f.start_time < '15:00:00'            THEN 'tarde'
      WHEN f.start_time < '19:00:00'            THEN 'pico_tarde'
      WHEN f.start_time < '22:00:00'            THEN 'noite'
      ELSE                                           'noite_tardia'
    END                                         AS periodo_dia
  FROM sptrans_catalog.gtfs_data.silver_frequencies f
  JOIN sptrans_catalog.gtfs_data.silver_trips       t ON f.trip_id   = t.trip_id
  JOIN sptrans_catalog.gtfs_data.silver_routes      r ON t.route_id  = r.route_id
),
agregado AS (
  SELECT
    route_id,
    numero_linha,
    nome_linha,
    route_type,
    periodo_dia,
    COUNT(DISTINCT trip_id)                     AS total_viagens,
    ROUND(AVG(headway_min), 1)                  AS headway_medio_min,
    ROUND(MIN(headway_min), 1)                  AS headway_minimo_min,
    ROUND(MAX(headway_min), 1)                  AS headway_maximo_min,
    -- Classificação qualitativa da frequência
    CASE
      WHEN AVG(headway_min) <= 5               THEN 'alta_frequencia'
      WHEN AVG(headway_min) <= 15              THEN 'frequencia_regular'
      WHEN AVG(headway_min) <= 30              THEN 'baixa_frequencia'
      ELSE                                          'muito_baixa_frequencia'
    END                                         AS classificacao_frequencia
  FROM freq_enriquecida
  GROUP BY
    route_id, numero_linha, nome_linha, route_type, periodo_dia
)
SELECT
  *,
  current_timestamp() AS dt_processamento
FROM agregado
ORDER BY numero_linha, periodo_dia;