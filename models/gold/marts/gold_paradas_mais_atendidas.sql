{{ config(
    materialized='table',
    file_format='delta',
    description='Ranking de paradas por volume de atendimentos e linhas que passam',
    tblproperties={
      'delta.autoOptimize.optimizeWrite': 'true',
      'delta.autoOptimize.autoCompact': 'true'
    }
) }}

WITH atendimentos AS (
  SELECT
    st.stop_id,
    s.stop_name                                   AS nome_parada,
    s.stop_lat                                    AS latitude,
    s.stop_lon                                    AS longitude,
    COUNT(st.trip_id)                             AS total_atendimentos,
    COUNT(DISTINCT t.route_id)                    AS total_linhas_distintas,
    -- Coleta as primeiras 5 linhas que atendem a parada (para referência)
    COLLECT_SET(r.route_short_name)               AS linhas_que_atendem
  FROM {{ source('sptrans_raw', 'silver_stop_times')}}  st
  JOIN {{ source('sptrans_raw', 'silver_stops')}}  s  ON st.stop_id  = s.stop_id
  JOIN {{ source('sptrans_raw', 'silver_trips')}}  t  ON st.trip_id  = t.trip_id
  JOIN {{ source('sptrans_raw', 'silver_routes')}} r  ON t.route_id  = r.route_id
  GROUP BY
    st.stop_id, s.stop_name, s.stop_lat, s.stop_lon
),
rankeado AS (
  SELECT
    *,
    RANK() OVER (ORDER BY total_atendimentos DESC)  AS ranking_atendimentos,
    RANK() OVER (ORDER BY total_linhas_distintas DESC) AS ranking_diversidade,
    -- Classificação da parada
    CASE
      WHEN total_linhas_distintas >= 10           THEN 'hub_principal'
      WHEN total_linhas_distintas >= 5            THEN 'hub_secundario'
      WHEN total_linhas_distintas >= 2            THEN 'ponto_conexao'
      ELSE                                             'parada_simples'
    END                                           AS tipo_parada
  FROM atendimentos
)
SELECT
  stop_id,
  nome_parada,
  latitude,
  longitude,
  total_atendimentos,
  total_linhas_distintas,
  linhas_que_atendem,
  ranking_atendimentos,
  ranking_diversidade,
  tipo_parada,
  current_timestamp()                             AS dt_processamento
FROM rankeado