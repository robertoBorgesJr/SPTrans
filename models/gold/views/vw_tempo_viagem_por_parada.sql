-- models/gold/vw_tempo_viagem_por_parada.sql

{{ config(materialized='view') }}

WITH stop_times_silver AS (
    SELECT * FROM {{ source('sptrans_raw', 'silver_stop_times') }}
),

stops_silver AS (
    SELECT * FROM {{ source('sptrans_raw', 'silver_stops') }}
),

trips_silver AS (
    SELECT * FROM {{ source('sptrans_raw', 'silver_trips') }}
)

SELECT 
    t.trip_id,
    t.trip_headsign,
    s.stop_name,
    st.stop_sequence,
    st.normalized_arrival_time, -- Aquela que criamos com % 24
    st.arrival_time_sec,
    -- Cálculo de tempo entre paradas (Lead/Lag no SQL)
    st.arrival_time_sec - LAG(st.arrival_time_sec) OVER (PARTITION BY t.trip_id ORDER BY st.stop_sequence) AS seconds_from_last_stop
FROM stop_times_silver st
JOIN stops_silver s ON st.stop_id = s.stop_id
JOIN trips_silver t ON st.trip_id = t.trip_id
ORDER BY t.trip_id, st.stop_sequence