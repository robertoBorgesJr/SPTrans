-- =============================================================================
--  Para consumo no BI / notebooks
--    Top 20 paradas (hubs principais para análise de mobilidade)  
-- =============================================================================

CREATE OR REPLACE VIEW sptrans_catalog_gtfs_data.vw_top20_paradas AS
SELECT *
FROM sptrans_catalog.gtfs_data.gold_paradas_mais_atendidas
WHERE ranking_atendimentos <= 20;