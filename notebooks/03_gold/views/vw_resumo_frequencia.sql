-- =============================================================================
--  Para consumo no BI / notebooks
--    frequência média geral por linha (todos os períodos)  
-- =============================================================================
CREATE OR REPLACE VIEW sptrans_catalog.gtfs_data.vw_resumo_frequencia AS
SELECT
  numero_linha,
  nome_linha,
  ROUND(AVG(headway_medio_min), 1)              AS headway_geral_medio_min,
  SUM(total_viagens)                            AS total_viagens_dia,
  COUNT(DISTINCT periodo_dia)                   AS periodos_operacao
FROM sptrans_catalog.gtfs_data.gold_frequencia_por_linha
GROUP BY numero_linha, nome_linha;



