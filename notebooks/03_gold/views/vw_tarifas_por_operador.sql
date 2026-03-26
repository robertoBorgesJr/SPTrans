-- =============================================================================
--  Para consumo no BI / notebooks
--    Comparativo tarifário operadores  
-- =============================================================================

CREATE OR REPLACE VIEW sptrans_catalog.gtfs_data.vw_tarifas_por_operador AS
SELECT
  operador,
  COUNT(DISTINCT route_id)                      AS total_rotas,
  ROUND(AVG(preco_brl), 2)                      AS preco_medio_brl,
  MIN(preco_brl)                                AS preco_minimo_brl,
  MAX(preco_brl)                                AS preco_maximo_brl,
  MAX(duracao_transferencia_horas)              AS janela_transferencia_horas
FROM sptrans_catalog.gtfs_data.gold_tarifa_por_rota
GROUP BY operador;
