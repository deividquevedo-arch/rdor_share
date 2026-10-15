-- Funil diário por linha de cuidado (exames). Fonte: relatório que cada job grava ao final.
-- Linhas sem randomização trazem qt_controle/qt_intervencao nulos.
SELECT
  nm_especialidade                AS linha,
  dt_execucao_modelo              AS dia,
  qt_entrada                      AS exames_entrada,
  qt_saida                        AS exames_processados,
  qt_relevantes                   AS exames_relevantes,
  qt_exportados                   AS exames_elegiveis_envio,
  qt_controle                     AS exames_controle,
  qt_intervencao                  AS exames_navegacao,
  qt_sem_chave                    AS exames_sem_cpf,
  vl_fracao_controle              AS fracao_controle_observada,
  st_execucao                     AS status,
  coalesce(ds_etapas_falha, '')   AS etapas_com_falha
FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_relatorio
WHERE tp_ambiente = 'prd'
  AND dt_execucao_modelo BETWEEN :dt_inicio AND :dt_fim
ORDER BY linha, dia;
