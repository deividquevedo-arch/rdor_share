-- Exames e pacientes por grupo, por linha (só linhas randomizadas).
SELECT
  nm_especialidade                              AS linha,
  tp_grupo                                      AS grupo,
  count(*)                                      AS exames,
  count(DISTINCT num_cpf)                       AS pacientes,
  sum(CASE WHEN fl_enviado THEN 1 ELSE 0 END)   AS exames_enviados,
  min(dt_execucao_modelo)                       AS primeiro_dia,
  max(dt_execucao_modelo)                       AS ultimo_dia
FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_log
WHERE tp_ambiente = 'prd'
  AND dt_execucao_modelo BETWEEN :dt_inicio AND :dt_fim
GROUP BY 1, 2
ORDER BY 1, 2;
