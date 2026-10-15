-- Primeira linha de atenção: status diário da randomização por linha ativa.
-- Uma linha por (linha de cuidado, dia), desde o primeiro dia randomizado em prd até hoje.
-- status = ALERTA quando qualquer checagem obrigatória falha; PENDENTE = hoje, job ainda sem registro.
WITH log AS (
  SELECT * FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'
),
ativas AS (
  SELECT nm_especialidade AS linha, min(to_date(dt_execucao_modelo)) AS ini FROM log GROUP BY 1
),
dias AS (  -- agenda dos jobs: segunda a sexta, 04:00 (America/Sao_Paulo)
  SELECT linha, dia FROM (
    SELECT linha, explode(sequence(ini, current_date())) AS dia FROM ativas
  ) WHERE dayofweek(dia) BETWEEN 2 AND 6
),
rel AS (  -- última execução registrada do dia
  SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia,
         count(*)                                         AS execucoes,
         max_by(st_execucao, dh_registro)                 AS st,
         max_by(coalesce(ds_etapas_falha, ''), dh_registro) AS falhas,
         max_by(qt_exportados, dh_registro)               AS exportados,
         max_by(qt_controle, dh_registro)                 AS controle,
         max_by(qt_intervencao, dh_registro)              AS navegacao,
         max_by(qt_sem_chave, dh_registro)                AS sem_cpf
  FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_relatorio
  WHERE tp_ambiente = 'prd'
  GROUP BY 1, 2
),
lg AS (
  SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia,
         count(*)                                                        AS registros,
         sum(CASE WHEN tp_grupo = 'controle' THEN 1 ELSE 0 END)           AS controle_log,
         sum(CASE WHEN tp_grupo = 'controle' AND fl_enviado THEN 1 ELSE 0 END) AS controle_enviado
  FROM log GROUP BY 1, 2
)
SELECT
  d.linha, d.dia,
  r.execucoes, r.st, r.exportados, r.controle, r.navegacao, r.sem_cpf,
  l.registros, l.controle_enviado,
  CASE WHEN r.linha IS NULL THEN 1 ELSE 0 END                                  AS chk_sem_execucao,
  CASE WHEN r.st <> 'SUCESSO' OR r.falhas <> '' THEN 1 ELSE 0 END              AS chk_falha,
  CASE WHEN r.exportados <> coalesce(r.controle,0) + coalesce(r.navegacao,0) + coalesce(r.sem_cpf,0)
       THEN 1 ELSE 0 END                                                       AS chk_conservacao,
  CASE WHEN coalesce(l.registros,0) <> coalesce(r.controle,0) + coalesce(r.navegacao,0)
       THEN 1 ELSE 0 END                                                       AS chk_log_x_relatorio,
  CASE WHEN coalesce(l.controle_enviado,0) > 0 THEN 1 ELSE 0 END               AS chk_controle_enviado,
  CASE WHEN coalesce(r.sem_cpf,0) > 0 THEN 1 ELSE 0 END                        AS info_sem_cpf,
  CASE
    WHEN r.linha IS NULL AND d.dia = current_date() THEN 'PENDENTE'
    WHEN r.linha IS NULL
      OR r.st <> 'SUCESSO' OR r.falhas <> ''
      OR r.exportados <> coalesce(r.controle,0) + coalesce(r.navegacao,0) + coalesce(r.sem_cpf,0)
      OR coalesce(l.registros,0) <> coalesce(r.controle,0) + coalesce(r.navegacao,0)
      OR coalesce(l.controle_enviado,0) > 0 THEN 'ALERTA'
    ELSE 'OK'
  END AS status
FROM dias d
LEFT JOIN rel r ON r.linha = d.linha AND r.dia = d.dia
LEFT JOIN lg  l ON l.linha = d.linha AND l.dia = d.dia
ORDER BY d.linha, d.dia;
