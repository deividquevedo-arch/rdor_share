-- Proporção de controle acumulada por linha e no total, contra a fração da base vigente.
-- O dia isolado tem poucos pacientes e oscila muito; a checagem é sobre o acumulado.
-- z = desvio em erros-padrão da binomial; ALERTA quando |z| > 3.
WITH base AS (
  SELECT avg(CASE WHEN tp_grupo = 'controle' THEN 1.0 ELSE 0.0 END) AS p
  FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_controle
),
pac AS (  -- um registro por paciente e linha
  SELECT nm_especialidade AS linha, num_cpf, max(tp_grupo) AS grupo
  FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_log
  WHERE tp_ambiente = 'prd'
  GROUP BY 1, 2
),
agg AS (
  SELECT linha, count(*) AS n, sum(CASE WHEN grupo = 'controle' THEN 1 ELSE 0 END) AS k FROM pac GROUP BY linha
  UNION ALL
  SELECT 'TOTAL', count(*), sum(CASE WHEN grupo = 'controle' THEN 1 ELSE 0 END) FROM pac
)
SELECT a.linha, a.n AS pacientes, a.k AS pacientes_controle,
       round(a.k / a.n, 4)                                        AS fracao_observada,
       round(b.p, 4)                                              AS fracao_base,
       round((a.k - a.n * b.p) / sqrt(a.n * b.p * (1 - b.p)), 2)  AS z,
       CASE WHEN abs((a.k - a.n * b.p) / sqrt(a.n * b.p * (1 - b.p))) > 3 THEN 'ALERTA' ELSE 'OK' END AS status
FROM agg a CROSS JOIN base b
ORDER BY a.linha;
