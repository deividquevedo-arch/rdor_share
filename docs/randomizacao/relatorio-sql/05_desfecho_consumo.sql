-- Desfecho financeiro por grupo, por intenção de tratar: consumo na Rede depois da randomização.
-- Chave: CPF (num_cpf_paciente_d). A fonte de consumo tem atraso (~2 semanas).
-- :janela_dias = horizonte de observação (30, 90, 180). Paciente cuja janela ainda não fechou
-- fica fora da taxa (fl_janela_completa = 0), para não comparar observação incompleta.
WITH pac AS (
  SELECT nm_especialidade AS linha, tp_grupo AS grupo,
         lpad(cast(num_cpf AS string), 11, '0') AS cpf,
         min(to_date(dt_execucao_modelo)) AS dt_randomizacao
  FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_log
  WHERE tp_ambiente = 'prd'
  GROUP BY 1, 2, 3
),
lim AS (
  SELECT max(Dt_Consumo) AS dt_max_consumo FROM gold_corporativo_ia.corporativo.vw_mov_fato_consumo
),
cons AS (
  SELECT lpad(regexp_replace(num_cpf_paciente_d, '[^0-9]', ''), 11, '0') AS cpf, Dt_Consumo, Vl_Produto
  FROM gold_corporativo_ia.corporativo.vw_mov_fato_consumo
  WHERE trim(Cd_Destino) <> 'H' AND Dt_Consumo >= :dt_inicio_pre
),
por_pac AS (
  SELECT p.linha, p.grupo, p.cpf,
         CASE WHEN date_add(p.dt_randomizacao, :janela_dias) <= lim.dt_max_consumo THEN 1 ELSE 0 END AS fl_janela_completa,
         max(CASE WHEN c.Dt_Consumo > p.dt_randomizacao
                   AND c.Dt_Consumo <= date_add(p.dt_randomizacao, :janela_dias) THEN 1 ELSE 0 END) AS converteu,
         coalesce(sum(CASE WHEN c.Dt_Consumo > p.dt_randomizacao
                   AND c.Dt_Consumo <= date_add(p.dt_randomizacao, :janela_dias) THEN c.Vl_Produto END), 0) AS vl_pos,
         coalesce(sum(CASE WHEN c.Dt_Consumo BETWEEN date_sub(p.dt_randomizacao, :janela_dias)
                   AND p.dt_randomizacao THEN c.Vl_Produto END), 0) AS vl_pre
  FROM pac p
  CROSS JOIN lim
  LEFT JOIN cons c ON c.cpf = p.cpf
  GROUP BY 1, 2, 3, 4
)
SELECT linha, grupo,
       count(*)                                                AS pacientes,
       sum(fl_janela_completa)                                 AS pacientes_janela_completa,
       avg(CASE WHEN fl_janela_completa = 1 THEN converteu END) AS taxa_conversao,
       avg(CASE WHEN fl_janela_completa = 1 THEN vl_pos END)    AS valor_medio_pos,
       avg(vl_pre)                                             AS valor_medio_pre
FROM por_pac
GROUP BY 1, 2
ORDER BY 1, 2;
