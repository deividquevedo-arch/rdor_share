-- Três checagens que devem dar ZERO, com o total como controle positivo.
WITH l AS (
  SELECT * FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'
)
SELECT
  (SELECT count(*) FROM l) AS registros_total,
  (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf
                         HAVING count(DISTINCT tp_grupo) > 1)) AS cpf_com_dois_grupos,
  (SELECT count(*) FROM l WHERE tp_grupo = 'controle' AND fl_enviado) AS controle_enviado,
  (SELECT count(*) FROM l
     LEFT JOIN mlops_fabrica_ia.operacao.tb_mov_randomizacao_controle b
       ON lpad(cast(l.num_cpf AS string), 11, '0') = lpad(cast(b.num_cpf AS string), 11, '0')
     WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) AS divergentes_da_base;
