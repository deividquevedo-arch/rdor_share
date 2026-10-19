-- DEMONSTRAÇÃO — dados SIMULADOS (não são resultado real). Base de pacientes, sem gravar tabela.
-- v2 (efeito visível para demonstração). Premissas: ~12 meses da onda 1 a 10% de controle; controle converte e consome menos.
WITH ids AS (
  SELECT id,
         CASE WHEN id < 2780 THEN 'Reumatologia' ELSE 'Tireoide' END AS linha,
         CASE WHEN (id < 2780 AND id < 280) OR (id >= 2780 AND id - 2780 < 1020) THEN 'Controle' ELSE 'Navegação' END AS grupo
  FROM range(0, 13000, 1, 1)
),
par AS (
  SELECT ids.*,
         CASE linha WHEN 'Reumatologia' THEN 0.0045 ELSE 0.0060 END AS taxa,
         CASE linha WHEN 'Reumatologia' THEN 1100.0 ELSE 780.0 END AS mediana,
         CASE linha WHEN 'Reumatologia' THEN 1.40 ELSE 1.35 END AS ef_taxa,
         CASE linha WHEN 'Reumatologia' THEN 1.15 ELSE 1.12 END AS ef_valor,
         rand(101) AS u1, randn(202) AS z1, rand(303) AS u2
  FROM ids
),
pac AS (
  SELECT id, linha, grupo,
         -ln(1 - u1) / (taxa * CASE WHEN grupo = 'Navegação' THEN ef_taxa ELSE 1 END) AS t,
         mediana * CASE WHEN grupo = 'Navegação' THEN ef_valor ELSE 1 END * exp(z1) AS base,
         0.35 + 0.65 * u2 AS ritmo
  FROM par
)
SELECT id, linha, grupo, j.janela,
       CASE WHEN t > j.janela THEN 0.0
            ELSE base * least(1.0, (j.janela - t) / 90.0 * ritmo + 0.35) END AS valor,
       t AS dias_ate_1o_consumo
FROM pac CROSS JOIN (SELECT explode(array(30, 90, 180)) AS janela) j
