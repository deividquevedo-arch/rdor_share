-- DEMONSTRAÇÃO — dados SIMULADOS (não são resultado real). Base de PACIENTES, sem gravar tabela.
-- v4: mesma mecânica de efeito da v2 (retorno e valor maiores na navegação) e campos novos:
-- data do sorteio ao longo de 12 meses, idade, sexo, consumo nos 12 meses ANTES do sorteio (igual entre
-- grupos, porque o sorteio é ao acaso), casamento do CPF com a base de consumo (~85%) e partição do valor
-- em corp/onco e produto da linha. Data de corte simulada: 14/10/2027; atraso da base de consumo: 14 dias.
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
         rand(101) AS u1, randn(202) AS z1, rand(303) AS u2,
         rand(404) AS u_data, rand(505) AS u_cpf, rand(606) AS u_idade, rand(707) AS u_sexo,
         randn(808) AS z_pre, rand(909) AS u_pre
  FROM ids
)
SELECT id, linha, grupo,
       date_add(DATE'2026-10-14', cast(floor(u_data * 365) AS INT)) AS dt_sorteio,
       DATE'2027-10-14' AS dt_corte,
       cast(CASE linha WHEN 'Reumatologia' THEN 30 + u_idade * 45 ELSE 25 + u_idade * 55 END AS INT) AS idade,
       CASE WHEN u_sexo < CASE linha WHEN 'Reumatologia' THEN 0.70 ELSE 0.78 END THEN 1 ELSE 0 END AS feminino,
       CASE WHEN u_pre < 0.55 THEN round(mediana * 0.6 * exp(z_pre), 2) ELSE 0.0 END AS consumo_pre,
       CASE WHEN u_cpf < 0.855 THEN 1 ELSE 0 END AS casou_cpf,
       -ln(1 - u1) / (taxa * CASE WHEN grupo = 'Navegação' THEN ef_taxa ELSE 1 END) AS t,
       mediana * CASE WHEN grupo = 'Navegação' THEN ef_valor ELSE 1 END * exp(z1) AS base,
       0.35 + 0.65 * u2 AS ritmo,
       CASE linha WHEN 'Reumatologia' THEN 0.04 ELSE 0.18 END AS sh_onco,
       CASE WHEN grupo = 'Navegação' THEN 0.40 ELSE 0.30 END AS sh_produto
FROM par
