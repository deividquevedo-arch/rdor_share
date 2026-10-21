-- DEMONSTRAÇÃO — dados SIMULADOS (não são resultado real). Base de PACIENTES, sem gravar tabela.
-- v5: CENÁRIO PORTFÓLIO Q4 — consumo por paciente +15% na navegação (apresentação do projeto v2, "a confirmar"),
-- dividido METADE no retorno e METADE no ticket: x1,0724 em cada (1,0724² ≈ 1,15).
-- Calibrado com o medido na base real (horizonte-uplift-por-linha-v0.md): retorno do controle ~58% em 30 dias,
-- consumo com cauda longa (lognormal sigma 1,43 -> CV por paciente ~3,5 em 30 dias).
-- Retorno: modelo de cura — parte dos pacientes nunca volta; quem volta, volta cedo (taxa ~0,05/dia).
-- Mesmos campos da v4: data do sorteio em 12 meses, idade, sexo, consumo nos 12 meses ANTES (igual entre
-- grupos), casamento do CPF (~85%), corp/onco e produto da linha. Corte simulado em 14/10/2027.
-- SEMENTES 113/204 escolhidas entre 10 pares testados em 20/10/2026 por darem um sorteio TÍPICO (observado em 90 dias:
-- +5,1 p.p. no retorno, +12,4% no consumo por paciente, +5,1% no ticket; verdadeiro: +5,4 p.p., +15%, +7,2%).
-- Nos 10 pares o consumo por paciente variou de +7% a +35% e o retorno de +3,9 a +9,3 p.p.: o ruído do R$ é a mensagem.
WITH ids AS (
  SELECT id,
         CASE WHEN id < 2780 THEN 'Reumatologia' ELSE 'Tireoide' END AS linha,
         CASE WHEN (id < 2780 AND id < 280) OR (id >= 2780 AND id - 2780 < 1020) THEN 'Controle' ELSE 'Navegação' END AS grupo
  FROM range(0, 13000, 1, 1)
),
par AS (
  SELECT ids.*,
         CASE linha WHEN 'Reumatologia' THEN 0.70 ELSE 0.76 END AS pi_ctrl,
         CASE linha WHEN 'Reumatologia' THEN 0.045 ELSE 0.0495 END AS taxa,
         CASE linha WHEN 'Reumatologia' THEN 1500.0 ELSE 1150.0 END AS mediana,
         1.0724 AS efeito,
         rand(113) AS u_volta, rand(101) AS u1, randn(204) AS z1, rand(303) AS u2,
         rand(404) AS u_data, rand(505) AS u_cpf, rand(606) AS u_idade, rand(707) AS u_sexo,
         randn(808) AS z_pre, rand(909) AS u_pre
  FROM ids
)
SELECT id, linha, grupo,
       date_add(DATE'2026-10-14', cast(floor(u_data * 365) AS INT)) AS dt_sorteio,
       DATE'2027-10-14' AS dt_corte,
       cast(CASE linha WHEN 'Reumatologia' THEN 30 + u_idade * 45 ELSE 25 + u_idade * 55 END AS INT) AS idade,
       CASE WHEN u_sexo < CASE linha WHEN 'Reumatologia' THEN 0.70 ELSE 0.78 END THEN 1 ELSE 0 END AS feminino,
       CASE WHEN u_pre < 0.60 THEN round(mediana * 0.8 * exp(1.43 * z_pre), 2) ELSE 0.0 END AS consumo_pre,
       CASE WHEN u_cpf < 0.855 THEN 1 ELSE 0 END AS casou_cpf,
       CASE WHEN u_volta < pi_ctrl * CASE WHEN grupo = 'Navegação' THEN efeito ELSE 1 END
            THEN -ln(1 - u1) / taxa ELSE 1e9 END AS t,
       mediana * CASE WHEN grupo = 'Navegação' THEN efeito ELSE 1 END * exp(1.43 * z1) AS base,
       0.35 + 0.65 * u2 AS ritmo,
       CASE linha WHEN 'Reumatologia' THEN 0.04 ELSE 0.18 END AS sh_onco,
       0.35 AS sh_produto
FROM par
