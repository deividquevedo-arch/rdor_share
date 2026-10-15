-- Pacientes randomizados que também foram relevantes em OUTRA linha (não randomizada) no período.
-- No grupo controle, isso é contaminação: o paciente pode ser navegado pela outra linha.
WITH g AS (
  SELECT DISTINCT nm_especialidade AS linha, tp_grupo AS grupo, cast(id_paciente AS string) AS idp
  FROM mlops_fabrica_ia.operacao.tb_mov_randomizacao_log
  WHERE tp_ambiente = 'prd' AND dt_execucao_modelo BETWEEN :dt_inicio AND :dt_fim
),
rel AS (
  SELECT 'ateromatose_coronariana' AS linha, cast(id_paciente AS string) AS idp, dt_execucao_modelo FROM diamond_fabrica_ia.ateromatose_coronariana.tb_mod_diamond_ateromatose_coronariana_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'cancer_colon', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.cancer_colon.tb_mod_diamond_cancer_colon_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'cancer_estomago', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.cancer_estomago.tb_mod_diamond_cancer_estomago_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'cancer_rim', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.cancer_rim.tb_mod_diamond_cancer_rim_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'doenca_inflamatoria_intestinal', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.doenca_inflamatoria_intestinal.tb_mod_diamond_doenca_inflamatoria_intestinal_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'doencas_biliares', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.doencas_biliares.tb_mod_diamond_doencas_biliares_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'hepatologia', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.hepatologia.tb_mod_diamond_hepatologia_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'neuroimunologia', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.neuroimunologia.tb_mod_diamond_neuroimunologia_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'pirads', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.pirads.tb_mod_diamond_pirads_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'transplante_pulmao', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.transplante_pulmao.tb_mod_diamond_transplante_pulmao_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'tumor_osseo', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.tumor_osseo.tb_mod_diamond_tumor_osseo_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'reumatologia', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.reumatologia.tb_mod_diamond_reumatologia_saida_v0 WHERE fl_relevante = 1
  UNION ALL SELECT 'tirads', cast(id_paciente AS string), dt_execucao_modelo FROM diamond_fabrica_ia.tirads.tb_mod_diamond_tirads_saida_v0 WHERE fl_relevante = 1
)
SELECT g.linha, g.grupo,
       count(DISTINCT g.idp)                                       AS pacientes_randomizados,
       count(DISTINCT CASE WHEN r.linha <> g.linha THEN g.idp END) AS relevantes_em_outra_linha
FROM g
LEFT JOIN rel r ON g.idp = r.idp AND to_date(r.dt_execucao_modelo) >= :dt_inicio
GROUP BY 1, 2
ORDER BY 1, 2;
