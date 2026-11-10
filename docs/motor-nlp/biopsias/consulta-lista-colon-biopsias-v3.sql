-- v3 (10/11/2026): ajustes da homologação (vereditos de 09/11): órgão aceita 'intestino grosso' e só exclui apêndice
--   quando ele é o único órgão citado; peça cirúrgica: 'ressecção de pólipo' e LST/dissecção submucosa = endoscópico,
--   'margem do grampeamento' = cirurgia, e material ambíguo decidido pela macroscopia ('segmento' antes de 'fragmentos': peça cirúrgica também cita fragmentos).
--   Regra de envio (definida pela navegação): enviar quando provavel_peca_cirurgica <> 'Sim'.
-- v2 (04/11/2026): + provavel_peca_cirurgica, pelo TEXTO do material (o modelo 'peça' do Motion inclui polipectomia).
-- =====================================================================================
-- Lista de pacientes relevantes — Ca de Cólon (Biópsias / anatomopatológico, Motion LIS)
-- v1 · 04/11/2026 · para homologação. Ref.: docs/motor-nlp/biopsias/biopsias-definicao-envio-v2.md
-- Card 393825 — [Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados do Motion LIS disponíveis no RC
--
-- Regra
--   relevante = campo `codificação da doença` = 'neoplasia maligna primária' (categoria do laboratório)
--   órgão     = texto dos campos de material ('material biópsia','material peça','material ihq','material ihq - complementar','material revisão')
--               cólon: cólon/sigmoide/ceco/cecal (fronteira explícita) ou cólon + segmento/flexura/ângulo
--               reto : reto/retal/retossigmoide (fronteira explícita); apêndice EXCLUÍDO
--   recorte   = colon_biopsia · reto_biopsia · colon_peca · reto_peca, pelo nome do campo de material
--               ('material biópsia' → biópsia; só 'material peça' → peça). As três decisões abertas
--               (reto, peça cirúrgica, paciente já navegado) ficam para a homologação.
--   unidades  = interseção da lista `e.id_unidade` das configs de navegação de prd com o filtro de unidades
--               da view de exportação das linhas (o que de fato chega ao negócio hoje).
-- Fontes
--   especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame  resultado do AP, 1 linha por campo
--   gold_corporativo_ia.corporativo.tb_gold_mov_exame     unidade, convênio, solicitante (num_pedido_integracao)
--   gold_corporativo_ia.corporativo.tb_gold_mov_paciente  cadastro, decriptado como na view de exportação
-- Janela
--   AP emitido (`issued`) nos últimos 30 dias até a data da execução.
-- Colunas = contrato do arquivo de navegação (reumatologia), com:
--   findings = categoria do AP · findings_match = conclusão do AP · laudo = laudo do AP montado dos campos
--   (material → macroscopia → microscopia → resultado IHQ → conclusão); extras para homologação:
--   recorte, material_ap, provavel_peca_cirurgica, pedido_motion, dt_emissao_ap, id_hospital.
-- ⚠️ A saída contém dado de paciente: gravar SOMENTE fora de repositório git.
-- =====================================================================================
WITH campos AS (
  SELECT numero_pedido_integracao AS pedido, lower(trim(component_code_text)) AS campo, component_value AS valor,
         to_timestamp(issued) AS emissao, descricao_procedimento_integracao AS proc_motion
  FROM especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame
  WHERE to_date(issued) >= date_sub(current_date(), 30)
),
ped AS (
  SELECT pedido,
         min(emissao) AS dt_emissao_ap,
         max(proc_motion) AS proc_motion,
         max(CASE WHEN campo = 'codificação da doença' THEN lower(valor) END) AS categoria,
         max(CASE WHEN campo = 'material biópsia' THEN 1 ELSE 0 END) AS tem_biop,
         max(CASE WHEN campo = 'material peça' THEN 1 ELSE 0 END) AS tem_peca,
         concat_ws(' | ', collect_set(CASE WHEN campo IN ('material biópsia','material peça','material ihq','material ihq - complementar','material revisão') THEN valor END)) AS material_ap,
         concat_ws(' | ', collect_list(CASE WHEN campo LIKE 'conclus%' THEN valor END)) AS conclusao_ap,
         lower(concat_ws(' | ', collect_list(CASE WHEN campo LIKE 'macroscopia%' THEN valor END))) AS macroscopia_ap,
         concat_ws(char(10), transform(array_sort(collect_list(
             CASE WHEN campo LIKE 'material%' OR campo LIKE 'macroscopia%' OR campo LIKE 'microscopia%'
                       OR campo LIKE 'conclus%' OR campo LIKE 'resultado ihq%'
                  THEN named_struct('o', CASE WHEN campo LIKE 'material%' THEN 1 WHEN campo LIKE 'macroscopia%' THEN 2
                                              WHEN campo LIKE 'microscopia%' THEN 3 WHEN campo LIKE 'resultado ihq%' THEN 4 ELSE 5 END,
                                    't', concat(upper(campo), ': ', valor)) END)), x -> x.t)) AS laudo_ap
  FROM campos GROUP BY pedido
),
cls AS (
  SELECT *,
    -- apêndice só exclui quando é o único órgão: tira as menções a apêndice ('apêndice cecal') antes de procurar cólon/reto
    CASE WHEN mat_sem_apendice RLIKE '(^|[^a-zà-ÿ])(c[oóô]lon|sigm[oó]i|ceco|cecal|c[eé]co)|intestino grosso'
           OR mat_sem_apendice RLIKE '(c[oóô]lon|cólico).{0,30}(transverso|ascendente|descendente|direito|esquerdo)|flexura (hep|espl)|[aâ]ngulo (hep|espl)' THEN 'colon'
         WHEN mat_sem_apendice RLIKE '(^|[^a-zà-ÿ])(reto|retal|retossigm)' THEN 'reto'
         WHEN lower(material_ap) RLIKE 'ap[eê]ndic' THEN 'apendice'
         ELSE 'outro' END AS orgao
  FROM (SELECT *, regexp_replace(lower(material_ap), 'ap[eê]ndic[a-zç]*( cecal)?', ' ') AS mat_sem_apendice FROM ped)
  WHERE categoria = 'neoplasia maligna primária'
),
alvo AS (
  SELECT *, concat(orgao, '_', CASE WHEN tem_biop = 1 THEN 'biopsia' WHEN tem_peca = 1 THEN 'peca' ELSE 'outro' END) AS recorte
  FROM cls WHERE orgao IN ('colon', 'reto')
),
ex AS (
  SELECT e.*, row_number() OVER (PARTITION BY e.num_pedido_integracao
            ORDER BY CASE WHEN lower(e.proced_descricao) RLIKE 'anatomo|bi[oó]ps|histopat' THEN 0 ELSE 1 END,
                     e.etl_data_atualizacao_carga_gold DESC, e.id_exame DESC) AS rn
  FROM gold_corporativo_ia.corporativo.tb_gold_mov_exame e
  WHERE e.dt_dia_exame >= date_sub(current_date(), 120)
    AND e.num_pedido_integracao IN (SELECT pedido FROM alvo)
)
SELECT
  current_date()                                                     AS dt_execucao_modelo,
  a.pedido                                                           AS id_inferencia,
  e.id_exame                                                         AS exame_an,
  security.prd.rdsl_decrypt(p.cli_nome, 1)                           AS nome_paciente,
  security.prd.rdsl_decrypt(p.doc_cpf, 1)                            AS cpf_paciente,
  CAST(p.cli_idade AS INT)                                           AS idade_paciente,
  p.cli_genero                                                       AS sexo_paciente,
  security.prd.rdsl_decrypt(p.cli_telefone_numero, 0)                AS telefone_paciente,
  e.nme_hospital                                                     AS nome_hospital,
  e.nme_regional_hospital                                            AS uf_regional,
  e.nme_convenio                                                     AS convenio,
  e.nme_plano                                                        AS plano,
  security.prd.rdsl_decrypt(e.nme_local_profissional_solicitante, 1) AS medico_solicitante,
  security.prd.rdsl_decrypt(e.reg_crm_solicitante, 1)                AS crm_solicitante,
  e.reg_uf_crm_solicitante                                           AS uf_crm_solicitante,
  CAST(COALESCE(e.dt_liberacao_laudo, a.dt_emissao_ap) AS DATE)      AS laudo_exame_data_liberacao,
  e.nme_origem_exame                                                 AS origem_exame,
  'PAT'                                                              AS modalidade,
  COALESCE(e.proced_descricao, a.proc_motion)                        AS tipo_exame,
  a.laudo_ap                                                         AS laudo_exame_original,
  a.categoria                                                        AS findings,
  a.conclusao_ap                                                     AS findings_match,
  a.recorte                                                          AS recorte,
  a.material_ap                                                      AS material_ap,
  CASE WHEN lower(a.material_ap) RLIKE '(?<!polip)(?<!mucos)ectomia|amputa|pe[çc]a cir[uú]rg|ressec[a-zçãõ]* (?!endosc|de p[oó]lip)|cirurgi|linfonod|omento|an[eé]is|anastomose|grampeamento' THEN 'Sim'
       WHEN lower(a.material_ap) RLIKE 'polipectomia|mucosectomia|p[oó]lipo|bi[oó]psia|endosc|colonosc|fragmento|(^|[^a-z])lst([^a-z]|$)|dissec[cç][aã]o|submucosa' THEN 'Não'
       WHEN a.macroscopia_ap RLIKE 'segmento|al[cç]a intestinal|pe[çc]a cir[uú]rg' THEN 'Sim'
       WHEN a.macroscopia_ap RLIKE 'fragmento' THEN 'Não'
       ELSE 'Indefinido' END                                        AS provavel_peca_cirurgica,
  a.pedido                                                           AS pedido_motion,
  CAST(a.dt_emissao_ap AS DATE)                                      AS dt_emissao_ap,
  e.id_unidade                                                       AS id_hospital
FROM alvo a
JOIN ex e ON e.num_pedido_integracao = a.pedido AND e.rn = 1
LEFT JOIN gold_corporativo_ia.corporativo.tb_gold_mov_paciente p ON p.id_paciente = e.id_patient
WHERE e.id_unidade IN ('135', '200146', '101', '200139', '134', '200021', '5', '100999', '16', '97', '200020', '1353', '86', '62', '200129', '67', '100968', '373', '200128', '42', '101043', '141', '101027', '257', '200023', '873', '874', '11', '200001', '100', '100965', '81', '200002', '54', '200130', '103', '100970', '39', '147', '133', '200019', '228', '200135', '95', '101031', '91', '101001', '41', '1073', '125', '17', '100946', '35', '94', '30', '2', '1661', '200000', '953', '107', '100990', '1662', '90', '92', '55', '136', '47', '200017', '83', '65', '77', '18', '50', '1296', '132', '100951', '31', '131', '200015', '146', '1293', '75', '130', '118', '122', '37', '1393', '313', '121', '52')
ORDER BY recorte, nome_hospital, dt_emissao_ap
