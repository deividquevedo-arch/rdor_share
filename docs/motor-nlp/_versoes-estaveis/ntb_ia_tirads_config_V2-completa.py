# SNAPSHOT DE BACKUP — V2 COMPLETA — achado lexico + TR4-6, SEM sangue
# Gerado de plataform/config/speciality/ntb_ia_tirads_config.py em 2026-08-18.
# NAO e a config em uso: a viva esta no repo da plataforma. Aqui e a rede de seguranca
# para reativacao. As variantes diferem por DUAS chaves — ver a MATRIZ DE ESCOPO no
# cabecalho do arquivo. Exige nlp_engine >= 0.8.5.
# Databricks notebook source
# MAGIC %md
# MAGIC # Config da especialidade `tirads`
# MAGIC
# MAGIC Define `CONFIG` e o devolve via `dbutils.notebook.exit(json...)`; o runner `ntb_ia_motor_e2e` carrega com `dbutils.notebook.run`.
# MAGIC
# MAGIC **Origem:** versao estavel homologada `0.1.0-tirads-rads-v22.11-v2` (`docs/motor-nlp/_versoes-estaveis/`).
# MAGIC
# MAGIC ⚠️ **As regras clinicas JA foram alteradas desde a origem** — ver SPEC de negocio V3:
# MAGIC - `0.2.0`–`0.4.0`: entram cintilografia e exame de sangue (TSH, T4 livre, TRAb promovem; T3 e Anti-TPO viram FLAG).
# MAGIC - `0.5.0`: **TR4 sai de `promote_categories`** — "TR4 sozinho nao aprova; a partir de 1 cm aprova".
# MAGIC - `0.6.0`: **TR4 VOLTA condicionado** — `gates_ordinal_promotion: ['TR4']` nos criterios de tamanho (exige `nlp_engine >= 0.8.5`); `findings` passa a exibir `TR4 - Nodulo (1.8 cm)`.
# MAGIC - `0.7.0`: **escopo estreitado** — `relevance_mode: ordinal_only` e sangue em `annotate_only`.
# MAGIC
# MAGIC ## ⚙️ MATRIZ DE ESCOPO — como ligar e desligar cada versao
# MAGIC
# MAGIC **Nada foi apagado.** As versoes diferem por DUAS chaves; o resto da config (achados,
# MAGIC prompts, limiares, ancoras) e identico nas tres. Reativar e virar chave, nao restaurar backup.
# MAGIC
# MAGIC | versao | o que entrega | `ordinal_extraction.relevance_mode` | `on_met` de tsh/t4/trab |
# MAGIC |---|---|---|---|
# MAGIC | **V2 completa** | achado lexico (bocio, linfonodomegalia, nodulo/cisto sem categoria) + TR4-6 | `normal_plus_ordinal` | `annotate_only` |
# MAGIC | **V2 estreita** ← ATIVA na 0.7.0 | so TR5 e TR4 com nodulo/cisto >= 1 cm | `ordinal_only` | `annotate_only` |
# MAGIC | **V3 (com sangue)** | o escopo escolhido acima + TSH suprimido / T4 livre elevado / TRAb positivo | (qualquer um) | `promote` |
# MAGIC
# MAGIC ⚠️ **`annotate_only` nao desliga o criterio** — ele SEGUE avaliando e gravando no bloco
# MAGIC `quantitative` do blob. So nao promove. Ou seja: da para **medir** o que a V3 entregaria
# MAGIC antes de decidir entrega-la, sem rodar outra config.
# MAGIC
# MAGIC **Por que a V2 estreita e a ativa:** decisao do negocio em 2026-08-18 — a operacao nao
# MAGIC absorve o volume dos achados lexicos. Medido: 1.389 dos 2.815 relevantes (~49%) vinham dai.
# MAGIC **E decisao de CAPACIDADE, nao clinica** — por isso as outras duas ficam prontas para voltar.
# MAGIC
# MAGIC **Snapshots** das tres em `docs/motor-nlp/_versoes-estaveis/` (repo de backup pessoal).
# MAGIC
# MAGIC **Adaptacoes para esta plataforma** (o runner legado fazia estas duas coisas sozinho):
# MAGIC - `runtime.profile`: `rule_only` -> `llm_http`.
# MAGIC - `runtime.llm_router.enabled`: `False` -> `True` (sem isso o juiz nunca e acionado).
# MAGIC - `column_map` inteiro reescrito: a versao estavel apontava para a CANONICA (`an`, `Laudo`, `dataexame`, `modalidade`, `tipoexame`), colunas que nao existem na Gold. Aqui a fonte e a Gold.
# MAGIC
# MAGIC ⚠️ `catalog` fica por procedencia: nao e lido aqui — o catalogo vem do `EnvironmentConfig` (`diamond_ia_dev`/`_hml`/`diamond_ia`). `data.legacy` foi REMOVIDO: pertence ao runner antigo, cujos configs vivem no repo dele.

# COMMAND ----------

CONFIG = {'specialty_id': 'tirads',
 'config_version': '0.7.0-tirads',
 'model_version': 'v0',
 'description': 'Motor NLP TI-RADS + achados clinicos (linha de cuidado tireoide).',
 'nlp': {'shared_organs_path': '../shared/organs.yaml',
         'target_organs': ['tireoide'],
         'organs': {'tireoide': {'regex': ['\\btireoid\\w*'],
                                 'seeds': ['tireóide',
                                           'glândula tireoide',
                                           'glândula tireóide',
                                           'glandula tireoide',
                                           'glandula tireóide',
                                           'lobo tireoidiano',
                                           'lobo tireóideo',
                                           'lobo tireoideo',
                                           'istmo tireoidiano',
                                           'parênquima tireoidiano',
                                           'parenquima tireoidiano']}},
         'segmentation': {'mode': 'full_doc'},
         'document_vet': {'enabled': True,
                          'normality_phrases': ['sem alterações significativas',
                                                'dentro dos limites da normalidade',
                                                'dentro dos parâmetros da normalidade',
                                                'exame normal',
                                                'ultrassonografia da tireoide normal'],
                          'soft_findings': ['nodulo', 'cisto']},
         'emit_decision_trail': True,
         'score_policy_version': 'v1_bins_legacy',
         'use_spacy_matcher': True,
         'feature_flags': {'rule_engine': True, 'calibrated_hybrid': True},
         'embeddings': {'use_embeddings': True,
                        'decision_mode': 'hybrid',
                        'embedding_backend': 'auto',
                        'embedding_model': '/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/st_models/paraphrase-multilingual-MiniLM-L12-v2',
                        'similarity_threshold': 0.88,
                        'ambiguity_band': [0.3, 0.7],
                        'hybrid_weight_rule': 0.7,
                        'hybrid_weight_semantic': 0.3,
                        'emit_as_finding': False},
         'llm_router': {'mode': 'llm',
                        'provider': 'openai_compatible',
                        'model': 'databricks-claude-haiku-4-5',
                        'fallback_models': ['databricks-claude-sonnet-4-5'],
                        'max_tokens': 256,
                        'api_key_env': 'DATABRICKS_TOKEN',
                        'uncertainty_band': [0.35, 0.65],
                        'fallback_policy': 'keep_current',
                        'max_input_chars': 8000,
                        'json_response_format': False,
                        'prompt_system': 'Voce e um assistente de triagem clinica em tireoide (linha de '
                                         'cuidado TI-RADS). Responda APENAS com um unico objeto JSON, sem '
                                         'texto adicional. Esquema: {"relevante": boolean}.',
                        'specialty_context': 'Tarefa: decidir se o laudo de imagem (US/Doppler de tireoide, '
                                             'US de pescoco, TC de pescoco, cintilografia, PAAF/biopsia) '
                                             'deve ser ENCAMINHADO para captacao na linha de cuidado de '
                                             'tireoide. Responda relevante=true SOMENTE quando houver, na '
                                             'tireoide ou cadeias cervicais, um destes achados REAIS (nao '
                                             'negados): nodulo ou cisto tireoidiano com dimensao >=1cm; '
                                             'TI-RADS 4 ou 5; massa, tumor ou neoplasia; linfonodomegalia ou '
                                             'linfonodo aumentado/suspeito/INDETERMINADO ou com perda de '
                                             'arquitetura hilar; bocio NODULAR '
                                             '(multinodular/mergulhante/nodular). Responda relevante=false '
                                             'quando: nodulo ou cisto com dimensao <1cm, OU sem a dimensao '
                                             'discriminada no laudo (sem a medida nao se confirma >=1cm); a '
                                             'tireoide estiver NORMAL ou sem lesoes; houver apenas AUMENTO '
                                             'DIFUSO da glandula (dimensoes/volume aumentado, tireoidopatia '
                                             'difusa/parenquimatosa, bocio difuso homogeneo) SEM '
                                             'nodulo/cisto (tratamento clinico/medicamentoso, sem foco '
                                             'cirurgico); houver apenas textura difusa/heterogenea SEM '
                                             'nodulo ou cisto; houver apenas linfonodo de aspecto '
                                             'REACIONAL/NORMAL sem outro achado; for '
                                             'pos-operatorio/pos-tireoidectomia sem achado; ou o achado '
                                             'estiver negado/ausente. Na duvida entre benigno/inespecifico e '
                                             'relevante, prefira relevante=false. IMPORTANTE: a regua e V2 — '
                                             'nodulo/cisto contam APENAS com dimensao >=1cm (o criterio do '
                                             'sistema LEGADO, so TI-RADS>3, NAO e a regra); '
                                             'massa/tumor/neoplasia, bocio nodular e linfonodo suspeito '
                                             'contam pela PRESENCA (independem de medida).',
                        'prompt_user_template': 'Contexto:\n{specialty_context}\n\nExcerto:\n{text}'},
         'ordinal_extraction': {'enabled': True,
                             'aggregation_policy': 'max_category',
                             # 0.7.0: SO a categoria ordinal promove. Decisao de negocio (2026-08-18):
                             # a operacao nao absorve o volume dos achados lexicos, entao
                             # bocio/linfonodomegalia/nodulo sem TI-RADS saem da entrega.
                             # Medido: 1.389 dos 2.815 relevantes vinham dai (~49%).
                             'relevance_mode': 'normal_plus_ordinal',
                             'negation': {'tokens': []},
                             'llm_fallback': {'enabled': False,
                                              'trigger': 'alias_without_category',
                                              'confidence': 0.5,
                                              'llm': {'model': 'databricks-claude-haiku-4-5',
                                                      'api_key_env': 'DATABRICKS_TOKEN',
                                                      'json_response_format': False}},
                             'systems': {'ti_rads': {'aliases': ['TI-RADS',
                                                                 'TIRADS',
                                                                 'TI RADS',
                                                                 'ACR TI-RADS'],
                                                     'categories': ['TR1', 'TR2', 'TR3', 'TR4', 'TR5', 'TR6'],
                                                     'patterns': ['(?:T[I]?[- '
                                                                  '_]?R{1,2}ADS|TIRADS)(?:TM)?(?:[^\\S\\n\\r]|[:.=°º®ª()-])*(?:(?:US|USG|ECO|ACR|categoria|cat)\\b(?:[^\\S\\n\\r]|[:.=°º®ª()-])*)*0*(TR\\s?\\d|\\d|iv|vi|v|i{1,3})',
                                                                  '(?:T[I]?[- '
                                                                  '_]?R{1,2}ADS|TIRADS)(?:TM)?(?:[^\\S\\n\\r]|[:.=°º®ª()-])*(?:(?:US|USG|ECO|ACR|categoria|cat)\\b(?:[^\\S\\n\\r]|[:.=°º®ª()-])*)*0*(?:TR\\s?\\d|\\d)\\s*e\\s*0*(TR\\s?\\d|\\d)',
                                                                  '\\bTR\\s?([1-6])\\b'],
                                                     'normalization': {'roman_to_arabic': True},
                                                     'aggregation_legend_filter': {'enabled': True},
                                                     # 0.6.0: TR4 VOLTA a `promote_categories`, agora CONDICIONADO.
                                                     # Regra de negocio (Natan, 2026-08-18): "TR5 aprova sozinho;
                                                     # TR4 so aprova com nodulo/cisto >= 1 cm". Demais categorias V3.
                                                     # MECANISMO (nlp_engine >= 0.8.5): `gates_ordinal_promotion:
                                                     # ['TR4']` nos criterios de tamanho dispensa a blindagem ordinal
                                                     # SO para TR4 — o gate passa a poder rebaixa-lo quando a medida
                                                     # nao atinge 1 cm. TR5/TR6 seguem blindados (nao declarados).
                                                     # POR QUE MUDOU: a 0.5.0 conseguia o mesmo tirando TR4 daqui,
                                                     # mas ai a relevancia dependia do achado LEXICO `nodulo` — e o
                                                     # gate de orgao descarta esse achado em 101 laudos TR4/TR5+
                                                     # (medido 2026-08-18). Nesses, TR4 >= 1 cm virava falso-negativo.
                                                     'relevance_policy': {'promote_categories': ['TR4',
                                                                              'TR5',
                                                                              'TR6']}}}},
         'quantitative_criteria': {'nodulo_maior_1cm': {'description': 'Maior dimensao (em cm) do MAIOR '
                                                                       'nodulo tireoidiano descrito. Em '
                                                                       'medidas "A x B x C" use a maior das '
                                                                       'tres; "6 mm" = 0,6 cm. IGNORE volume '
                                                                       '(cm3) e medidas da '
                                                                       'glandula/lobos/istmo. So achados '
                                                                       'reais (nao negados) e fora da secao '
                                                                       'de indicacao.',
                                                        'anchor': {'finding': 'nodulo', 'text': r'n[oó]dul'},
                                                        'measure': {'name': 'nodulo_max', 'unit': 'cm'},
                                                        'threshold': {'op': '>=', 'value': 1.0},
                                                        'require_measure': True,
                                                        # 0.6.0: condiciona SO o TR4 (TR5/TR6 seguem blindados).
                                                        'gates_ordinal_promotion': ['TR4'],
                                                        'on_met': 'gate_relevance',
                                                        'llm': {'model': 'databricks-claude-haiku-4-5',
                                                                'fallback_models': ['databricks-claude-sonnet-4-5'],
                                                                'api_key_env': 'DATABRICKS_TOKEN',
                                                                'temperature': 0,
                                                                'max_tokens': 256}},
                                   'cisto_maior_1cm': {'description': 'Maior dimensao (em cm) do MAIOR cisto '
                                                                      'tireoidiano descrito (cisto '
                                                                      'coloide/simples/misto). "A x B x C" '
                                                                      'use a maior; "5 mm" = 0,5 cm. IGNORE '
                                                                      'volume da glandula e cistos de outros '
                                                                      'orgaos (renal, epididimo). So achados '
                                                                      'reais e fora da indicacao.',
                                                       'anchor': {'finding': 'cisto', 'text': r'cist[oó]'},
                                                       'measure': {'name': 'cisto_max', 'unit': 'cm'},
                                                       'threshold': {'op': '>=', 'value': 1.0},
                                                       'require_measure': True,
                                                       # 0.6.0: condiciona SO o TR4 (TR5/TR6 seguem blindados).
                                                       'gates_ordinal_promotion': ['TR4'],
                                                       'on_met': 'gate_relevance',
                                                       'llm': {'model': 'databricks-claude-haiku-4-5',
                                                               'fallback_models': ['databricks-claude-sonnet-4-5'],
                                                               'api_key_env': 'DATABRICKS_TOKEN',
                                                               'temperature': 0,
                                                               'max_tokens': 256}},
                                   'linfonodo_suspeito': {'kind': 'qualitative',
                                                          'anchor': {'finding': 'linfonodo'},
                                                          'question': 'Ha, no laudo, linfonodo '
                                                                      'cervical/tireoidiano SUSPEITO ou '
                                                                      'PATOLOGICO (linfonodomegalia '
                                                                      'arredondada, sem hilo gorduroso, com '
                                                                      'necrose, microcalcificacoes, aspecto '
                                                                      'atipico, ou claramente '
                                                                      'aumentado/patologico)? Responda false '
                                                                      'se os linfonodos forem apenas '
                                                                      'REACIONAIS, proeminentes benignos, de '
                                                                      'aspecto habitual/normal, com '
                                                                      'hilo/morfologia preservados, ou se '
                                                                      'estiverem NEGADOS/ausentes ("ausencia '
                                                                      'de/nao ha linfonodomegalias").',
                                                          'on_met': 'gate_relevance',
                                                          'llm': {'model': 'databricks-claude-haiku-4-5',
                                                                  'fallback_models': ['databricks-claude-sonnet-4-5'],
                                                                  'api_key_env': 'DATABRICKS_TOKEN',
                                                                  'temperature': 0,
                                                                  'max_tokens': 256}},
                                   # ------------------------------------- V3: EXAME DE SANGUE
                                   # REQUER nlp_engine >= 0.8.0 (`measure.source.kind: value_text`).
                                   # Com lib anterior a chave e ignorada EM SILENCIO e o criterio
                                   # cai no caminho LLM — conferir a versao antes de comparar.
                                   #
                                   # Por que NAO usam LLM: o resultado laboratorial nao e prosa,
                                   # o laudo E o valor ("1,24"). Medido em jun/2026: 99,4% dos TSH
                                   # tem valor numerico direto. Extrair isso com LLM seriam ~67k
                                   # chamadas/mes para ler numero sem ambiguidade.
                                   #
                                   # `applies_to_exam_type` casa por SUBSTRING contra
                                   # `proced_descricao` (column_map.exm_tipo). E ele que impede o
                                   # criterio de TSH de avaliar um TRAb — cujo nome de exame
                                   # contem "tsh" e cujo valor normal (~0,25) fica ABAIXO de 0,4.
                                   #
                                   # Regra de negocio (2026-08-03): sangue e imagem valem
                                   # ISOLADAMENTE, sem janela de 12 meses. Dai `promote`.
                                   'tsh_suprimido': {
                                       'label': 'Hipertireoidismo',
                                       'description': 'TSH suprimido (hipertireoidismo)',
                                       # NAO usar 'tsh' puro: capturaria TRAb, TSH neonatal e
                                       # paineis de fenilalanina, que tem outra faixa de
                                       # referencia. Estas substrings cobrem 42.440 exames/mes
                                       # com ZERO colisao (medido em jun/2026).
                                       'applies_to_exam_type': ['tsh ultra', 'tsh - hormonio',
                                                                'tireoestimulante',
                                                                'tireostimulante'],
                                       'measure': {'name': 'tsh', 'unit': 'mUI/L',
                                                   'source': {'kind': 'value_text'}},
                                       'threshold': {'op': '<', 'value': 0.4},
                                       # 187 TSH/mes chegam com valor 0 (ausencia gravada como
                                       # zero). Como 0 satisfaz '< 0,4', virariam falso-positivo —
                                       # ~15% das promocoes. Faixa medida: min real 0,001; max 272,9.
                                       'plausible_range': [0.001, 500.0],
                                       # V2 (0.7.0): sangue NAO promove. `annotate_only` preserva prompt,
                                       # limiar e ancora, e SEGUE avaliando e auditando — da para medir o
                                       # que a V3 entregaria sem entregar. Reativar = voltar a 'promote'.
                                       'on_met': 'annotate_only'},
                                   't4_livre_elevado': {
                                       'label': 'Hipertireoidismo',
                                       'description': 'T4 livre acima do limite superior',
                                       # 't4 livre' e NAO 't4': evita 't4 total', que tem outra
                                       # faixa de referencia.
                                       'applies_to_exam_type': ['t4 livre'],
                                       'measure': {'name': 't4_livre', 'unit': 'ng/dL',
                                                   'source': {'kind': 'value_text'}},
                                       # 1,8 ng/dL. A Carol passou 23 pmol/L; convertido
                                       # (x12,87) da 1,79 ng/dL — as duas fontes concordam.
                                       # ⚠️ Copiar "23" direto NUNCA seria atingido (p95 do lake
                                       # = 1,6) e a falha seria SILENCIOSA: a origem nao declara
                                       # unidade.
                                       'threshold': {'op': '>', 'value': 1.8},
                                       # 162 zeros/mes. Inofensivos aqui (0 nao satisfaz '>'), mas
                                       # o guard protege de extracao absurda. Max medido: 9,2.
                                       'plausible_range': [0.01, 50.0],
                                       # V2 (0.7.0): sangue NAO promove. `annotate_only` preserva prompt,
                                       # limiar e ancora, e SEGUE avaliando e auditando — da para medir o
                                       # que a V3 entregaria sem entregar. Reativar = voltar a 'promote'.
                                       'on_met': 'annotate_only'},
                                   'trab_positivo': {
                                       'label': 'Doença de Graves',
                                       'description': 'TRAb (anti-receptor de TSH) positivo',
                                       'applies_to_exam_type': ['trab', 'anti receptor do tsh'],
                                       'measure': {'name': 'trab', 'unit': 'UI/L',
                                                   'source': {'kind': 'value_text'}},
                                       # Corroborado pelo dado: o limite superior da faixa de
                                       # referencia do proprio laboratorio tem mediana 1,76.
                                       'threshold': {'op': '>', 'value': 1.5},
                                       # Max medido: 19,6.
                                       'plausible_range': [0.01, 200.0],
                                       # V2 (0.7.0): sangue NAO promove. `annotate_only` preserva prompt,
                                       # limiar e ancora, e SEGUE avaliando e auditando — da para medir o
                                       # que a V3 entregaria sem entregar. Reativar = voltar a 'promote'.
                                       'on_met': 'annotate_only'},
                                   # 🚩 FLAG, nao promotor — decisao clinica da Carol (2026-08-10):
                                   # "Anti-TPO isolado, sem TSH suprimido e sem TRAb, indica
                                   # paciente para a linha? -> SO ACOMPANHADA."
                                   # O anticorpo marca autoimunidade EM GERAL: e positivo tanto em
                                   # Graves (hiper, alvo) quanto em Hashimoto (HIPO, fora do alvo),
                                   # e Hashimoto e muito mais prevalente. Isolado, infla o escopo
                                   # sem indicar a doenca-alvo — eram 442 encaminhamentos/mes.
                                   # A propria spec de negocio ja o descrevia como "flag autoimune";
                                   # promove-lo foi erro de leitura na implementacao.
                                   'anti_tpo_positivo': {
                                       'label': 'Flag autoimunidade (Anti-TPO)',
                                       'description': 'Anti-TPO (antitireoperoxidase) positivo',
                                       'applies_to_exam_type': ['anti-tpo', 'antitireoperox',
                                                                'tireoperox'],
                                       'measure': {'name': 'anti_tpo', 'unit': 'IU/mL',
                                                   'source': {'kind': 'value_text'}},
                                       # ⚠️ A spec de negocio classificou o Anti-TPO como
                                       # QUALITATIVO; o dado mostra o contrario — e numerico com
                                       # censura a esquerda (2.263 de 3.962 vem "Inferior a 0,2").
                                       # A 0.8.0 trata censura como INTERVALO, entao esses saem
                                       # corretamente como nao-atende. O limiar captura 418 de
                                       # 1.462 com valor (28,6%).
                                       'threshold': {'op': '>', 'value': 34.0},
                                       # Cauda longa: max medido 7.285 no recorte amplo.
                                       'plausible_range': [0.01, 50000.0],
                                       'on_met': 'annotate_only'},
                                   # T3 — FLAG, nao promotor (decisao clinica da Carol, 2026-08-10).
                                   #
                                   # "T3 elevado sozinho SEM TSH suprimido NAO estabelece o
                                   # diagnostico de hipertireoidismo." Ha elevacao de T3/T4 TOTAIS
                                   # sem hipertireoidismo real: excesso de medicacao, disalbuminemia
                                   # familiar, gravidez, uso de estrogenio (anticoncepcional,
                                   # menopausa, pessoas trans) — condicoes que aumentam as proteinas
                                   # ligadoras. Por isso o T4 aqui e LIVRE (nao sofre esse efeito),
                                   # mas o T3 TOTAL sofre — mais um motivo para ser flag.
                                   #
                                   # No pipeline diagnostico o TSH e a PORTA DE ENTRADA: T3/T4 so
                                   # entram DEPOIS de TSH suprimido, para separar franco de
                                   # subclinico. O T3 serve como diagnostico COMPLEMENTAR —
                                   # confirmar T3-toxicose quando TSH baixo + T4 livre normal.
                                   #
                                   # `annotate_only`: avalia e registra no audit, NAO promove
                                   # sozinho. Limiares definidos pela Carol.
                                   #
                                   # DOIS analitos distintos, unidades e faixas diferentes.
                                   # Limiar = limite superior da faixa de referencia que o PROPRIO
                                   # laboratorio grava (campo estruturado), com concentracao quase
                                   # total: 6,3 em 98% dos T3 livre; 1,81 em 99,9% dos T3 total.
                                   # ⚠️ RESSALVA LEVADA A HOMOLOGACAO: faixa de referencia e
                                   # "acima do normal", que NAO e necessariamente "relevante para
                                   # captacao". A spec dizia so "elevado"; a validacao clinica
                                   # decide se o corte fica aqui.
                                   # ⚠️ NAO usar 't3' puro: casaria `t3 reverso` (outro analito,
                                   # mediana 0,49) e exames geneticos (FLT3, bcr/abl t(9;22)).
                                   # As substrings abaixo cobrem 4.129 (livre) e 4.110 (total) por
                                   # mes com ZERO colisao (medido em jun/2026).
                                   't3_livre_elevado': {
                                       'label': 'Flag T3 (livre elevado)',
                                       'description': 'T3 livre elevado',
                                       'applies_to_exam_type': ['t3 livre'],
                                       'measure': {'name': 't3_livre', 'unit': 'pg/mL',
                                                   'source': {'kind': 'value_text'}},
                                       'threshold': {'op': '>=', 'value': 4.4},
                                       # 1 zero medido; max 19,9.
                                       'plausible_range': [0.01, 100.0],
                                       'on_met': 'annotate_only'},
                                   't3_total_elevado': {
                                       'label': 'Flag T3 (total elevado)',
                                       'description': 'T3 total elevado',
                                       'applies_to_exam_type': ['t3 total', 'iodotironina'],
                                       'measure': {'name': 't3_total', 'unit': 'ng/mL',
                                                   'source': {'kind': 'value_text'}},
                                       'threshold': {'op': '>=', 'value': 2.0},
                                       # Max medido 79,0 (provavel outlier de unidade).
                                       'plausible_range': [0.01, 50.0],
                                       'on_met': 'annotate_only'}},
                                   # T3 FORA desta versao: a spec pede "elevado" sem limiar
                                   # numerico. Inventar um geraria FP ou criterio inerte.
         'findings': {'nodulo': {'label': 'Nódulo',
                                 'terms': ['nodulo',
                                           'nódulo',
                                           'nodulos',
                                           'nódulos',
                                           'lesao focal',
                                           'lesões focais'],
                                 'regex': ['\\bn[oó]dulo[s]?\\b',
                                           '\\bles[aã]o\\s+focal\\b',
                                           '\\bles[oõ]es\\s+focais\\b',
                                           '\\bmicron[oó]dulo[s]?\\b',
                                           '\\b(?:imagem|imagens|forma[çc][aã]o|forma[çc][oõ]es|les[aã]o|les[oõ]es)\\s+nodular(es)?\\b',
                                           '\\b(?:imagem|imagens|forma[çc][aã]o|forma[çc][oõ]es)\\s+(?:hipoeco[a-z]*|hipoecog[a-z]*|s[oó]lid[a-z]*|ovoide|ovalad[ao])\\b']},
                      'cisto': {'label': 'Cisto',
                                'terms': ['cisto',
                                          'cistos',
                                          'lesao cistica',
                                          'lesão cística',
                                          'lesoes cisticas',
                                          'lesões císticas',
                                          'formacao cistica',
                                          'formação cística'],
                                'regex': ['\\bcisto[s]?\\b',
                                          '\\bcomponente\\s+c[ií]stico\\b',
                                          '\\bforma[çc][aã]o\\s+c[ií]stica\\b',
                                          '\\bles[aã]o\\s+c[ií]stica\\b']},
                      'massa': {'label': 'Massa',
                                'terms': ['massa',
                                          'massas',
                                          'lesao expansiva',
                                          'lesão expansiva',
                                          'formacao expansiva',
                                          'formação expansiva',
                                          'processo expansivo'],
                                'regex': ['\\bmassa[s]?\\b',
                                          '\\bforma[çc][aã]o\\s+expansiva\\b',
                                          '\\bprocesso\\s+expansivo\\b',
                                          '\\bles[aã]o\\s+expansiva\\b'],
                                'negation_direction': 'both'},
                      'linfonodo': {'label': 'Linfonodomegalia',
                                    'terms': ['linfonodomegalia',
                                              'linfonodomegalias',
                                              'adenomegalia',
                                              'adenopatia',
                                              'linfonodo aumentado',
                                              'linfonodos aumentados',
                                              'linfonodo suspeito',
                                              'linfonodos suspeitos',
                                              'linfonodo atipico',
                                              'linfonodo com necrose',
                                              'linfonodo proeminente',
                                              'linfonodos proeminentes'],
                                    'regex': ['\\blinfonodomegalia[s]?\\b',
                                              '\\badenomegalia[s]?\\b',
                                              '\\badenopati[a-z]*\\b',
                                              '\\blinfonodo[s]?\\b[^.\\n]{0,40}\\b(aumentad[oa]s?|suspeit[oa]s?|at[ií]pic[oa]s?|com\\s+necrose|proeminente[s]?|indeterminad[oa]s?|perda\\s+(?:parcial\\s+)?d[ae]\\s+(?:sua\\s+)?arquitetura\\s+hilar)\\b',
                                              '\\bproemin[êe]ncia\\s+num[ée]rica[^.\\n]{0,40}\\blinfonodo',
                                              '\\baumento\\s+(?:volum[eé]trico|do\\s+n[uú]mero|em\\s+n[uú]mero)[^.\\n]{0,40}\\blinfonodo'],
                                    'skip_organ_gate': True,
                                    'exclude': ['reacional',
                                                'reacionais',
                                                'reativo',
                                                'reativos',
                                                'reativa',
                                                'reativas',
                                                'racional',
                                                'racionais'],
                                    'unless': ['necrose',
                                               'atipico',
                                               'atipicos',
                                               'suspeito',
                                               'suspeita',
                                               'suspeitos',
                                               'suspeitas',
                                               'metastase',
                                               'metastatico',
                                               'metastaticos',
                                               'irregular',
                                               'irregulares',
                                               'globoso',
                                               'globosos'],
                                    'negation_direction': 'both'},
                      'tumor': {'label': 'Tumor',
                                'terms': ['tumor',
                                          'tumores',
                                          'neoplasia',
                                          'neoplasias',
                                          'neoplasia maligna',
                                          'processo tumoral',
                                          'formacao tumoral',
                                          'formação tumoral'],
                                'regex': ['\\btumor(es)?\\b',
                                          '\\bneoplasi[a-z]+\\b',
                                          '\\bprocesso\\s+tumoral\\b',
                                          '\\bforma[çc][aã]o\\s+tumoral\\b']},
                      'bocio': {'label': 'Bócio',
                                'terms': ['bocio',
                                          'bócio',
                                          'bocio mergulhante',
                                          'bócio mergulhante',
                                          'bocio difuso',
                                          'bócio difuso',
                                          'bocio multinodular',
                                          'bócio multinodular',
                                          'bocio nodular',
                                          'bócio nodular',
                                          'tireomegalia'],
                                'regex': ['\\bb[oó]cio(\\s+(mergulhante|difuso|multinodular|nodular))?\\b',
                                          '\\btireomegalia\\b'],
                                'exclude': ['difuso',
                                            'difusa',
                                            'difusos',
                                            'difusas',
                                            'homogeneo',
                                            'homogenea',
                                            'homogeneos',
                                            'homogeneas',
                                            'parenquimatosa',
                                            'parenquimatoso',
                                            'inespecifico',
                                            'inespecifica',
                                            'constitucional'],
                                'unless': ['mergulhante',
                                           'multinodular',
                                           'nodular',
                                           'nodulares',
                                           'nodulo',
                                           'nodulos']},
                      'hipertireoidismo': {'label': 'Hipertireoidismo',
                                           'terms': ['hipertiroidismo',
                                                     'hipertireoidismo',
                                                     'doenca de graves',
                                                     'doença de graves',
                                                     'tirotoxicose'],
                                           'regex': ['\\bhiperti(reoidismo|roidismo)\\b',
                                                     '\\bdoen[cç][a]?\\s+de\\s+graves\\b',
                                                     '\\btirotoxicose\\b']}},
         'findings_policy': {# 0.6.0: exibicao de NEGOCIO na coluna `findings` (nlp_engine >= 0.8.5).
                             # Formato pedido pelo Natan em 2026-08-18: "TRx - Nodulo (dimensoes)".
                             # So o achado MEDIDO recebe a categoria — TI-RADS classifica nodulo/cisto,
                             # nao bocio nem hipertireoidismo.
                             'display': {'measure_suffix': True, 'ordinal_prefix': True},
                             'ignore_sections': ['indicação',
                                                 'indicacao',
                                                 'indicação clínica',
                                                 'indicacao clinica',
                                                 'história clínica',
                                                 'historia clinica',
                                                 'informação clínica',
                                                 'informacao clinica',
                                                 'hipótese diagnóstica',
                                                 'hipotese diagnostica',
                                                 'dados clínicos',
                                                 'dados clinicos',
                                                 'quadro clínico',
                                                 'quadro clinico'],
                             'organ': {'scope': 'block', 'max_chars': 220}},
         'negation': {'phrases': ['sem',
                                  'nao ha',
                                  'não há',
                                  'ausencia de',
                                  'ausência de',
                                  'livre de',
                                  'não se identificam',
                                  'não identificamos',
                                  'não se observam',
                                  'não se caracterizam',
                                  'não se evidenciam',
                                  'não foram visualizados',
                                  'não foram visualizadas',
                                  'não foram detectados',
                                  'não foram detectadas',
                                  'não foram identificados',
                                  'não foram identificadas',
                                  'não foram observados',
                                  'não foram observadas',
                                  'não foram caracterizados',
                                  'não foram caracterizadas',
                                  'não foram evidenciados',
                                  'não foram evidenciadas',
                                  'não foi visualizado',
                                  'não foi visualizada',
                                  'não foi detectado',
                                  'não foi detectada',
                                  'não foi identificado',
                                  'não foi identificada',
                                  'não foi observado',
                                  'não foi observada',
                                  'não foi caracterizado',
                                  'não foi caracterizada',
                                  'não foi evidenciado',
                                  'não foi evidenciada',
                                  'não se observando',
                                  'não se identificando',
                                  'não se caracterizando',
                                  'não se observa',
                                  'não se identifica',
                                  'não se caracteriza',
                                  'não se visualiza',
                                  'não se visualizam',
                                  'não identifica-se',
                                  'não identificam-se',
                                  'não evidencia-se',
                                  'não evidenciam-se',
                                  'não observa-se',
                                  'não observam-se',
                                  'não visualiza-se',
                                  'não visualizam-se',
                                  'não caracteriza-se',
                                  'não caracterizam-se',
                                  'não se evidenciando',
                                  'não caracterizadas',
                                  'não caracterizada',
                                  'não caracterizados',
                                  'não caracterizado',
                                  'ausente',
                                  'ausentes'],
                      'window': 8,
                      'direction_default': 'left'}},
 'catalog': 'diamond_tirads',
 'data': {'gold_domains': ['exame.identificacao', 'exame.procedimento', 'exame.datas', 'exame.laudos'],
          # ⚠️ AQUI AS KEYWORDS CASAM O **NOME DO EXAME** (`proced_descricao`), nao o texto do
          # laudo — `plataform/data/ntb_ia_gold_filters.py:25,48`. E o oposto do runner legado,
          # onde `gold_filter` era substring sobre o laudo. Consequencia medida no run de
          # 2026-08-04 (janela 25-27/06): com as 3 keywords originais entraram 376 dos 586 ids
          # da base ouro, e 25 POSITIVOS ficaram de fora — 19,7% do recall — porque o nome do
          # exame nao contem "tireoide". Eram TC de pescoco (13), US de pescoco/cervical (6) e
          # biopsia/PAAF de linfonodo (5).
          # `ultrassom tireoide` foi removida: e substring que ja contem `tireoide`, nao
          # acrescentava nada. `pescoco`/`pescoço` entram porque a spec do especialista lista
          # explicitamente "US de pescoco e da tireoide" e "TC de pescoco (partes moles,
          # laringe, tireoide e faringe)". Custo medido: +557 exames na janela (+26%),
          # recuperando 19 dos 25 positivos.
          # NAO incluidos de proposito: `cervical` (casa "coluna cervical" — +1.120 exames para
          # 3 positivos) e `biopsia`/`punc` genericos (+1.996 para 4). Nenhum dos dois consta da
          # lista da spec; os 6 positivos restantes sao "regiao cervical/supraclavicular" e
          # "biopsia/PAAF de linfonodo", que precisam de decisao do especialista antes de virar
          # criterio. Ver `docs/motor-nlp/tireoide/nota-mlops-filtro-selecao-gold.md`.
          # V3: + cintilografia de tireoide + exame de sangue.
          #
          # ⚠️ NAO existe keyword `cintilog`: ela seria REDUNDANTE e nociva. Medido em jun/2026:
          # das 1.523 cintilografias do mes, apenas 25 tem "tireoid" no nome — e essas ja entram
          # pela keyword `tireoide`, que existe desde a V2. A keyword generica trazia 1.498 exames
          # de miocardio, figado, refluxo e galio-67, sem nenhum relevante possivel.
          # Das 25: 11 sao PARATIREOIDE (fora do escopo por decisao registrada) e 14 sao tireoide.
          # Ficam fora tambem as 17 de "corpo inteiro com iodo-131" (pesquisa de metastase
          # pos-tireoidectomia) — paciente ja tratado, que a regua V2 exclui.
          # ⚠️ Nesta plataforma `gold_filter.keywords` vira `proced_descricao rlike '(?i)<kw>'` —
          # casa o NOME DO EXAME. (No runner legado a MESMA chave e substring no TEXTO DO LAUDO e
          # DESCARTA a linha que nao casa; foi o que zerou o sangue la, porque um laudo de TSH e
          # literalmente "1,24" e nunca contem a palavra "tireoide".)
          #
          # As keywords de sangue trazem tambem TRAb, TSH neonatal e paineis de fenilalanina para
          # o lote. Isso e SEGURO: a desambiguacao acontece no `applies_to_exam_type` de cada
          # criterio — um TRAb no lote simplesmente nao e avaliado pelo criterio de TSH.
          'filters': {'gold_filter': {'keywords': ['tireoide', 'tireóide', 'pescoco', 'pescoço',
                                                   'tsh', 'tireoestimulante', 't4 livre',
                                                   'trab', 'anti receptor do tsh',
                                                   'anti-tpo', 'antitireoperox',
                                                   't3 livre', 't3 total', 'iodotironina'],
                                      'mode': 'any'}},
          'column_map': {'id_exame': 'id_exame',
                         'id_paciente': ['id_paciente', 'id_patient'],
                         'id_unidade': 'id_unidade',
                         'exm_laudo_texto': ['proced_laudo_exame_original', 'proced_laudo_exame'],
                         'exm_mod': ['cod_procedimento', 'tp_codigo_procedimento'],
                         'exm_tipo': 'proced_descricao',
                         'dt_exame': 'dt_exame'},
          # `legacy` REMOVIDO: a chave NAO e lida nesta plataforma (zero ocorrencias em .py).
          # Ela pertence ao runner antigo, que a usa em `homolog.py` para a comparacao
          # motor x legado — e os configs daquele runner vivem no proprio repo dele
          # (`fabrica-ia-plataforma`). Manter a heranca aqui so apontava para tabelas de outro
          # catalogo (`diamond_tirads`) que esta plataforma nem acessa.
          },
 'runtime': {'profile': 'llm_http',
             'llm_router': {'enabled': True,
                            'mode': 'llm',
                            'api_key_env': 'DATABRICKS_TOKEN',
                            'fallback_policy': 'keep_current',
                            'json_response_format': False}},
 'monitoring': {'metrics_table': '{catalog}.tirads.tb_diamond_mod_metricas_qualidade',
                'alert_threshold_relevance_drop': 0.15,
                'baseline_lookback_days': 30},
 'distribution': {'nome_solucao': 'tirads',
                  'estagio': 'homologacao',
                  'outbound_volume': '/Volumes/{catalog}/ia/outbound/tirads/{run_id}/',
                  'control_tables': {'outbox': '{catalog}.ia.delivery_outbox',
                                     'inbox': '{catalog}.ia.delivery_inbox'},
                  'delivery': {'logic_app_secret_scope': 'fabrica-ia',
                               'logic_app_secret_key': 'logic-app-delivery-url',
                               'onedrive_user': ''},
                  'outputs': [{'name': 'saida_por_unidade',
                               'format': 'multi_file_xlsx',
                               'datasets': ['saida'],
                               'split_by': 'id_unidade',
                               'remote_subpath': 'tirads/saida'}]}}

import json  # noqa: E402

dbutils.notebook.exit(json.dumps(CONFIG, ensure_ascii=False))  # noqa: F821
