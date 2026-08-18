# Databricks notebook source
# MAGIC %md
# MAGIC # Config da especialidade `tirads`
# MAGIC
# MAGIC Define `CONFIG` (fonte de verdade desta especialidade) e o devolve via `dbutils.notebook.exit(json...)` — o runner `ntb_ia_motor_e2e` carrega com `dbutils.notebook.run`. Edite à mão; para uma nova especialidade, copie `ntb_ia_template_config.py`. O YAML em `configs/nlp/tirads/config.yaml` fica apenas como referência.

# COMMAND ----------


CONFIG = {
    'specialty_id': 'tirads',
    'config_version': '0.1.0-tirads-rads-v22.11-v2',
    'model_version': 'v0',
    'description': 'Motor NLP TI-RADS + achados clinicos (linha de cuidado tireoide).',
    'nlp': {
        'shared_organs_path': '../shared/organs.yaml',
        'target_organs': ['tireoide'],
        'organs': {
            'tireoide': {
                'regex': ['\\btireoid\\w*'],
                'seeds': [
                    'tireóide', 'glândula tireoide', 'glândula tireóide', 'glandula tireoide',
                    'glandula tireóide', 'lobo tireoidiano', 'lobo tireóideo', 'lobo tireoideo',
                    'istmo tireoidiano', 'parênquima tireoidiano', 'parenquima tireoidiano',
                ],
            },
        },
        'segmentation': {
            'mode': 'full_doc',
        },
        'document_vet': {
            'enabled': True,
            'normality_phrases': [
                'sem alterações significativas', 'dentro dos limites da normalidade',
                'dentro dos parâmetros da normalidade', 'exame normal',
                'ultrassonografia da tireoide normal',
            ],
            'soft_findings': ['nodulo', 'cisto'],
        },
        'emit_decision_trail': True,
        'score_policy_version': 'v1_bins_legacy',
        'use_spacy_matcher': True,
        'feature_flags': {
            'rule_engine': True,
            'calibrated_hybrid': True,
        },
        'embeddings': {
            'use_embeddings': True,
            'decision_mode': 'hybrid',
            'embedding_backend': 'auto',
            'embedding_model': '/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/st_models/paraphrase-multilingual-MiniLM-L12-v2',
            'similarity_threshold': 0.88,
            'ambiguity_band': [0.3, 0.7],
            'hybrid_weight_rule': 0.7,
            'hybrid_weight_semantic': 0.3,
            'emit_as_finding': False,
        },
        'llm_router': {
            'mode': 'llm',
            'provider': 'openai_compatible',
            'model': 'databricks-claude-haiku-4-5',
            # Resiliencia a rate limit (nlp_engine>=0.5.12): ao 429 no haiku, FAILOVER p/ sonnet
            # (tier separado) em vez de serializar a espera. max_tokens baixo: a resposta e um JSON
            # curto {"relevante": bool} — limita a demanda de tokens/chamada.
            'fallback_models': ['databricks-claude-sonnet-4-5'],
            'max_tokens': 256,
            'api_key_env': 'DATABRICKS_TOKEN',
            'uncertainty_band': [0.35, 0.65],
            'fallback_policy': 'keep_current',
            'max_input_chars': 8000,
            'json_response_format': False,
            'prompt_system': 'Voce e um assistente de triagem clinica em tireoide (linha de cuidado TI-RADS). Responda APENAS com um unico objeto JSON, sem texto adicional. Esquema: {"relevante": boolean}.',
            'specialty_context': 'Tarefa: decidir se o laudo de imagem (US/Doppler de tireoide, US de pescoco, TC de pescoco, cintilografia, PAAF/biopsia) deve ser ENCAMINHADO para captacao na linha de cuidado de tireoide. Responda relevante=true SOMENTE quando houver, na tireoide ou cadeias cervicais, um destes achados REAIS (nao negados): nodulo ou cisto tireoidiano com dimensao >=1cm; TI-RADS 4 ou 5; massa, tumor ou neoplasia; linfonodomegalia ou linfonodo aumentado/suspeito/INDETERMINADO ou com perda de arquitetura hilar; bocio NODULAR (multinodular/mergulhante/nodular). Responda relevante=false quando: nodulo ou cisto com dimensao <1cm, OU sem a dimensao discriminada no laudo (sem a medida nao se confirma >=1cm); a tireoide estiver NORMAL ou sem lesoes; houver apenas AUMENTO DIFUSO da glandula (dimensoes/volume aumentado, tireoidopatia difusa/parenquimatosa, bocio difuso homogeneo) SEM nodulo/cisto (tratamento clinico/medicamentoso, sem foco cirurgico); houver apenas textura difusa/heterogenea SEM nodulo ou cisto; houver apenas linfonodo de aspecto REACIONAL/NORMAL sem outro achado; for pos-operatorio/pos-tireoidectomia sem achado; ou o achado estiver negado/ausente. Na duvida entre benigno/inespecifico e relevante, prefira relevante=false. IMPORTANTE: a regua e V2 — nodulo/cisto contam APENAS com dimensao >=1cm (o criterio do sistema LEGADO, so TI-RADS>3, NAO e a regra); massa/tumor/neoplasia, bocio nodular e linfonodo suspeito contam pela PRESENCA (independem de medida).',
            'prompt_user_template': 'Contexto:\n{specialty_context}\n\nExcerto:\n{text}',
        },
        'rads_extraction': {
            'enabled': True,
            'aggregation_policy': 'max_category',
            'relevance_mode': 'rule_plus_rads',
            'negation': {
                'tokens': [],
            },
            'llm_fallback': {
                'enabled': False,
                'trigger': 'alias_without_category',
                'confidence': 0.5,
                'llm': {
                    'model': 'databricks-claude-haiku-4-5',
                    'api_key_env': 'DATABRICKS_TOKEN',
                    'json_response_format': False,
                },
            },
            'systems': {
                'ti_rads': {
                    'aliases': ['TI-RADS', 'TIRADS', 'TI RADS', 'ACR TI-RADS'],
                    'categories': ['TR1', 'TR2', 'TR3', 'TR4', 'TR5', 'TR6'],
                    'patterns': [
                        '(?:T[I]?[- _]?R{1,2}ADS|TIRADS)(?:TM)?(?:[^\\S\\n\\r]|[:.=°º®ª()-])*(?:(?:US|USG|ECO|ACR|categoria|cat)\\b(?:[^\\S\\n\\r]|[:.=°º®ª()-])*)*0*(TR\\s?\\d|\\d|iv|vi|v|i{1,3})',
                        '(?:T[I]?[- _]?R{1,2}ADS|TIRADS)(?:TM)?(?:[^\\S\\n\\r]|[:.=°º®ª()-])*(?:(?:US|USG|ECO|ACR|categoria|cat)\\b(?:[^\\S\\n\\r]|[:.=°º®ª()-])*)*0*(?:TR\\s?\\d|\\d)\\s*e\\s*0*(TR\\s?\\d|\\d)',
                        '\\bTR\\s?([1-6])\\b',
                    ],
                    'normalization': {
                        'roman_to_arabic': True,
                    },
                    'aggregation_legend_filter': {
                        'enabled': True,
                    },
                    'relevance_policy': {
                        'promote_categories': ['TR4', 'TR5', 'TR6'],
                    },
                },
            },
        },
        'quantitative_criteria': {
            'nodulo_maior_1cm': {
                'description': 'Maior dimensao (em cm) do MAIOR nodulo tireoidiano descrito. Em medidas "A x B x C" use a maior das tres; "6 mm" = 0,6 cm. IGNORE volume (cm3) e medidas da glandula/lobos/istmo. So achados reais (nao negados) e fora da secao de indicacao.',
                'anchor': {
                    'finding': 'nodulo',
                },
                'measure': {
                    'name': 'nodulo_max',
                    'unit': 'cm',
                },
                'threshold': {
                    'op': '>=',
                    'value': 1.0,
                },
                # Regua V2 (nlp_engine>=0.6.2): so e relevante com a dimensao >=1cm DISCRIMINADA.
                # Sem medida (met=None) rebaixa, em vez do fail-safe. Failover no llm reduz o risco
                # de met=None por falha de extracao (que geraria FN).
                'require_measure': True,
                'on_met': 'gate_relevance',
                'llm': {
                    'model': 'databricks-claude-haiku-4-5',
                    # Failover (nlp_engine>=0.5.12): em 429 do haiku, tenta o sonnet antes de
                    # desistir — evita met=None por rate-limit (que rebaixaria com require_measure).
                    'fallback_models': ['databricks-claude-sonnet-4-5'],
                    'api_key_env': 'DATABRICKS_TOKEN',
                    'temperature': 0,
                    'max_tokens': 256,
                },
            },
            'cisto_maior_1cm': {
                'description': 'Maior dimensao (em cm) do MAIOR cisto tireoidiano descrito (cisto coloide/simples/misto). "A x B x C" use a maior; "5 mm" = 0,5 cm. IGNORE volume da glandula e cistos de outros orgaos (renal, epididimo). So achados reais e fora da indicacao.',
                'anchor': {
                    'finding': 'cisto',
                },
                'measure': {
                    'name': 'cisto_max',
                    'unit': 'cm',
                },
                'threshold': {
                    'op': '>=',
                    'value': 1.0,
                },
                # Regua V2 (nlp_engine>=0.6.2): so e relevante com a dimensao >=1cm DISCRIMINADA.
                # Sem medida (met=None) rebaixa, em vez do fail-safe. Failover no llm reduz o risco
                # de met=None por falha de extracao (que geraria FN).
                'require_measure': True,
                'on_met': 'gate_relevance',
                'llm': {
                    'model': 'databricks-claude-haiku-4-5',
                    # Failover (nlp_engine>=0.5.12): em 429 do haiku, tenta o sonnet antes de
                    # desistir — evita met=None por rate-limit (que rebaixaria com require_measure).
                    'fallback_models': ['databricks-claude-sonnet-4-5'],
                    'api_key_env': 'DATABRICKS_TOKEN',
                    'temperature': 0,
                    'max_tokens': 256,
                },
            },
            'linfonodo_suspeito': {
                'kind': 'qualitative',
                'anchor': {
                    'finding': 'linfonodo',
                },
                'question': 'Ha, no laudo, linfonodo cervical/tireoidiano SUSPEITO ou PATOLOGICO (linfonodomegalia arredondada, sem hilo gorduroso, com necrose, microcalcificacoes, aspecto atipico, ou claramente aumentado/patologico)? Responda false se os linfonodos forem apenas REACIONAIS, proeminentes benignos, de aspecto habitual/normal, com hilo/morfologia preservados, ou se estiverem NEGADOS/ausentes ("ausencia de/nao ha linfonodomegalias").',
                'on_met': 'gate_relevance',
                'llm': {
                    'model': 'databricks-claude-haiku-4-5',
                    # Failover (nlp_engine>=0.5.12): em 429 do haiku, tenta o sonnet antes de
                    # desistir — evita met=None por rate-limit (que rebaixaria com require_measure).
                    'fallback_models': ['databricks-claude-sonnet-4-5'],
                    'api_key_env': 'DATABRICKS_TOKEN',
                    'temperature': 0,
                    'max_tokens': 256,
                },
            },
        },
        'findings': {
            'nodulo': {
                'terms': ['nodulo', 'nódulo', 'nodulos', 'nódulos', 'lesao focal', 'lesões focais'],
                'regex': [
                    '\\bn[oó]dulo[s]?\\b', '\\bles[aã]o\\s+focal\\b', '\\bles[oõ]es\\s+focais\\b',
                    '\\bmicron[oó]dulo[s]?\\b',
                    '\\b(?:imagem|imagens|forma[çc][aã]o|forma[çc][oõ]es|les[aã]o|les[oõ]es)\\s+nodular(es)?\\b',
                    '\\b(?:imagem|imagens|forma[çc][aã]o|forma[çc][oõ]es)\\s+(?:hipoeco[a-z]*|hipoecog[a-z]*|s[oó]lid[a-z]*|ovoide|ovalad[ao])\\b',
                ],
            },
            'cisto': {
                'terms': [
                    'cisto', 'cistos', 'lesao cistica', 'lesão cística', 'lesoes cisticas',
                    'lesões císticas', 'formacao cistica', 'formação cística',
                ],
                'regex': [
                    '\\bcisto[s]?\\b', '\\bcomponente\\s+c[ií]stico\\b',
                    '\\bforma[çc][aã]o\\s+c[ií]stica\\b', '\\bles[aã]o\\s+c[ií]stica\\b',
                ],
            },
            'massa': {
                'terms': [
                    'massa', 'massas', 'lesao expansiva', 'lesão expansiva', 'formacao expansiva',
                    'formação expansiva', 'processo expansivo',
                ],
                'regex': [
                    '\\bmassa[s]?\\b', '\\bforma[çc][aã]o\\s+expansiva\\b',
                    '\\bprocesso\\s+expansivo\\b', '\\bles[aã]o\\s+expansiva\\b',
                ],
                'negation_direction': 'both',
            },
            'linfonodo': {
                'terms': [
                    'linfonodomegalia', 'linfonodomegalias', 'adenomegalia', 'adenopatia',
                    'linfonodo aumentado', 'linfonodos aumentados', 'linfonodo suspeito',
                    'linfonodos suspeitos', 'linfonodo atipico', 'linfonodo com necrose',
                    'linfonodo proeminente', 'linfonodos proeminentes',
                ],
                'regex': [
                    '\\blinfonodomegalia[s]?\\b', '\\badenomegalia[s]?\\b', '\\badenopati[a-z]*\\b',
                    '\\blinfonodo[s]?\\b[^.\\n]{0,40}\\b(aumentad[oa]s?|suspeit[oa]s?|at[ií]pic[oa]s?|com\\s+necrose|proeminente[s]?|indeterminad[oa]s?|perda\\s+(?:parcial\\s+)?d[ae]\\s+(?:sua\\s+)?arquitetura\\s+hilar)\\b',
                    '\\bproemin[êe]ncia\\s+num[ée]rica[^.\\n]{0,40}\\blinfonodo',
                    '\\baumento\\s+(?:volum[eé]trico|do\\s+n[uú]mero|em\\s+n[uú]mero)[^.\\n]{0,40}\\blinfonodo',
                ],
                'skip_organ_gate': True,
                'exclude': [
                    'reacional', 'reacionais', 'reativo', 'reativos', 'reativa', 'reativas',
                    'racional', 'racionais',
                ],
                'unless': [
                    'necrose', 'atipico', 'atipicos', 'suspeito', 'suspeita', 'suspeitos',
                    'suspeitas', 'metastase', 'metastatico', 'metastaticos', 'irregular',
                    'irregulares', 'globoso', 'globosos',
                ],
                'negation_direction': 'both',
            },
            'tumor': {
                'terms': [
                    'tumor', 'tumores', 'neoplasia', 'neoplasias', 'neoplasia maligna',
                    'processo tumoral', 'formacao tumoral', 'formação tumoral',
                ],
                'regex': [
                    '\\btumor(es)?\\b', '\\bneoplasi[a-z]+\\b', '\\bprocesso\\s+tumoral\\b',
                    '\\bforma[çc][aã]o\\s+tumoral\\b',
                ],
            },
            'bocio': {
                'terms': [
                    'bocio', 'bócio', 'bocio mergulhante', 'bócio mergulhante', 'bocio difuso',
                    'bócio difuso', 'bocio multinodular', 'bócio multinodular', 'bocio nodular',
                    'bócio nodular', 'tireomegalia',
                ],
                'regex': [
                    '\\bb[oó]cio(\\s+(mergulhante|difuso|multinodular|nodular))?\\b',
                    '\\btireomegalia\\b',
                ],
                'exclude': [
                    'difuso', 'difusa', 'difusos', 'difusas', 'homogeneo', 'homogenea',
                    'homogeneos', 'homogeneas', 'parenquimatosa', 'parenquimatoso', 'inespecifico',
                    'inespecifica', 'constitucional',
                ],
                'unless': ['mergulhante', 'multinodular', 'nodular', 'nodulares', 'nodulo', 'nodulos'],
            },
            'hipertireoidismo': {
                'terms': [
                    'hipertiroidismo', 'hipertireoidismo', 'doenca de graves', 'doença de graves',
                    'tirotoxicose',
                ],
                'regex': [
                    '\\bhiperti(reoidismo|roidismo)\\b', '\\bdoen[cç][a]?\\s+de\\s+graves\\b',
                    '\\btirotoxicose\\b',
                ],
            },
        },
        'findings_policy': {
            'ignore_sections': [
                'indicação', 'indicacao', 'indicação clínica', 'indicacao clinica',
                'história clínica', 'historia clinica', 'informação clínica', 'informacao clinica',
                'hipótese diagnóstica', 'hipotese diagnostica', 'dados clínicos', 'dados clinicos',
                'quadro clínico', 'quadro clinico',
            ],
            'organ': {
                'scope': 'block',
                'max_chars': 220,
            },
        },
        'negation': {
            'phrases': [
                'sem', 'nao ha', 'não há', 'ausencia de', 'ausência de', 'livre de',
                'não se identificam', 'não identificamos', 'não se observam', 'não se caracterizam',
                'não se evidenciam', 'não foram visualizados', 'não foram visualizadas',
                'não foram detectados', 'não foram detectadas', 'não foram identificados',
                'não foram identificadas', 'não foram observados', 'não foram observadas',
                'não foram caracterizados', 'não foram caracterizadas', 'não foram evidenciados',
                'não foram evidenciadas', 'não foi visualizado', 'não foi visualizada',
                'não foi detectado', 'não foi detectada', 'não foi identificado',
                'não foi identificada', 'não foi observado', 'não foi observada',
                'não foi caracterizado', 'não foi caracterizada', 'não foi evidenciado',
                'não foi evidenciada', 'não se observando', 'não se identificando',
                'não se caracterizando', 'não se observa', 'não se identifica',
                'não se caracteriza', 'não se visualiza', 'não se visualizam', 'não identifica-se',
                'não identificam-se', 'não evidencia-se', 'não evidenciam-se', 'não observa-se',
                'não observam-se', 'não visualiza-se', 'não visualizam-se', 'não caracteriza-se',
                'não caracterizam-se', 'não se evidenciando', 'não caracterizadas',
                'não caracterizada', 'não caracterizados', 'não caracterizado', 'ausente',
                'ausentes',
            ],
            'window': 8,
            'direction_default': 'left',
        },
    },
    'catalog': 'diamond_tirads',
    'data': {
        'gold_domains': ['exame.identificacao', 'exame.procedimento', 'exame.datas', 'exame.laudos'],
        'filters': {
            'gold_filter': {
                'keywords': ['tireoide', 'tireóide', 'ultrassom tireoide'],
                'mode': 'any',
            },
        },
        'column_map': {
            'id_exame': 'an',
            'id_paciente': 'id_pct',
            'id_unidade': 'idunidade',
            'exm_laudo_texto': ['Laudo'],
            'exm_mod': 'modalidade',
            'exm_tipo': 'tipoexame',
            'dt_exame': 'dataexame',
        },
        'legacy': {
            'enabled': True,
            'entrada_ref': 'diamond_tirads.tirads.tb_diamond_mod_tirads_entrada',
            'saida_ref': 'diamond_tirads.tirads.tb_diamond_mod_tirads_saida',
            'aliases': {
                'exec_date_col': 'dataExecucaoModelo',
                'saida_id_col': 'exm_an',
            },
        },
    },
    'runtime': {
        'profile': 'rule_only',
        'llm_router': {
            'enabled': False,
            'mode': 'llm',
            'api_key_env': 'DATABRICKS_TOKEN',
            'fallback_policy': 'keep_current',
            'json_response_format': False,
        },
    },
    'monitoring': {
        'metrics_table': '{catalog}.tirads.tb_diamond_mod_metricas_qualidade',
        'alert_threshold_relevance_drop': 0.15,
        'baseline_lookback_days': 30,
    },
    'distribution': {
        'nome_solucao': 'tirads',
        'estagio': 'homologacao',
        'outbound_volume': '/Volumes/{catalog}/ia/outbound/tirads/{run_id}/',
        'control_tables': {
            'outbox': '{catalog}.ia.delivery_outbox',
            'inbox': '{catalog}.ia.delivery_inbox',
        },
        'delivery': {
            'logic_app_secret_scope': 'fabrica-ia',
            'logic_app_secret_key': 'logic-app-delivery-url',
            'onedrive_user': '',
        },
        'outputs': [
            {
                'name': 'saida_por_unidade',
                'format': 'multi_file_xlsx',
                'datasets': ['saida'],
                'split_by': 'id_unidade',
                'remote_subpath': 'tirads/saida',
            },
        ],
    },
}

import json  # noqa: E402

dbutils.notebook.exit(json.dumps(CONFIG, ensure_ascii=False))  # noqa: F821
