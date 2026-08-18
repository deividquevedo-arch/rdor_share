# Databricks notebook source
# MAGIC %md
# MAGIC # Config da especialidade `hepatologia`
# MAGIC
# MAGIC Define `CONFIG` (fonte de verdade desta especialidade) e o devolve via `dbutils.notebook.exit(json...)` — o runner `ntb_ia_motor_e2e` carrega com `dbutils.notebook.run`. Edite à mão; para uma nova especialidade, copie `ntb_ia_template_config.py`. O YAML em `configs/nlp/hepatologia/config.yaml` fica apenas como referência.

# COMMAND ----------


CONFIG = {
    'specialty_id': 'hepatologia',
    'config_version': '0.1.14-hep-v3',
    'model_version': 'v0',
    'description': 'Motor NLP de hepatologia (v0): relevância hepato-biliar em laudos de imagem. Léxico fígado/vesícula/vias biliares + achados; perfil llm_http. v0.1.13-hep-emb-volume: bloco `nlp` SINCRONIZADO do YAML homologado na HML (ntb-config_absolute.yaml) — a homologação usara só o YAML (infra parcial); agora o ntb reflete o mesmo nlp. Demais blocos (catalog/data/monitoring/distribution) preservados. (Cientistas: descrevam aqui objetivo, escopo e mudanças desta versão.)',
    'nlp': {
        'shared_organs_path': '../shared/organs.yaml',
        'target_organs': ['figado', 'vesicula_biliar', 'vias_biliares'],
        'organs': {
            'figado': {
                'regex': ['\\bhepat\\w*'],
            },
            'vesicula_biliar': {
                'seeds': ['vesicula biliar', 'vesícula biliar', 'vesicula', 'vesícula', 'colecisto'],
            },
            'vias_biliares': {
                'seeds': ['vias biliares', 'via biliar', 'coledoco', 'colédoco', 'hepatocoledoco'],
            },
        },
        'segmentation': {
            'mode': 'auto',
            'force_full_doc_for': [],
        },
        'score_policy_version': 'v1_bins_legacy',
        'use_spacy_matcher': True,
        'feature_flags': {
            'calibrated_hybrid': True,
        },
        'embeddings': {
            'use_embeddings': True,
            'decision_mode': 'hybrid',
            'embedding_backend': 'auto',
            'embedding_model': '/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/st_models/paraphrase-multilingual-MiniLM-L12-v2',
            'similarity_threshold': 0.78,
            'ambiguity_band': [0.3, 0.7],
            'hybrid_weight_rule': 0.7,
            'hybrid_weight_semantic': 0.3,
            'similarity_threshold_by_model': {
                'pucpr/biobertpt-all': {
                    'similarity_threshold': 0.88,
                    'ambiguity_band': [0.35, 0.65],
                },
            },
        },
        'text_pipeline': {
            'trailing_line_patterns': ['(?i)^\\s*este\\s+laudo\\s+pode\\s+nao\\s+estar\\s+completo.*webris.*$'],
        },
        'llm_router': {
            'mode': 'llm',
            'provider': 'openai_compatible',
            'model': 'databricks-claude-haiku-4-5',
            'uncertainty_band': [0.35, 0.65],
            'fallback_policy': 'keep_current',
            'max_input_chars': 8000,
            'json_response_format': False,
            'prompt_system': 'Voce e um assistente de triagem clinica em hepatologia. Responda APENAS com um unico objeto JSON, sem texto adicional. Esquema: {"relevante": boolean}.',
            'specialty_context': 'Tarefa: decidir se o laudo de imagem deve ser ENCAMINHADO para avaliacao em hepatologia. Responda relevante=true SOMENTE quando houver doenca ou alteracao hepatica que justifique acompanhamento, por exemplo: esteatose hepatica; cirrose ou sinais de hepatopatia cronica; hipertensao portal; fibrose hepatica; lesao focal suspeita ou indeterminada (ex.: nodulo hepatico, LI-RADS 4 ou 5); dilatacao das vias biliares. Responda relevante=false quando o figado e as vias biliares estiverem NORMAIS, quando houver apenas achados BENIGNOS tipicos (ex.: cisto hepatico simples, hemangioma tipico), quando houver apenas alteracoes INESPECIFICAS, ou quando o achado for negado/ausente. Na duvida entre benigno e relevante, prefira relevante=false, EXCETO esteatose, que e sempre relevante.',
            'prompt_user_template': 'Contexto:\n{specialty_context}\n\nExcerto:\n{text}',
            'fallback_models': ['databricks-claude-sonnet-4-5'],
            'max_tokens': 256,
        },
        'findings': {
            'lesao_focal': {
                'terms': ['nodulo hipervascular', 'lirads 4', 'lirads 5'],
            },
            'esteatose': {
                'terms': [
                    'degeneracao gordurosa figado', 'degeneração gordurosa figado',
                    'figado com ecogenicidade aumentada',
                ],
            },
            'colecao_hepatica': {
                'terms': ['infarto figado'],
            },
            'hepatomegalia': {
                'terms': ['hipertrofia lobo caudado'],
            },
            'dilatacao_biliar': {
                'terms': ['dilatacao vias-biliares', 'dilatação vias-biliares'],
            },
            'hepatopatia': {
                'terms': [
                    'baco aumentado', 'cavernoma porta', 'circulacao colateral',
                    'circulação colateral', 'cirrose', 'congestao passiva cronica figado',
                    'congestão passiva crônica figado', 'contorno irregular',
                    'doenca alcoolica figado', 'doença alcoólica figado', 'doenca hepatica',
                    'doença hepática', 'encefalopatia hepatica', 'encefalopatia hepática',
                    'esclerose hepatica', 'esclerose hepática', 'fibrose',
                    'fibrose esclerose alcoolicas', 'figado gorduroso alcoolico',
                    'figado gorduroso alcoólico', 'hepatite', 'hipertensao portal',
                    'hipertensão portal', 'hepatopatia cronica', 'hepatopatia crônica',
                    'hiperplasia nodular regenerativa', 'hiperpasia nodular regenerativa',
                    'lobulado', 'sindrome obstrucao sinusoidal hepatica',
                    'síndrome obstrução sinusoidal hepática', 'trombose veia porta',
                    'varizes esofagianas', 'varizes gastricas', 'varizes gástricas',
                    'veia porta dilatada', 'insuficiencia hepatica', 'insuficiência hepática',
                ],
                'regex': [
                    '\\bhiperplasia\\s+nodular\\s+regenerativa\\b',
                    '\\bhiperpasia\\s+nodular\\s+regenerativa\\b',
                ],
            },
        },
        'findings_policy': {
            'organ': {
                'max_chars': 220,
            },
        },
        'negation': {
            'phrases': ['ausencia', 'nao ha', 'não há', 'sem', 'ausencia de', 'ausência de'],
            'window': 3,
        },
    },
    'catalog': 'diamond_hepatologia',
    'data': {
        'gold_domains': ['exame.identificacao', 'exame.procedimento', 'exame.datas', 'exame.laudos'],
        'filters': {
            'gold_filter': {
                'keywords': ['figado', 'fígado', 'abd', 'bdo'],
                'mode': 'any',
            },
        },
        'column_map': {
            'id_exame': 'id_exame',
            'id_paciente': ['id_paciente', 'id_patient'],
            'id_unidade': 'id_unidade',
            'exm_laudo_texto': ['proced_laudo_exame_original', 'proced_laudo_exame'],
            'exm_mod': ['cod_procedimento', 'tp_codigo_procedimento'],
            'exm_tipo': 'proced_nome_exame',
            'dt_exame': 'dt_exame',
        },
        'legacy': {
            'enabled': True,
            'entrada_ref': 'diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_entrada_v2',
            'saida_ref': 'diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_saida',
        },
    },
    'runtime': {
        'profile': 'llm_http',
        'llm_router': {
            'enabled': True,
            'mode': 'llm',
            'api_key_env': 'DATABRICKS_TOKEN',
            'fallback_policy': 'positive_in_band',
            'json_response_format': False,
        },
    },
    'monitoring': {
        'metrics_table': '{catalog}.hepatologia.tb_diamond_mod_metricas_qualidade',
        'alert_threshold_relevance_drop': 0.15,
        'baseline_lookback_days': 30,
    },
    'distribution': {
        'nome_solucao': 'hepatologia',
        'estagio': 'homologacao',
        'outbound_volume': '/tmp/ia/distribuicao/hepatologia/{run_id}/',
        'control_tables': {
            'outbox': '{catalog}.hepatologia.tb_diamond_mod_hepatologia_entrega_saida',
            'inbox': '{catalog}.hepatologia.tb_diamond_mod_hepatologia_entrega_entrada',
        },
        'delivery': {
            'logic_app_secret_scope': 'fabrica-ia',
            'logic_app_secret_key': 'logic-app-delivery-url',
            'onedrive_user': '',
            'blob': {
                'dbfs_mount_base': '/mnt/trusted/datalake/ia/projetos/hepatologia/data/{env}/envio',
                'dbfs_mount_strip_prefix': '/mnt',
            },
        },
        'outputs': [
            {
                'name': 'saida_por_regional',
                'format': 'multi_file_xlsx',
                'datasets': ['saida'],
                'split_by': '_entrega_dir',
                'remote_subpath': 'Central_Captacao/',
            },
        ],
    },
}

import json  # noqa: E402

dbutils.notebook.exit(json.dumps(CONFIG, ensure_ascii=False))  # noqa: F821
