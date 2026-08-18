# Databricks notebook source
# MAGIC %md
# MAGIC # Config — Transplante de Pulmão (V1) · rastreio QUANTITATIVO de função pulmonar
# MAGIC
# MAGIC Especialidade **quantitativa pura**: a relevância vem de **limiares numéricos** em medidas de
# MAGIC função pulmonar (VEF1 / CVF / DLCO em % do previsto) — não de achados textuais. Usa a camada
# MAGIC `quantitative_criteria` do motor (`on_met: promote`): o LLM extrai só valor+unidade+evidência e o
# MAGIC **código** aplica o limiar (determinístico). Sem embeddings, sem llm-router de relevância.
# MAGIC
# MAGIC **Escopo V1:** DPOC (VEF1<30) + supurativas (VEF1<40 adulto / <50 se <18a, via `threshold_by`)
# MAGIC → cobertos por VEF1<40/<50; intersticiais (**CVF<70 OU DLCO<40**). Eco/cateterismo = V2.
# MAGIC
# MAGIC **Exige `nlp_engine >= 0.4.1`** (`extraction_hint` + `threshold_by`). O runner injeta o `base_url` do LLM p/
# MAGIC o extrator quantitativo mesmo em `rule_only` (perfil quantitativo-puro). SPEC:
# MAGIC `docs/motor-nlp/pulmao/spec-negocio-transplante-pulmao-v1.md`.

# COMMAND ----------

CONFIG = {
    "specialty_id": "transplante_pulmao",
    "config_version": "0.1.2-pulmao-failover",
    "model_version": "v0",
    "description": (
        "Rastreio de elegibilidade a Transplante de Pulmão a partir do resultado de exames de função "
        "pulmonar (espirometria / prova de função completa / difusão). V1 quantitativo: VEF1<40% "
        "previsto (DPOC+supurativa adulto) e intersticial (CVF<70% ou DLCO<40% previsto)."
    ),

    # --- Motor NLP: QUANTITATIVO puro (sem findings/embeddings/llm-router de relevância) -----------
    "nlp": {
        # Ordem coerente da cascata (evidencia dura antes do juiz) e default da lib desde 0.6.0 —
        # nao precisa mais declarar `pipeline_order`. Validado 1:1 vs legacy na base ouro
        # (TP126/FP1/FN0, recall 1,0 / MCC 0,996): pulmao e quantitativo puro, a ordem nao altera.
        "target_organs": ["pulmao"],
        "organs": {"pulmao": {"seeds": ["pulmão", "pulmao", "pulmonar", "respiratório"]}},
        "segmentation": {"mode": "full_doc"},  # laudos de função pulmonar são curtos
        "findings": {},  # quantitativo puro — nenhuma relevância por achado textual
        "negation_phrases": ["sem", "não há", "nao ha", "ausência de", "ausencia de"],
        "negation_window": 5,
        "score_policy_version": "v1_bins_legacy",
        "use_spacy_matcher": False,  # não há findings/matcher
        "feature_flags": {"calibrated_hybrid": False},
        "embeddings": {"use_embeddings": False},  # sem etapa semântica
        "text_pipeline": {"trailing_line_patterns": []},  # to_plain já limpa HTML/base64 (laudos wate)
        # llm_router de RELEVÂNCIA desligado; presente só p/ o extrator quantitativo herdar model/token.
        # O runner injeta base_url quando há quantitative_criteria (mesmo em rule_only).
        "llm_router": {
            "enabled": False,
            "mode": "llm",
            "model": "databricks-claude-haiku-4-5",
            "api_key_env": "DATABRICKS_TOKEN",
        },
        # Camada quantitativa: o LLM EXTRAI valor+unidade; o código aplica o limiar. on_met=promote
        # (a medida É a relevância — não há achado de regra a ancorar). anchor.text = gate de custo
        # (só chama o LLM se o termo aparece). extraction_hint = orientação de domínio (config-in):
        # % do previsto, valor entre colchetes nos laudos wate, sinônimos/nomes por extenso, e o que
        # NÃO confundir (relação VEF1/CVF, litros).
        "quantitative_criteria": {
            "funcao_vef1": {
                "description": (
                    "VEF1 (Volume Expiratório Forçado no 1º segundo), em % do PREVISTO/PREDITO. "
                    "Cobre DPOC (VEF1 < 30) e supurativa (VEF1 < 40 adulto / < 50 se < 18 anos)."
                ),
                "measure": {"name": "vef1_pct_previsto", "unit": "%"},
                "threshold": {"op": "<", "value": 40.0},  # default (adulto: DPOC<30 ⊂ supurativa<40)
                # supurativa pediátrica: < 18 anos -> VEF1 < 50%. Idade sai do próprio laudo ("N anos").
                # Sem idade no texto -> usa o default (40, conservador). Exige nlp_engine >= 0.4.1.
                "threshold_by": {
                    "source": {"regex": r"(\d{1,3})\s*anos", "cast": "int"},
                    "rules": [{"max": 17, "value": 50.0}],
                },
                "on_met": "promote",
                # FP-01: VEF1 % do previsto plausivel entre 5 e 200; fora disso o LLM leu litros/
                # relacao por engano -> descarta a medida (met=None, fail-safe) em vez de promover FP.
                "plausible_range": [5, 200],
                "anchor": {"text": r"(?i)\bvef\s?-?1\b|\bfev\s?-?1\b|volume expirat[óo]rio"},
                "extraction_hint": (
                    "Extraia o VEF1 em % do PREVISTO/PREDITO. O termo pode vir por SIGLA (VEF1, FEV1) "
                    "OU por EXTENSO — e MUITAS VEZES SÓ pelo nome, sem a sigla: 'Volume expiratório "
                    "(forçado) no 1º/primeiro segundo'. O valor costuma vir entre PARÊNTESES logo após "
                    "o nome: '...no 1º segundo (80% previsto)' => 80; '(VEF1 76% previsto)' => 76; nos "
                    "laudos wate pode vir entre COLCHETES: 'VEF1 [76]% previsto' => 76. **NÃO** use a "
                    "RELAÇÃO VEF1/CVF (ex.: 'Relação VEF1/CVF (0,81)' ou [69]) — isso é a razão, não o "
                    "VEF1%. NÃO use valor absoluto em LITROS. Só o percentual do previsto do VEF1. Se "
                    "não houver, found=false."
                ),
                "llm": {
                    "model": "databricks-claude-haiku-4-5",
                    # Failover de modelo (nlp_engine>=0.5.12): em 429 do haiku, tenta o sonnet antes
                    # de desistir — distribui carga em vez de esperar/serializar. max_tokens limita
                    # a resposta (só extrai valor+unidade+evidencia, JSON curto).
                    "fallback_models": ["databricks-claude-sonnet-4-5"],
                    "api_key_env": "DATABRICKS_TOKEN",
                    "temperature": 0,
                    "max_tokens": 256,
                },
            },
            "funcao_intersticial": {
                "description": (
                    "CVF (Capacidade Vital Forçada) e DLCO (Difusão do Monóxido de Carbono), em % do "
                    "PREVISTO/PREDITO."
                ),
                "measures": [
                    {"name": "cvf_pct_previsto", "unit": "%"},
                    {"name": "dlco_pct_previsto", "unit": "%"},
                ],
                "condition": {
                    "any_of": [
                        {"measure": "cvf_pct_previsto", "op": "<", "value": 70.0},
                        {"measure": "dlco_pct_previsto", "op": "<", "value": 40.0},
                    ]
                },
                "on_met": "promote",
                "anchor": {
                    "text": r"(?i)\bcvf\b|capacidade vital for[çc]ada|\bdlco\b|\bdco\b|difus[ãa]o (do )?mon[óo]xido"
                },
                "extraction_hint": (
                    "Extraia CVF e DLCO em % do PREVISTO/PREDITO. CVF = 'Capacidade Vital Forçada' "
                    "(sigla CVF); o valor vem em PARÊNTESES: '(CVF 78% previsto)' => 78 (ou [82] nos "
                    "wate). DLCO = 'Difusão do Monóxido de Carbono' (siglas DLCO/DCO): extraia SÓ de um "
                    "RESULTADO, ex.: 'Difusão de monóxido de carbono (DLCO 63% previsto)'. **IGNORE a "
                    "seção 'Equações de valores de referência'** — linhas como 'Difusão CO, não "
                    "corrigida para Hb: Guimarães, 2019' são CITAÇÃO de equação, NÃO resultado. NÃO "
                    "confunda CVF com Capacidade Pulmonar Total (CPT), Volume Residual (VR) nem VR/CPT "
                    "(são outras medidas). NÃO use litros/mL nem a relação VEF1/CVF. Se uma medida não "
                    "aparecer, found=false só para ela."
                ),
                "llm": {
                    "model": "databricks-claude-haiku-4-5",
                    # Failover de modelo (nlp_engine>=0.5.12): em 429 do haiku, tenta o sonnet antes
                    # de desistir — distribui carga em vez de esperar/serializar. max_tokens limita
                    # a resposta (só extrai valor+unidade+evidencia, JSON curto).
                    "fallback_models": ["databricks-claude-sonnet-4-5"],
                    "api_key_env": "DATABRICKS_TOKEN",
                    "temperature": 0,
                    "max_tokens": 256,
                },
            },
        },
        "emit_decision_trail": True,  # trilha (medida·valor·decisão·evidência) p/ a homologação
    },

    # --- Dados: Gold corporativa (gold_corporativo_ia) via data_manager (fonte_staging=motor_gold) ---
    # HML/dev: catálogo IA que TEM schema `workarea` (o runner grava as tabelas do motor em
    # `{catalog}.workarea.dev_*` no cluster dev). O catálogo DEDICADO `diamond_transplante_pulmao`
    # (padrão das demais especialidades) ainda NÃO foi provisionado — criar via infra (CREATE CATALOG
    # + schema + grants) para PRD e então trocar aqui.
    "catalog": "diamond_ia_hml",
    "data": {
        "gold_domains": ["exame.identificacao", "exame.procedimento", "exame.datas", "exame.laudos"],
        "filters": {
            # Pré-seleção SQL no Gold. O builder renderiza "{coluna} {expr}" com %value% substituído,
            # então o expr é SÓ o operador (não repetir a coluna). Case-insensitive via (?i) inline.
            # Lookbehind (?<!ergo) exclui a ergoespirometria isolada; imunodifusão não casa (exige
            # 'monóxido'). Resta ~2% do pacote "ergoespirometria ou teste cardiopulmonar (espirometria
            # forçada)" (casa pelo 'espirometria' interno) — tolerado no V1 (tem VEF1; refinar depois).
            # Precisão fina fica p/ gold_filter (texto do laudo) + anchor.text (regex por medida).
            "gold_query": {
                "proced_descricao": {
                    "value": "(?<!ergo)espiromet|prova (de )?fun[cç][aã]o pulmonar|difus[aã]o.*mon[oó]xido|volumes pulmonares por pletismog|resist.ncia.*vias a.reas por pletismog",
                    "expr": "rlike '(?i)%value%'",
                },
            },
            # SEM gold_filter: seria redundante com o anchor.text (que ja gateia por medida no motor) e
            # derrubava ~11% em junho — incl. ~80 laudos com conteudo mas sem a keyword exata (PFP
            # legitimos perdidos ANTES do motor). A selecao por PROCEDIMENTO fica no gold_query; a
            # presenca da MEDIDA fica no anchor.text (laudo vazio/sem medida -> skip, custo LLM zero).
        },
        # Espelha o mapeamento da hepatologia (mesma Gold corporativa). No motor_gold o LAUDO vem do
        # struct proced_lista_exames (extract_laudo), não do column_map — este cobre id/datas/tipo.
        "column_map": {
            "id_exame": "id_exame",
            "id_paciente": ["id_paciente", "id_patient"],
            "id_unidade": "id_unidade",
            "exm_laudo_texto": ["proced_laudo_exame_original", "proced_laudo_exame"],
            "exm_mod": ["cod_procedimento", "tp_codigo_procedimento"],
            "exm_tipo": "proced_nome_exame",
            "dt_exame": "dt_exame",
        },
        # Sem tabela legada: força fonte_staging=motor_gold (data_manager lê a Gold corporativa).
        "legacy": {"enabled": False},
    },

    # --- Runtime: rule_only (relevância só do quantitative_criteria) ------------------------------
    # O runner injeta base_url/token do LLM p/ o extrator quantitativo mesmo em rule_only.
    "runtime": {
        "profile": "rule_only",
        "llm_router": {
            "enabled": False,
            "mode": "llm",
            "api_key_env": "DATABRICKS_TOKEN",
        },
    },

    # --- Monitoração / distribuição (mínimo; opt-in) ----------------------------------------------
    "monitoring": {
        "metrics_table": "{catalog}.transplante_pulmao.tb_diamond_mod_metricas_qualidade",
        "alert_threshold_relevance_drop": 0.15,
        "baseline_lookback_days": 30,
    },
}

import json  # noqa: E402

dbutils.notebook.exit(json.dumps(CONFIG, ensure_ascii=False))  # noqa: F821
