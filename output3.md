(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> .venv\Scripts\python.exe scripts\demo_engine_e2e.py --csv tests\fixtures\hml_parity_minimal.csv
========================================================================
MODO CSV — tests\fixtures\hml_parity_minimal.csv (2 linha(s), config fixa para demo bexiga/calculos)
========================================================================

========================================================================
LINHA 1/2 — id_exame=SYN1
========================================================================
========================================================================
PASSO 1 — Linha de entrada (contrato data_manage -> motor)
========================================================================
{
  "id_exame": "SYN1",
  "id_paciente": "P-SYN-CSV",
  "id_unidade": "U-SYN-CSV",
  "exm_laudo_texto": "Bexiga com paredes de espessura habitual no exame sintetico.",
  "exm_mod": "SYN",
  "exm_tipo": "csv_fixture",
  "dt_exame": "2026-04-14"
}

========================================================================
PASSO 2 — Texto bruto extraido (`exm_laudo_texto` ou fallback `Laudo`)
========================================================================
'Bexiga com paredes de espessura habitual no exame sintetico.'

========================================================================
PASSO 3 — `to_plain` (TextPipeline): HTML/RTF/plain -> texto limpo
========================================================================
trailing_line_patterns: None
--- texto tratado ---
Bexiga com paredes de espessura habitual no exame sintetico.

========================================================================
PASSO 4 — `process_rule_based` (S02 T02.2): achados + negação + orgaos
========================================================================
nlp_config: {
  "findings": {
    "calculos": [
      "calculos"
    ],
    "parede": [
      "paredes"
    ]
  },
  "target_organs": [
    "bexiga"
  ],
  "organs": {
    "bexiga": {
      "seeds": [
        "bexiga"
      ]
    }
  },
  "negation_phrases": [
    "sem"
  ],
  "negation_window": 7,
  "score_policy_version": "v1_bins_legacy"
}
{
  "summary_compact": [
    "parede: paredes"
  ],
  "n_positive_spans": 1,
  "n_negated_spans": 0,
  "rule_engine_version": "t022_v1"
}

========================================================================
PASSO 5 — Scoring (S02 T02.3)
========================================================================
score_policy_version: v1_bins_legacy
n_positive_spans=1, n_negated_spans=0
confidence_score=0.9, fl_relevante=1

========================================================================
PASSO 6 — `ClinicalNlpEngine.process` (saida contrato completa)
========================================================================
{
  "id_exame": "SYN1",
  "id_paciente": "P-SYN-CSV",
  "id_unidade": "U-SYN-CSV",
  "exm_laudo_texto": "Bexiga com paredes de espessura habitual no exame sintetico.",
  "exm_mod": "SYN",
  "exm_tipo": "csv_fixture",
  "dt_exame": "2026-04-14",
  "id_predicao": "b883d439-5e02-4e2b-a0a5-528e07e8cbd2",
  "dt_execucao": "2026-04-14T19:29:42.180059+00:00",
  "specialty_id": "csv_demo",
  "config_version": "1.0.0-csv-demo",
  "engine_version": "demo-9.9.9",
  "fl_relevante": 1,
  "confidence_score": 0.9,
  "exm_laudo_resultado": "{\"summary_compact\": [\"parede: paredes\"], \"n_positive_spans\": 1, \"n_negated_spans\": 0, \"rule_engine_version\": \"t022_v1\", \"score_policy_version\": \"v1_bins_legacy\"}",
  "exm_laudo_texto_tratado": "Bexiga com paredes de espessura habitual no exame sintetico."
}

========================================================================
FIM — `exm_laudo_resultado` (JSON parseado)
========================================================================
{
  "summary_compact": [
    "parede: paredes"
  ],
  "n_positive_spans": 1,
  "n_negated_spans": 0,
  "rule_engine_version": "t022_v1",
  "score_policy_version": "v1_bins_legacy"
}

========================================================================
LINHA 2/2 — id_exame=SYN2
========================================================================
========================================================================
PASSO 1 — Linha de entrada (contrato data_manage -> motor)
========================================================================
{
  "id_exame": "SYN2",
  "id_paciente": "P-SYN-CSV",
  "id_unidade": "U-SYN-CSV",
  "exm_laudo_texto": "Bexiga sem calculos radiopacos no estudo sintetico.",
  "exm_mod": "SYN",
  "exm_tipo": "csv_fixture",
  "dt_exame": "2026-04-14"
}

========================================================================
PASSO 2 — Texto bruto extraido (`exm_laudo_texto` ou fallback `Laudo`)
========================================================================
'Bexiga sem calculos radiopacos no estudo sintetico.'

========================================================================
PASSO 3 — `to_plain` (TextPipeline): HTML/RTF/plain -> texto limpo
========================================================================
trailing_line_patterns: None
--- texto tratado ---
Bexiga sem calculos radiopacos no estudo sintetico.

========================================================================
PASSO 4 — `process_rule_based` (S02 T02.2): achados + negação + orgaos
========================================================================
nlp_config: {
  "findings": {
    "calculos": [
      "calculos"
    ],
    "parede": [
      "paredes"
    ]
  },
  "target_organs": [
    "bexiga"
  ],
  "organs": {
    "bexiga": {
      "seeds": [
        "bexiga"
      ]
    }
  },
  "negation_phrases": [
    "sem"
  ],
  "negation_window": 7,
  "score_policy_version": "v1_bins_legacy"
}
{
  "summary_compact": [],
  "n_positive_spans": 0,
  "n_negated_spans": 1,
  "rule_engine_version": "t022_v1"
}

========================================================================
PASSO 5 — Scoring (S02 T02.3)
========================================================================
score_policy_version: v1_bins_legacy
n_positive_spans=0, n_negated_spans=1
confidence_score=0.35, fl_relevante=0

========================================================================
PASSO 6 — `ClinicalNlpEngine.process` (saida contrato completa)
========================================================================
{
  "id_exame": "SYN2",
  "id_paciente": "P-SYN-CSV",
  "id_unidade": "U-SYN-CSV",
  "exm_laudo_texto": "Bexiga sem calculos radiopacos no estudo sintetico.",
  "exm_mod": "SYN",
  "exm_tipo": "csv_fixture",
  "dt_exame": "2026-04-14",
  "id_predicao": "6f922d94-7c51-4250-af7a-55545b9a403c",
  "dt_execucao": "2026-04-14T19:29:42.188057+00:00",
  "specialty_id": "csv_demo",
  "config_version": "1.0.0-csv-demo",
  "engine_version": "demo-9.9.9",
  "fl_relevante": 0,
  "confidence_score": 0.35,
  "exm_laudo_resultado": "{\"summary_compact\": [], \"n_positive_spans\": 0, \"n_negated_spans\": 1, \"rule_engine_version\": \"t022_v1\", \"score_policy_version\": \"v1_bins_legacy\"}",
  "exm_laudo_texto_tratado": "Bexiga sem calculos radiopacos no estudo sintetico."
}

========================================================================
FIM — `exm_laudo_resultado` (JSON parseado)
========================================================================
{
  "summary_compact": [],
  "n_positive_spans": 0,
  "n_negated_spans": 1,
  "rule_engine_version": "t022_v1",
  "score_policy_version": "v1_bins_legacy"
}
(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> 