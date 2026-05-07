(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> .venv\Scripts\python.exe scripts\gen_synthetic_motor_export_csv.py
Escrito: C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine\_local_samples\exports\demo_motor_export_sintetico.csv (3 linhas)
(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> .venv\Scripts\python.exe scripts\demo_engine_e2e.py --csv _local_samples\exports\demo_motor_export_sintetico.csv
========================================================================
MODO CSV — _local_samples\exports\demo_motor_export_sintetico.csv (3 linha(s), config fixa para demo bexiga/calculos)
========================================================================

========================================================================
LINHA 1/3 — id_exame=SYN-EXP-001
========================================================================
========================================================================
PASSO 1 — Linha de entrada (contrato data_manage -> motor)
========================================================================
{
  "id_exame": "SYN-EXP-001",
  "id_paciente": "P-SYN-001",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "Bexiga com paredes de espessura habitual no exame sintetico.",
  "exm_mod": "US",
  "exm_tipo": "pelvica",
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
  "id_exame": "SYN-EXP-001",
  "id_paciente": "P-SYN-001",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "Bexiga com paredes de espessura habitual no exame sintetico.",
  "exm_mod": "US",
  "exm_tipo": "pelvica",
  "dt_exame": "2026-04-14",
  "id_predicao": "62abc734-80b9-4812-975c-aaee7389dc80",
  "dt_execucao": "2026-04-14T19:32:16.718491+00:00",
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
LINHA 2/3 — id_exame=SYN-EXP-002
========================================================================
========================================================================
PASSO 1 — Linha de entrada (contrato data_manage -> motor)
========================================================================
{
  "id_exame": "SYN-EXP-002",
  "id_paciente": "P-SYN-002",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "Bexiga sem calculos radiopacos no estudo sintetico.",
  "exm_mod": "US",
  "exm_tipo": "pelvica",
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
  "id_exame": "SYN-EXP-002",
  "id_paciente": "P-SYN-002",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "Bexiga sem calculos radiopacos no estudo sintetico.",
  "exm_mod": "US",
  "exm_tipo": "pelvica",
  "dt_exame": "2026-04-14",
  "id_predicao": "9296b3da-634e-418b-aaf6-4d670f6672ce",
  "dt_execucao": "2026-04-14T19:32:16.725045+00:00",
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

========================================================================
LINHA 3/3 — id_exame=SYN-EXP-003
========================================================================
========================================================================
PASSO 1 — Linha de entrada (contrato data_manage -> motor)
========================================================================
{
  "id_exame": "SYN-EXP-003",
  "id_paciente": "P-SYN-003",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "<p>Bexiga distendida.</p><p>Observa-se calculos no interior da bexiga.</p>",
  "exm_mod": "CT",
  "exm_tipo": "abdome",
  "dt_exame": "2026-04-14"
}

========================================================================
PASSO 2 — Texto bruto extraido (`exm_laudo_texto` ou fallback `Laudo`)
========================================================================
'<p>Bexiga distendida.</p><p>Observa-se calculos no interior da bexiga.</p>'

========================================================================
PASSO 3 — `to_plain` (TextPipeline): HTML/RTF/plain -> texto limpo
========================================================================
trailing_line_patterns: None
--- texto tratado ---
Bexiga distendida.
Observa-se calculos no interior da bexiga.

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
    "calculos: calculos"
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
  "id_exame": "SYN-EXP-003",
  "id_paciente": "P-SYN-003",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "<p>Bexiga distendida.</p><p>Observa-se calculos no interior da bexiga.</p>",
  "exm_mod": "CT",
  "exm_tipo": "abdome",
  "dt_exame": "2026-04-14",
  "id_predicao": "3bd1c111-1366-4b1c-aaeb-0881d5ae65d1",
  "dt_execucao": "2026-04-14T19:32:16.735160+00:00",
  "specialty_id": "csv_demo",
  "config_version": "1.0.0-csv-demo",
  "engine_version": "demo-9.9.9",
  "fl_relevante": 1,
  "confidence_score": 0.9,
  "exm_laudo_resultado": "{\"summary_compact\": [\"calculos: calculos\"], \"n_positive_spans\": 1, \"n_negated_spans\": 0, \"rule_engine_version\": \"t022_v1\", \"score_policy_version\": \"v1_bins_legacy\"}",
  "exm_laudo_texto_tratado": "Bexiga distendida.\nObserva-se calculos no interior da bexiga."
}

========================================================================
FIM — `exm_laudo_resultado` (JSON parseado)
========================================================================
{
  "summary_compact": [
    "calculos: calculos"
  ],
  "n_positive_spans": 1,
  "n_negated_spans": 0,
  "rule_engine_version": "t022_v1",
  "score_policy_version": "v1_bins_legacy"
}
(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> 