(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> .venv\Scripts\python.exe scripts\demo_engine_e2e.py --scenario broad

========================================================================
CENARIO — broad
========================================================================
========================================================================
PASSO 1 — Linha de entrada (contrato data_manage -> motor)
========================================================================
{
  "id_exame": "EX-SYN-BROAD-001",
  "id_paciente": "P-SYN-BROAD",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "<p><strong>Tecnica</strong> TC abdome com contraste — cenario sintetico.</p><p>Colon: evidencia-se lesao sesil na curvatura hepatica, 6 mm.</p><p>Reto: sem polipo radiologico no segmento distal estudado.</p><p>Figado e vias biliares sem alteracoes agudas no trecho sintetico.</p><p>Medico responsavel</p>",
  "exm_mod": "CT",
  "exm_tipo": "abdome",
  "dt_exame": "2026-04-14"
}

========================================================================
PASSO 2 — Texto bruto extraido (`exm_laudo_texto` ou fallback `Laudo`)
========================================================================
'<p><strong>Tecnica</strong> TC abdome com contraste — cenario sintetico.</p><p>Colon: evidencia-se lesao sesil na curvatura hepatica, 6 mm.</p><p>Reto: sem polipo radiologico no segmento distal estudado.</p><p>Figado e vias biliares sem alteracoes agudas no trecho sintetico.</p><p>Medico responsavel</p>'

========================================================================
PASSO 3 — `to_plain` (TextPipeline): HTML/RTF/plain -> texto limpo
========================================================================
trailing_line_patterns: ['(?i)^\\s*medico\\s+responsavel\\s*$']
--- texto tratado ---
Tecnica TC abdome com contraste — cenario sintetico.
Colon: evidencia-se lesao sesil na curvatura hepatica, 6 mm.
Reto: sem polipo radiologico no segmento distal estudado.
Figado e vias biliares sem alteracoes agudas no trecho sintetico.

========================================================================
PASSO 4 — `process_rule_based` (S02 T02.2): achados + negação + orgaos
========================================================================
nlp_config: {
  "findings": {
    "lesao": [
      "lesao",
      "nodulo",
      "polipo",
      "massa"
    ]
  },
  "target_organs": [
    "colon",
    "reto"
  ],
  "organs": {
    "colon": {
      "seeds": [
        "colon",
        "colico"
      ]
    },
    "reto": {
      "seeds": [
        "reto",
        "retoide"
      ]
    }
  },
  "negation_phrases": [
    "sem"
  ],
  "negation_window": 7,
  "text_pipeline": {
    "trailing_line_patterns": [
      "(?i)^\\s*medico\\s+responsavel\\s*$"
    ]
  },
  "score_policy_version": "v1_bins_legacy"
}
{
  "summary_compact": [
    "lesao: lesao"
  ],
  "n_positive_spans": 1,
  "n_negated_spans": 1,
  "rule_engine_version": "t022_v1"
}

========================================================================
PASSO 5 — Scoring (S02 T02.3)
========================================================================
score_policy_version: v1_bins_legacy
n_positive_spans=1, n_negated_spans=1
confidence_score=0.9, fl_relevante=1

========================================================================
PASSO 6 — `ClinicalNlpEngine.process` (saida contrato completa)
========================================================================
{
  "id_exame": "EX-SYN-BROAD-001",
  "id_paciente": "P-SYN-BROAD",
  "id_unidade": "U-SYN-01",
  "exm_laudo_texto": "<p><strong>Tecnica</strong> TC abdome com contraste — cenario sintetico.</p><p>Colon: evidencia-se lesao sesil na curvatura hepatica, 6 mm.</p><p>Reto: sem polipo radiologico no segmento distal estudado.</p><p>Figado e vias biliares sem alteracoes agudas no trecho sintetico.</p><p>Medico responsavel</p>",
  "exm_mod": "CT",
  "exm_tipo": "abdome",
  "dt_exame": "2026-04-14",
  "id_predicao": "bf19195b-5d9c-4c28-9852-9fcaa2853334",
  "dt_execucao": "2026-04-14T19:25:12.510091+00:00",
  "specialty_id": "colon",
  "config_version": "1.0.0-demo-broad",
  "engine_version": "demo-9.9.9",
  "fl_relevante": 1,
  "confidence_score": 0.9,
  "exm_laudo_resultado": "{\"summary_compact\": [\"lesao: lesao\"], \"n_positive_spans\": 1, \"n_negated_spans\": 1, \"rule_engine_version\": \"t022_v1\", \"score_policy_version\": \"v1_bins_legacy\"}",
  "exm_laudo_texto_tratado": "Tecnica TC abdome com contraste — cenario sintetico.\nColon: evidencia-se lesao sesil na curvatura hepatica, 6 mm.\nReto: sem polipo radiologico no segmento distal estudado.\nFigado e vias biliares sem alteracoes agudas no trecho sintetico."
}

========================================================================
FIM — `exm_laudo_resultado` (JSON parseado)
========================================================================
{
  "summary_compact": [
    "lesao: lesao"
  ],
  "n_positive_spans": 1,
  "n_negated_spans": 1,
  "rule_engine_version": "t022_v1",
  "score_policy_version": "v1_bins_legacy"
}
(.venv) PS C:\Users\deivid.quevedo_a3dat\Desktop\Rede D'Or\Projects\plataform\nlp_engine> 