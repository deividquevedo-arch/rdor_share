# Matriz legado vs motor + registo de gaps (v0)

**Objetivo:** alinhar expectativas entre o que os notebooks Grupo 1a/1b/2 fazem ([02-analise-profunda-engines-nlp-v0.md](../02-analise-profunda-engines-nlp-v0.md)) e o que a Fase 1 em [`nlp_engine`](../../plataform/nlp_engine/) cobre, para **superar o legado sem regressao** (evidencia numerica + S06/S06b).

**Legenda paridade:** **P** paridade quando YAML/amostra alinhados; **G** gap com task/owner; **F2** depende da Fase 2 (S09+).

---

## Matriz por dimensao

| Dimensao | Legado (resumo) | Motor Fase 1 (`nlp_engine`) | Paridade |
|----------|-----------------|-----------------------------|----------|
| Entrada canónica | Varias colunas / wide schema | `id_exame`, `exm_laudo_texto`, … (contrato) | **P** (apos `data_manage`) |
| Limpeza RTF/HTML/plain | `to_plain`, ftfy, boilerplate | `to_plain` + `text_pipeline.trailing_line_patterns` no dict | **P** / **G** se gold HML desalinhado |
| Segmentacao anatomica | Blocos spaCy / headers / ancora | `ClinicalNlpEngine.process`: headers -> ancora -> full_doc (T01.6) | **P** com YAML alinhado; ver [doc-contrato-engine-rule-based-v0.md](../doc-contrato-engine-rule-based-v0.md) |
| Lista negacao | 2 a ~30 expressoes inline | `negation_phrases` + `negation_window` no YAML | **P** se YAML migrado; senao **G** |
| Matcher / regex achados | spaCy + `_accent_rx` | Matcher + `findings` / `findings_regex` | **P** com mesma lista |
| Proximidade orgao–achado | ~200 chars, regras 1b | `finding_organ_max_chars` opcional | **P** se configurado |
| Embeddings / `semantic_expand` | Grupo 1b/2 | Fora do pacote ate S09 | **F2** — Gap-002 |
| Scoring | ex. 0.9 / 0.35 (1b) | `score_policy_version`, `confidence_rule_based` | **P** com policy + testes |
| `fl_relevante` | Resumo nao vazio / regras | `fl_relevante_from_counts(n_positive)` | **P** com SPEC alinhada |
| Schema saida motor | JSON + muitas colunas PHI | Contrato minimo [doc-contrato-runtime-config-especialidade-v0.md](../doc-contrato-runtime-config-especialidade-v0.md) | **P** (desenho alvo) |
| Invariantes / schema Delta | Implicito / disperso | `output_invariants` + JSON minimo; composition `--validate-output-invariants`; T02.4b `monitoring` pendente | **P** local; **G** Delta completo — Gap-003 |

---

## Registo de gaps (Gap_ID)

| Gap_ID | Descricao | Historia / task (anexo03) | Bloqueador |
|--------|-----------|---------------------------|------------|
| **Gap-001** | Segmentacao T01.2 encadeada em `ClinicalNlpEngine.process` (fechado na lib; monitorar regressoes) | **T01.6** | Evidencia S01/S02 em amostra; regressoes via S06 |
| **Gap-002** | Ausencia de `semantic_expand` / SentenceTransformer | **S09** (Fase 2) | Sim para paridade 1b “completa” sem rule-only |
| **Gap-003** | Schema completo Delta / KPI vs invariantes locais + composition | **T02.4b** lib `monitoring` (wheel); ate la: [doc-quality-guard-t024b-composition-v0.md](../doc-quality-guard-t024b-composition-v0.md) | Operacional ate EngML fechar |
| **Gap-004** | Metricas P/R/F1 com gold (T04.3) nao implementadas na lib | **S04** T04.3 | Nao para MVP rule-based |
| **Gap-005** | `shared/organs.yaml` + YAMLs por especialidade fora do repo unico | **S03** T03.5/T03.6 + repos plataforma | Config |
| **Gap-006** | Paridade engine vs coluna gold em CSV Diamond | **S06** / **S06b** T06.x | Dados + processo |

---

## Criterio “superar legado sem regressao”

1. **Medir:** taxa S01 + auditoria S02 (scripts e nota [s01-s02-paridade-evidencia-v0.md](s01-s02-paridade-evidencia-v0.md)).
2. **Classificar:** toda divergencia motor vs baseline como melhoria / regressao / gap de escopo (S06b T06.7).
3. **Governar:** mudanca de lista/threshold apenas com fluxo **S08**.
4. **Nao regressar:** nenhuma mudanca que piore metrica acordada sem aceite explicito.

---

## Snapshot de gate Grupo 1b (2026-04-20)

- Baseline de motor: `pytest tests -q --ignore=tests/local` = **86 passed**.
- Colon/DII: **ok** na evidencia registada (paridade 1.0000 com `--parity-relaxed`).
- Demais especialidades Grupo 1b: **bloqueadas por indisponibilidade de amostra local** nesta corrida; nao caracterizar como regressao funcional sem dado.
