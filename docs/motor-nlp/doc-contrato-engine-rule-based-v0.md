# Contrato minimo — motor rule-based (`ClinicalNlpEngine`) v0

**Historia:** S02 (T02.1–T02.5, T02.4a local). **Relacionado:** [doc-contrato-runtime-config-especialidade-v0.md](doc-contrato-runtime-config-especialidade-v0.md), [notas/t02-4-quality-guard-monitoring-v0.md](notas/t02-4-quality-guard-monitoring-v0.md).

**Objetivo:** especificar entrada, dict `nlp` e saida para testes, invariantes e validacao em lote, sem PHI.

---

## 1) Linha de entrada (`process(rows, nlp_config, ...)`)

Cada `row` e um `Mapping` com pelo menos uma fonte de texto:

| Chave | Obrigatorio | Notas |
|-------|-------------|--------|
| `exm_laudo_texto` | Condicional | Se ausente ou so espacos, usa-se `Laudo`. |
| `Laudo` | Fallback | Usado quando `exm_laudo_texto` vazio. |
| `id_exame`, `id_paciente`, `id_unidade`, `exm_mod`, `exm_tipo`, `dt_exame` | Nao | Repassados se presentes (`_INPUT_PASS_THROUGH`). O motor **nao** valida formato de datas nem tipos clinicos. |

**O motor nao faz:** validacao de schema wide table, join com tabelas externas, ou rejeicao por campo opcional ausente (exceto texto vazio em ambos os campos de laudo).

---

## 2) Dict `nlp` (rule-based — campos relevantes)

Todos opcionais salvo onde a logica exige lista nao vazia para ter achados.

| Chave | Tipo esperado | Uso |
|-------|----------------|-----|
| `findings` | `dict[str, list[str]]` | Termos por categoria de achado. |
| `findings_regex` | `dict[str, list[str]]` | Regex por categoria (opcional). |
| `target_organs` | `list[str]` | Filtra mencao a orgao; segmentacao usa quando definido. |
| `organs` / `all_organs` | `dict` | Config por orgao (`seeds`, `regex`); merge via `config_loader.merge_with_shared_organs`. |
| `header_aliases` | `dict[str, Iterable[str]]` | Aliases para `segment_by_headers_plain`. |
| `segmentation` | `dict` | `mode`: `auto` \| `headers_only` \| `anchors_only` \| `full_doc`; `force_full_doc_for`: lista de orgaos. |
| `negation_phrases` / `negation_expressions` | `list[str]` | Lista de negação. |
| `negation_window` | `int` | Janela em tokens (default 7). |
| `finding_organ_max_chars` | `int` \| omit | Proximidade orgao–achado. |
| `score_policy_version` | `str` | `v1_bins_legacy` (default) ou `v2_density`. |
| `use_spacy_matcher` | `bool` | Default ligado. |
| `feature_flags.rule_engine` | `bool` | Desliga rule-based se `false`. |
| `text_pipeline.trailing_line_patterns` | `list[str]` | Padroes de linha final removidos no `to_plain`. |

**Governanca:** listas clinicas e thresholds vêm do YAML por especialidade (S03); nao hardcode no codigo.

---

## 3) Linha de saida + JSON `exm_laudo_resultado`

### Colunas de linha (obrigatorias para invariantes locais)

`id_predicao`, `dt_execucao`, `specialty_id`, `config_version`, `engine_version`, `fl_relevante` (0 ou 1), `confidence_score` ([0,1]), `exm_laudo_resultado` (string JSON nao vazia), `exm_laudo_texto_tratado`.

Validacao: [`output_invariants.validate_engine_output_row`](../../plataform/nlp_engine/nlp_engine/output_invariants.py).

### Objeto JSON em `exm_laudo_resultado` (minimo)

| Campo | Tipo | Obrigatorio | Notas |
|-------|------|-------------|--------|
| `summary_compact` | `list[str]` | Sim | Evidencias textuais agregadas. |
| `n_positive_spans` | `int` | Sim | >= 0 |
| `n_negated_spans` | `int` | Sim | >= 0 |
| `rule_engine_version` | `str` | Sim | Vazio se `feature_flags.rule_engine` desligado. |
| `score_policy_version` | `str` | Sim | Ex.: `v1_bins_legacy` |
| `segmentation_strategy` | `str` | Condicional | Presente quando rule-based ligado e segmentacao aplicada: `headers` \| `anchors` \| `full_doc` |

Validacao estrutural: `validate_exm_laudo_resultado_json` no mesmo modulo.

**Versoes:** alteracoes incompativeis no JSON exigem bump de `engine_version` / `config_version` e registo em S06/S06b.

---

## 4) T02.4b (schema completo / Delta)

Invariantes locais ficam no `nlp_engine`. Schema completo ou validacao Delta/KPI fica na lib **`monitoring`** ou no **Composition Root** ate existir pacote dedicado — ver [doc-quality-guard-t024b-composition-v0.md](doc-quality-guard-t024b-composition-v0.md) (gate opcional: `--validate-output-invariants` nos scripts de auditoria).

---

## 5) Compatibilidade

Mudancas aditivas (novos campos opcionais no JSON) sao permitidas com semver menor da lib. Remocao ou mudanca de tipo de campo obrigatorio exige major e alinhamento com consumidores.
