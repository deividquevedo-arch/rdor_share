# SPEC — Portagem evolução motor `nlp_engine` → `fabrica_ia.nlp_engine` (lib)

**Backlog:** S02 (T02.1–T02.5, T02.4a), alinhado a [anexo03-historias-e-tasks-v0.md](anexo03-historias-e-tasks-v0.md).  
**Contrato:** [doc-contrato-engine-rule-based-v0.md](doc-contrato-engine-rule-based-v0.md).  
**Validação:** [doc-validacao-paridade-databricks-v0.md](doc-validacao-paridade-databricks-v0.md).

## 1. Objectivo

Fundir a lógica consolidada em `plataform/nlp_engine/nlp_engine/` na lib [fabrica-ia-lib/src/fabrica_ia/nlp_engine/](../../fabrica-ia-lib/src/fabrica_ia/nlp_engine/) **sem sobrescrever à cego** o que já foi validado no Databricks: preservar `import_module`/paths DBX onde existirem, alinhar contrato S02 (`process(rows, nlp_config, …)`), invariantes completos, LLM via serving DBX (`model` no YAML; URL/token no composition root).

## 2. Inputs (motor)

- `rows`: `Sequence[Mapping[str, Any]]` com pelo menos `exm_laudo_texto` ou `Laudo` por linha; campos opcionais repassados conforme contrato.
- `nlp_config`: `Mapping[str, Any]` — secção `nlp` (dict-in) com `findings`, `target_organs`, `segmentation`, `feature_flags`, `llm_router`, `embeddings`, etc., conforme doc-contrato.
- `specialty_id`, `config_version`: metadados de linha de saída.

## 3. Outputs

- `list[dict[str, Any]]` por linha: colunas obrigatórias + `exm_laudo_resultado` JSON com payload completo (incl. `decision_source`, semântica, LLM meta quando aplicável).

## 4. Casos limite

- Texto vazio em ambos os campos de laudo.
- `feature_flags.rule_engine` falso → saída com `decision_source: disabled` e contagens zero.
- LLM desligado ou fora da banda → não chama rede; `llm_called: false`.
- RTF/HTML: dependência de ambiente (Pandoc/striprtf) — smoke DBX.

## 5. Fora de âmbito desta entrega

- `data_manager` completo (Diamond entrada/saída), serving, T02.4b em `monitoring`.
- Adapter `process(text, metadata)` na lib (legados ficam fora do pacote novo).
- PHI em testes ou repositório.

## 6. Critérios de aceite

- `pytest` na raiz `fabrica-ia-lib` verde para `tests/nlp_engine` e imports.
- `PlatformOrchestrator` delega contrato S02 (sem API legacy).
- `validate_exm_laudo_resultado_json` alinhado ao contrato v0 (campos obrigatórios do doc-contrato).
