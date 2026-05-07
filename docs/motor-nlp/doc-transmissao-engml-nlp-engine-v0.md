# Documento de transmissao EngML -- nlp_engine (baseline minima + objetivo final)

Objetivo: fornecer a estrutura real atual para a EngML provisionar a lib minima, com visao consolidada da evolucao DS ate o alvo final (rule-based + embeddings opcionais).

---

## Arvore consolidada da lib (fonte unica)

```text
plataform/nlp_engine/nlp_engine/
├─ __init__.py
├─ engine.py
├─ scoring.py
├─ config_loader.py
├─ quality_guard.py          # planejado (S02)
├─ semantic_expand.py        # opcional (Fase 2 / embeddings)
└─ text_pipeline/
   ├─ __init__.py
   ├─ to_plain.py
   ├─ by_headers.py
   ├─ negation.py
   ├─ boilerplate.py         # S01 T01.4 (drop_boilerplate_lines, strip_trailing_line_patterns)
   ├─ anchors.py             # S01 T01.2 (organ_anchors, segment_by_organs; extra [spacy])
   ├─ norm.py
   ├─ footer.py
   ├─ html_plain.py
   └─ rtf_fallback.py
```

Observacao: `text_pipeline` e um **pacote** (`text_pipeline/`), nao arquivo unico.  
Legenda: `planejado` = proxima evolucao mapeada; `opcional` = fase 2.

---

## Progresso planejado (anexo03)

### Feito (S01)
- **T01.1** `to_plain`: RTF/HTML/plain.
- **T01.2** cabecalhos (`by_headers`) + **ancoras** `anchors.py` (`organ_anchors`, `segment_by_organs`, `sentence_mentions_organ`; extra `[spacy]`, paridade DII/colon).
- **T01.3** negacao em texto plano (tokens + janela via dict/YAML).
- **T01.4** `boilerplate`: linhas RTF/HTML/avisos + tail opcional por regex (`trailing_line_patterns` em `to_plain`).
- **T01.5** testes com laudos sinteticos plain/HTML/RTF, com e sem negacao (`tests/text_pipeline/test_s01_t015_synthetic_laudos.py`).

### Proximo imediato (S01)
- **S01** TextPipeline com T01.1-T01.5 e ancoras entregues na lib; evoluir segmentacao/motor por nova historia/task.

### Na sequencia
- **S02**: `ClinicalNlpEngine` (interface/process), rule-based e scoring.
- **S03**: schema YAML, `config_loader` com validacao e merge (`shared/organs` + specialty).

---

## Objetivo final da lib (visao consolidada)

### Fase 1 (MVP rule-based)
- `text_pipeline`: limpeza + segmentacao + negacao compartilhadas.
- `engine.py`: matching rule-based com config por especialidade.
- `scoring.py`: `confidence_score` continuo (0.0-1.0).
- `quality_guard.py` (planejado): validacao de contrato de saida.

### Fase 2 (evolucao opcional com embeddings)
- `semantic_expand.py`: expansao semantica controlada por flag no config.
- Uso de embeddings para ampliar recall sem quebrar fluxo rule-based.
- Regra de produto: ativacao gradual por especialidade e validacao contra baseline.

### Contrato funcional alvo (DS)
- Entrada principal: texto bruto + metadados + `config["nlp"]` (dict-in).
- Saida: `fl_relevante`, `confidence_score`, `exm_laudo_resultado`, `config_version`, `engine_version`, `specialty_id`.
- Notebook continua como composition root; lib concentra logica de NLP.

---

## Minimo necessario para provisionar

- Provisionar esta estrutura de **modulos de negocio** como baseline da lib.
- Config de negocio e **dict-in** (YAML e lido fora da lib, no notebook).
- Detalhes de empacotamento/ambiente (`pyproject`, README, venv, CI, deploy) ficam com EngML/MLOps.
