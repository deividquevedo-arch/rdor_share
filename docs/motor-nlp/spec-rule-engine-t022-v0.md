# SPEC — Rule engine S02 T02.2 (v0)

## Responsabilidade

Extrair mencoes de achados configuraveis sobre texto ja normalizado (`to_plain`), com suporte a multiplas estrategias de match e filtros espaciais, sem embeddings.

## Input

- `text: str` — texto plano pos-`to_plain`.
- `nlp_config` (sub-arvore `nlp`):
  - `findings: Mapping[str, Sequence[str]]` — categorias e frases (tokens consecutivos apos `norm`).
  - `findings_regex: Mapping[str, Sequence[str]]` opcional — padroes `re` por categoria (aplicados sobre a sentenca original).
  - `use_spacy_matcher: bool` opcional (default `true`) — ativa match via `Matcher` com regex tolerante a acentos por token.
  - `target_organs`, `organs`, `negation_phrases` / `negation_expressions`, `negation_window` — como antes.
  - `finding_organ_max_chars: int | null` opcional — se definido, exige mencao de orgao alvo a menos ou igual a N caracteres (gap minimo entre extremos do achado e do orgao).

## Output

- `summary_compact: list[str]`
- `n_positive_spans`, `n_negated_spans: int`
- `rule_engine_version: str` — versao da logica de agregacao de spans (telemetria).

## Invariantes

- Spans sempre dentro da mesma sentenca processada.
- Dedupe por `(categoria, start, end)` antes de aplicar negação/proximidade.
- Sem termos clinicos hardcoded fora do dict injetado.

## Edge cases

- Sentenca vazia; findings vazio; regex invalida (ignorar padrao com aviso silencioso em teste).
- Orgao sem `seeds`: usa nome do orgao como termo.
- `finding_organ_max_chars` com orgao nao encontrado na sentenca: rejeita o achado (comportamento conservador).

## O que NAO faz

- Segmentacao por cabecalhos de laudo (modulo `by_headers`).
- Embeddings / `semantic_expand`.
- Validacao de schema de saida do motor (T02.4 / monitoring).
