# Proposta — Config Schema v2 (simplificação estrutural)

> Etapa 3 da simplificação da config. Objetivo: **reduzir a complexidade do schema** (consolidar
> blocos soltos por afinidade), não só a verbosidade (comentários — já resolvido). Byte-compat via
> camada de normalização no `config_loader`: aceita o formato antigo (v1) E o novo (v2); o motor não
> muda. Base: as 3 configs de negócio (hepato, tireoide, pulmão) na `branch-from-versao-alpha`.

## Princípio
Uma mudança **aditiva e reversível**: `config_loader.normalize_config(nlp)` traduz v2 → forma interna
que o motor já lê. Nenhum config quebra; migrar é opcional e gradual. Gate: golden 3/3 + os 3 configs
atuais carregam idênticos.

## Mapa dos campos `nlp` (uso nas 3 configs)
| campo hoje | hep | ti | pu | grupo |
|---|---|---|---|---|
| findings | ✅ | ✅ | ✅ | **findings** |
| findings_regex | ✅ | ✅ | | **findings** |
| findings_exclusion_terms | | ✅ | | **findings** |
| findings_ignore_sections | | ✅ | | **findings** |
| findings_skip_organ_gate | | ✅ | | **findings** |
| finding_organ_max_chars | ✅ | ✅ | | **findings** |
| finding_organ_scope | | ✅ | | **findings** |
| negation_phrases | ✅ | ✅ | ✅ | **negation** |
| negation_window | ✅ | ✅ | ✅ | **negation** |
| negation_direction | | ✅ | | **negation** |
| emit_decision_trail | | ✅ | ✅ | **feature_flags** |
| use_spacy_matcher | ✅ | ✅ | ✅ | **feature_flags** |
| organs / target_organs / shared_organs_path | ✅ | ✅ | ✅ | **organs** (opcional) |
| embeddings, llm_router, rads_extraction, quantitative_criteria, document_vet, segmentation, text_pipeline, score_policy_version, pipeline_order, feature_flags | — | — | — | **já coesos (NÃO mexer)** |

## Consolidações propostas (coerentes, não forçadas)

### 1. `findings*` (7 → 1) — maior ganho
```python
# v1 (7 chaves soltas no top-level)
'findings': {...}, 'findings_regex': {...}, 'findings_exclusion_terms': {...},
'findings_ignore_sections': [...], 'findings_skip_organ_gate': [...],
'finding_organ_max_chars': 200, 'finding_organ_scope': 'block',
# v2 (1 bloco coeso)
'findings': {
    'terms': {...},              # era 'findings'
    'regex': {...},              # era 'findings_regex'
    'exclusions': {...},         # era 'findings_exclusion_terms'
    'ignore_sections': [...],    # era 'findings_ignore_sections'
    'skip_organ_gate': [...],    # era 'findings_skip_organ_gate'
    'organ': {'scope': 'block', 'max_chars': 200},  # era finding_organ_scope/max_chars
}
```

### 2. `negation*` (3 → 1)
```python
# v1                                        # v2
'negation_phrases': [...],                  'negation': {
'negation_window': 8,                           'phrases': [...],
'negation_direction': {...},                    'window': 8,
                                                'direction': {...},
                                            }
```

### 3. flags soltas → `feature_flags` (bloco já existe)
`emit_decision_trail`, `use_spacy_matcher` entram em `feature_flags` (onde já vivem `rule_engine`,
`calibrated_hybrid`). Vocabulário uniforme de toggles num lugar só.

### 4. `organs*` (3 → 1) — OPCIONAL (mais invasivo)
`organs` + `target_organs` + `shared_organs_path` → `organs: {targets:[...], defs:{...}, shared_path:...}`.
Coerente, mas `target_organs` é muito referenciado; avaliar se o ganho compensa. **Recomendo deixar
fora da 1ª rodada** (fazer 1-2-3 primeiro).

## Impacto (top-level do `nlp`)
| Config | Hoje | Após 1+2+3 | Redução |
|---|---|---|---|
| Tireoide | 24 | ~13 | −46% |
| Hepato | ~15 | ~10 | −33% |
| Pulmão | 14 | ~10 | −29% |

## Regra de normalização (o ponto técnico crítico)
Como distinguir v1 de v2 sem ambiguidade:
- **negation / feature_flags**: sem risco — v1 usa `negation_phrases`/`negation_window` (chaves
  separadas); a presença do bloco `negation` (nome novo) = v2. Idem flags (já em feature_flags).
- **findings**: colisão de NOME (`findings` existe em v1 como dict de termos e em v2 como bloco).
  Detecção robusta: **v2 sse `findings` contém a subchave `terms`** (categorias clínicas nunca se
  chamam "terms"); OU se não há irmãos `findings_regex`/`findings_*` no top-level.
- **Alternativa mais segura**: campo marcador **`nlp.config_schema: 2`** — explícito, zero
  ambiguidade, custo de 1 campo. **Recomendado.**

`normalize_config(nlp)` roda no início do `config_loader.load`: detecta o schema, e se v2, **expande**
pros campos internos que o motor já consome (findings, findings_regex, ...). O motor e todos os steps
ficam **inalterados**.

## Plano de implementação
1. **Lib** (release v0.5.9, byte-compat):
   - `normalize_config(nlp)` no `config_loader` (aceita v1 e v2 → forma interna).
   - Testes: v1 e v2 do mesmo config produzem `nlp` interno idêntico; golden 3/3; os 3 configs reais (v1) seguem carregando.
2. **Migração dos configs** (opcional, gradual): reescrever os 3 no formato v2 quando quiser — o v1 continua válido indefinidamente.

## Gate
- Lib: golden diff=0 + suíte + os 3 configs de negócio (v1) carregam idênticos + testes v1==v2.
- Nenhuma mudança comportamental (é só forma de escrita da config).

## Decisões TOMADAS (2026-07-21, head delegou)
- **Detecção v1/v2 → marcador explícito `nlp.config_schema: 2`.** Zero ambiguidade (findings tem
  colisão de nome v1/v2). Ausência do marcador = v1 → todos os configs atuais seguem v1 (byte-compat).
- **Escopo 1ª rodada → consolidações 1+2+3** (findings, negation, flags). `organs` (4) fica p/ depois.
- **Nomes das subchaves → mantidos**: `terms/regex/exclusions/ignore_sections/skip_organ_gate/organ`
  (findings); `phrases/window/direction` (negation).

## Contrato de normalização (implementação)
`normalize_config(nlp)` no `config_loader.load`:
1. Se `nlp.config_schema` >= 2 → EXPANDE o bloco v2 para os campos internos v1 que o motor consome:
   `findings.terms→findings`, `findings.regex→findings_regex`, `findings.exclusions→findings_exclusion_terms`,
   `findings.ignore_sections→findings_ignore_sections`, `findings.skip_organ_gate→findings_skip_organ_gate`,
   `findings.organ.scope→finding_organ_scope`, `findings.organ.max_chars→finding_organ_max_chars`;
   `negation.phrases→negation_phrases`, `negation.window→negation_window`, `negation.direction→negation_direction`;
   `feature_flags.emit_decision_trail→emit_decision_trail`, `feature_flags.use_spacy_matcher→use_spacy_matcher`.
2. Sem marcador (ou <2) → v1 → passa direto (sem tocar).
3. O motor e todos os steps consomem a forma interna v1 — inalterados.
Gate: v1 e v2 do mesmo config produzem `nlp` interno idêntico; golden 3/3; os 3 configs reais (v1) OK.
