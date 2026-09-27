# Design — limiar quantitativo condicionado a campo da linha (age-conditioning) · GLOBAL/opt-in

**Data:** 2026-07-14 · **Escopo:** `nlp_engine` (camada `quantitative`) · **Motivação:** demanda médica
(Carolina Marques, Transplante de Pulmão) — "achado X só é válido para idade Y". · **Status:** design
p/ implementação (lib 0.4.1).

## Problema
Alguns critérios quantitativos têm **limiar dependente de um atributo do paciente**. No pulmão V1:
supurativa = VEF1 **< 40%** (adulto) · **< 50%** (< 18 anos). Hoje a camada só aplica **limiar fixo**.

## Requisitos
- **Genérico:** vale p/ qualquer especialidade e qualquer campo (idade, sexo…), não hardcode de pulmão.
- **Opt-in / byte-compat:** critério sem a nova chave = limiar fixo de hoje (golden diff 0).
- **No escopo `nlp_engine`:** sem depender de plumbing de dados na `fabrica-ia-lib` (a row do motor tem
  colunas fixas; `idade` estruturado não chega). **A idade está NO laudo** ("(77 anos)") → parse do texto.

## Mecanismo — `threshold_by` (config-in)
Novo campo opcional no critério dimensional single (`measure`+`threshold`):

```python
"supurativa_vef1": {
    "measure": {"name": "vef1_pct_previsto", "unit": "%"},
    "threshold": {"op": "<", "value": 40.0},        # DEFAULT (usado se a condicao nao resolver)
    "threshold_by": {                                # OPT-IN
        "source": {"regex": r"(\d{1,3})\s*anos", "cast": "int"},  # de onde vem o valor de condicao
        "rules": [{"max": 17, "value": 50.0}],       # idade <= 17 -> limiar 50 (op herda do threshold)
    },
    "on_met": "promote",
}
```

**Semântica:**
- `source.regex` casa no **texto tratado** (`treated`); grupo 1 = valor de condição; `cast` (int/float).
  Se não casar → condição indeterminada → usa o `threshold` **default** (conservador).
- `rules` avaliadas em ordem; primeira que casar define o `value` do limiar. Cada rule aceita
  `min`/`max` (inclusive) sobre o valor de condição. Sem match → default.
- O **operador** (`op`) e a **medida** não mudam; só o **valor** do limiar. (Extensível: `applies_if`
  p/ desligar o critério fora de uma faixa — não necessário no V1.)

## Onde no código (`quantitative.py`)
- `Criterion.threshold_by: Mapping` (default `{}`).
- `parse_criterion`: lê `threshold_by`.
- `assess_criterion(treated, criterion, ...)` já tem `treated`. Antes de `evaluate_criterion`, resolver
  o limiar efetivo: `_effective_criterion(criterion, treated)` — parse do valor de condição + escolha da
  rule → devolve o critério com o `value` da condição ajustado. Evaluate/audit seguem iguais.
- O bloco de audit ganha `threshold` efetivo + (opcional) o valor de condição usado (ex.: `idade=15`)
  p/ rastreabilidade.

## Byte-compat / validação
- Critério sem `threshold_by` → caminho idêntico (golden 3/3: tirads/hepato/pirads).
- Testes novos: rule pediátrica (<18→50), adulto (→40), idade ausente→default, cast/parse.
- Extração continua via LLM; a resolução do limiar é **determinística em código**.

## Pulmão V1 (aplicação)
- `funcao_vef1`: `threshold` `<40` (default) + `threshold_by` idade `{max:17 → 50}`. Cobre supurativa
  (adulto<40, ped<50) e DPOC (<30 ⊂). Cobertura de idade no laudo ~68% ("N anos"); sem idade → adulto.
- Requer wheel **0.4.1**. Fonte de idade = texto do laudo ("N anos"); fallback futuro = nascimento+data.

## Limitações / futuro
- Idade só quando presente no laudo (~68%). Fonte estruturada (`cli_idade` de `mov_paciente`) exigiria
  a `fabrica-ia-lib` repassar a coluna à row do motor (hoje fixa em process.py) — recomendação de longo
  prazo p/ o time MLOps (passar row completa ou adicionar `idade` ao set fixo).
- `source` extensível a `{"kind":"row_field","field":"idade"}` quando o plumbing existir — mesmo
  `threshold_by`, só muda a fonte.
