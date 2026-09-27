# Proposta — Config Schema v3 (findings por-entidade)

> Evolução do v2 (que agrupou por *aspecto*) para agrupar por **entidade de domínio**: a definição
> completa de cada achado (linfonodo, nódulo, bócio…) vive num bloco só. A `normalize_config`
> **transpõe** o formato por-entidade (como o humano pensa/mantém) → forma interna por-aspecto (como
> o pipeline executa). Byte-compat, opt-in via `config_schema: 3`. **Prova de conceito validada**:
> round-trip do tireoide real (7 findings) → forma interna == v1, campo-a-campo.

## Motivação — coesão no nível certo
Hoje a definição de **um** achado está pulverizada. O linfonodo aparece em **7 blocos** do `nlp`
(`findings`, `findings_regex`, `findings_skip_organ_gate`, `findings_exclusion_terms`,
`negation_direction`, `quantitative_criteria`, `document_vet`). Para entender/alterar a regra de
negócio do linfonodo, caça-se o arquivo inteiro. O v2 (por-aspecto) só ameniza — o linfonodo seguiria
dividido entre `findings.terms.linfonodo`, `findings.regex.linfonodo`, `negation`, `quantitative`.

## Dois eixos, dois donos (o insight)
- **Por-aspecto** = como o **motor executa**. Cada nó do grafo (`DecisionState` transita entre `Step`s)
  processa *um aspecto de todos os findings*: `extract` lê todos os termos, `negate` nega todos,
  `measure` mede todos. Formato "plano" interno.
- **Por-entidade** = como o **humano pensa e mantém** (linguagem ubíqua do DDD: "o linfonodo tem
  estas regras"). Cada achado é uma unidade auto-contida.

O motor precisa do eixo-aspecto; o humano quer o eixo-entidade. **A `normalize_config` é o adapter que
transpõe entre os dois** — esse é o seu propósito real (não renomear chaves).

## Princípios (aplicados ao contexto, sem dogma)
- **DDD** — cada finding é um *agregado* do domínio clínico, com identidade e regras coesas. A config
  passa a falar a linguagem ubíqua (o clínico pensa "linfonodo", não "findings_regex").
- **Clean Architecture** — config por-entidade = camada externa (domínio); `normalize_config` = adapter
  na fronteira; motor consome a forma interna (estável). O domínio não conhece o formato de escrita.
- **SOLID** — *SRP*: cada bloco de finding tem uma responsabilidade (definir aquele achado); a
  transposição tem uma (pivotar). *OCP*: novo finding = novo bloco, motor intocado. *DIP*: o motor
  depende da forma interna (abstração), não do formato de escrita.
- **DRY** — o nome do finding aparece **1 vez** (a chave do bloco), não repetido em 7 lugares.
- **Programação pragmática** — *ortogonalidade* preservada (aspectos independentes seguem
  independentes, só reagrupados sob a entidade); mudança *reversível* (byte-compat) e *incremental*.
- **Engenharia de plataforma** — o schema é contrato multi-especialidade: versionado
  (`config_schema`), retrocompatível, com a transposição como fonte única de mapeamento.

## Pragmatismo — o que NÃO vira por-entidade (boas práticas conforme contexto)
Nem tudo é do finding. Fica **global** (não force):
- `negation.phrases` / `negation.window` — o léxico de negação é do documento, não de um achado.
  Só a **direção** (`negation_direction`) é por-finding (linfonodo=both).
- `findings_policy.ignore_sections`, `organ.scope/max_chars` — política de detecção global.
- `embeddings`, `llm_router`, `segmentation`, `score_policy`, `feature_flags` — blocos já coesos.
- **`quantitative_criteria`** — HÍBRIDO: critério livre (pulmão: VEF1, sem finding) é global;
  critério ancorado (tireoide: `linfonodo_suspeito`, anchor=linfonodo) *pode* ir para o finding.
  **Decisão pragmática: fase 2.** Fase 1 move só o núcleo, para reduzir risco.

## Contrato v3 (bloco `findings` por-entidade)
```python
'config_schema': 3,
'findings': {
    'linfonodo': {
        'terms':   [...],                       # léxico
        'regex':   [r'...'],                    # padrões
        'skip_organ_gate': True,                # relevante sem "tireoide" adjacente
        'exclude': ['reacional', ...],          # rebaixa se qualificado
        'unless':  ['necrose', 'suspeito', ...],# override de segurança
        'negation_direction': 'both',           # senão usa o default global
    },
    'nodulo': { 'terms': [...], 'regex': [...] },
    'bocio':  { 'terms': [...], 'exclude': ['difuso', ...], 'unless': ['nodular', ...] },
},
'findings_policy': { 'ignore_sections': [...], 'organ': {'scope': 'block', 'max_chars': 220} },
'negation': { 'phrases': [...], 'window': 8, 'direction_default': 'left' },
# embeddings / llm_router / segmentation / quantitative_criteria: inalterados (fase 1)
```

## Regra de transposição (entidade → aspecto)
`normalize_config`, para cada `findings.<nome>`:
`terms→findings[nome]` · `regex→findings_regex[nome]` · `skip_organ_gate:True→findings_skip_organ_gate+=[nome]`
· `exclude/unless→findings_exclusion_terms[nome]` · `negation_direction→negation_direction[nome]`.
Globais: `negation.*→negation_phrases/window`, `direction_default→negation_direction._default`,
`findings_policy.*→findings_ignore_sections/finding_organ_scope/finding_organ_max_chars`.
O motor e os steps ficam **inalterados** (consomem a forma interna de sempre).

## Prova de conceito (validada)
Protótipo `proto_v3.py`: converteu o tireoide real (v1) → v3 por-entidade → transpôs de volta →
**== v1 campo-a-campo (round-trip True)**, 7 findings. O linfonodo consolidou em 1 bloco
(terms/regex/skip_organ_gate/exclude/unless/negation_direction).

## Compatibilidade e escopo
- `config_schema`: **1** (plano, atual), **2** (por-aspecto, já na lib), **3** (por-entidade, esta
  proposta). Todos coexistem; ausência do marcador = v1. Migração **opcional/gradual**.
- **Fase 1** (esta): núcleo por-entidade (terms, regex, skip_organ_gate, exclude/unless,
  negation_direction) + `findings_policy` + `negation`. Gate: round-trip==v1 nos 3 configs + golden 3/3.
- **Fase 2** (depois): `quantitative`/`llm_check` ancorados migram para o finding (linfonodo_suspeito).

## Decisão a confirmar
- Nomes: `findings_policy` (globais) e `negation{phrases,window,direction_default}` — ok?
- `skip_organ_gate` como booleano no finding (vs lista global) — ok?
- Fase 1 só núcleo (quantitative fica fase 2) — ok?
