# Proposta de Arquitetura Alvo — `nlp_engine` (v0)

> Documento de decisão. Objetivo: reorganizar e sanitizar a lib para escalar de forma
> profissional e segura. Didático e enxuto — cada decisão amarra um princípio a uma ação concreta.

---

## 0. Contexto (e uma correção importante)

- **Nenhuma especialidade está em produção.** Validamos as funcionalidades *durante o próprio
  desenvolvimento*, usando pilotos: **hepato, BI-RADS, TI-RADS, PI-RADS, tireoide, transplante de
  pulmão**. A **base ouro** foi construída com esses casos → é o nosso critério de validação de
  **comportamento**.
- **Estado atual:** lib em **v0.5.2** na `hml` (refactor P0-A DecisionState + P0-B dedup + P1-A
  singleton spaCy — tudo **byte-compat**). Motor agnóstico, config-driven.
- **Dois gates de validação** (fundamentais para o plano):
  - **Golden (byte-compat):** `regress_golden` — mesma saída byte-a-byte. Vale para *refactor puro*.
  - **Base ouro (comportamento):** compara a decisão do motor contra o gabarito humano por piloto
    (precision/recall/MCC/FN). Vale para *mudança de comportamento intencional*.

---

## 1. Reflexão: princípios → decisões concretas

| Princípio | O que exige | Decisão nesta lib |
|---|---|---|
| **Clean Code** | nomes que revelam intenção; funções pequenas/puras | steps puros nomeados (já em DecisionState); renomear `rads`→escala ordinal |
| **Clean Architecture** (regra de dependência) | domínio não depende de infraestrutura | separar `llm.connection` (infra) do *router/juiz* (domínio); `config_loader` sem depender de módulos de feature (já feito) |
| **DDD** (linguagem ubíqua + bounded context) | vocabulário único; contextos separados | léxico: *achado · categoria ordinal · medida · relevância(fl) · veto*; contexts: `nlp_engine`(decisão) / `monitoring` / MLOps(I/O, lake) |
| **SOLID** | SRP/OCP/DIP/ISP | SRP: conexão ≠ router ≠ juiz; OCP: nova especialidade = **config**, novo step = **registry**; DIP: injeção de `caller` (já em quantitative); ISP: API pública curada |
| **DRY / código mínimo** | 1 fonte de verdade | `_util` (feito); estender p/ *schema de config* e *resolução de modelo* |
| **Agnosticidade** | nada hardcoded por especialidade | camadas opt-in config-in; generalizar nomenclatura RADS |
| **Escalabilidade / Platform Eng** | lib = produto; multi-modelo; stateless | conexão única + modelo por etapa; steps puros escalam no Spark; semver + `py.typed` + API curada (estudo de componentes isoláveis) |
| **NLP clínico (segurança)** | anti-alucinação; auditável | LLM extrai **valor**, código decide **limiar**; **LLM-juiz por último**; decisão **por-achado**; negação; trilha |
| **Refatoração incremental** | preservar invariantes; passos pequenos | fases pequenas, cada uma com gate; byte-compat antes de comportamento |

**Síntese:** o motor já é agnóstico e modular. O que falta é **coerência de fluxo**, **separação
conexão/uso do LLM**, **config mais simples e uniforme** e **nomenclatura verdadeiramente global** —
tudo entregue de forma **incremental e validável**.

---

## 2. Arquitetura alvo

### 2.1 Ordem coerente da cascata

Guia: **determinístico → evidência dura (medidas) → agregação → LLM-juiz por último → veto/saída.**

| # | Passo | Muda vs hoje |
|---|---|---|
| 1 | `treat` (plain + segmentação) | = |
| 2 | `extract_categories` (escala ordinal, regex) | = |
| 3 | `extract_findings` (regra) | = |
| 4 | `expand_semantic` → **vira finding** (opt-in) | 🔀 hoje só sobe score |
| 5 | `measure` (LLM extrai valor → código aplica limiar) | ⬆️ sobe (antes vinha depois do juiz) |
| 6 | `apply_ordinal` (promoção/zera determinístico) | ⬆️ sobe p/ antes do juiz |
| 7 | `score + calibrate` (agrega regra+semântica+medida+ordinal) | 🔀 agrega depois de coletar tudo |
| 8 | `decide_llm` (juiz — só na banda de incerteza, vê tudo) | ⬇️ desce p/ penúltimo |
| 9 | `vet` (veto do doc) | = |
| 10 | `finalize` (invariants + JSON + trilha) | = |

Ganho: o juiz decide **depois** da evidência dura; a semântica deixa de ser invisível p/ `measure`/`vet`.

### 2.2 LLM: conexão única + modelo por etapa (com herança)

> **Config = dict-in (Python), NÃO YAML.** Os configs dos pilotos são arquivos `.py` no runner
> (`ntb_ia_<especialidade>_config.py`) que produzem um `CONFIG` dict; o `config_loader` consome dict.
> Os blocos abaixo mostram a ESTRUTURA do dict (em notação YAML só por legibilidade).

```yaml
llm:
  connection:            # 1 FONTE DE VERDADE (só transporte)
    base_url: ...
    token_env: ...
    timeout: ...
    retry: {...}
  model: "modelo-default"   # modelo do fluxo (declarado 1x)

llm_router:                 # juiz — só a TAREFA (model herda llm.model)
  enabled: true
  prompt_system: "..."
rads_extraction:            # (futuro: ordinal_extraction)
  llm_fallback: { trigger: alias_without_category }   # herda
quantitative:
  criteria: [...]           # model: "modelo-X"  <- override opcional (multi-modelo)
```

Regra: `modelo_da_etapa = etapa.model  OU  llm.model`. Declara o default **uma vez**; especifica
`model:` na etapa **só** para multi-modelo. **Conexão nunca se repete** — única em `llm.connection`,
injetada pelo hub (`call_openai_compatible_chat`) em toda chamada.

### 2.3 Config v2 (simplificação + compat)

- **Validação simétrica** no `config_loader` (hoje valida só parte dos blocos).
- **Vocabulário uniforme** entre blocos (`enabled`, `model`, `gate`, nomes de campos consistentes).
- **Defaults seguros explícitos** → menos campos obrigatórios.
- **Compat obrigatória:** chave de config é contrato dos configs `.py` (dict-in) dos pilotos →
  **camada de normalização** aceita nome antigo e novo (nunca rename seco).

### 2.4 Generalização RADS → escala ordinal

- O `rads_extraction` **já é genérico** (systems + aliases + categories ordenadas + patterns). Só o
  **nome** está preso ao caso de uso. Serve p/ qualquer escala graduada (Bethesda, Bosniak, TNM, …).
- **Renomear** `rads`→`ordinal_extraction` (ou `graded_categories`) **com alias**: nome novo +
  antigo aceito. API pública (`extract_rads_summary`, `rads_promoted_systems`), chave de config e
  semântica `rads_only` ganham alias.
- ⚠️ Colunas/audit no **lake** com "rads" = hand-off **MLOps** (não DS).

---

## 3. Estratégia de segurança

- **Refactor puro** (sem mudar decisão): gate = **golden diff=0 + suíte**. Ex.: Fases 1 e 2.
- **Mudança de comportamento** (intencional): gate = **base ouro por piloto** (precision/recall/MCC/FN),
  **opt-in** por flag/especialidade, **default = comportamento atual**. Ex.: Fases 3 e 4.
- **Nunca** misturar refactor puro com mudança de comportamento no mesmo PR.
- Bump de versão sempre com **`pyproject` + `uv.lock` no mesmo commit** (lição do v0.5.1).

---

## 4. Plano de implementação em fases (do mais seguro ao mais sensível)

| Fase | Escopo | Tipo | Gate | Risco |
|---|---|---|---|---|
| **0** | Congelar base ouro por piloto (snapshot+hash) + harness "vs base ouro" | infra de teste | — | — |
| **1** | LLM `connection` única + `resolve_model` (herança) + compat | byte-compat | golden + suíte | baixo |
| **2** | Config v2 (validação simétrica, vocabulário uniforme, RADS→ordinal c/ alias) | byte-compat | golden + suíte | baixo-médio |
| **3** | Semântica → finding (`embeddings.emit_as_finding`, default OFF) | comportamental (opt-in) | base ouro | médio |

> **STATUS F3 (2026-07-19): CAPACIDADE IMPLEMENTADA localmente** — branch `refactor/f3-semantic-finding`
> (stacked sobre FP-01 → **v0.5.7**). Flag `embeddings.emit_as_finding` (default OFF). Quando ON e há
> match semântico, o achado vira finding mensurável (injeta em `summary_compact` + `n_positive_spans`),
> tornando-o visível a `measure`/`vet` (antes cegos). Byte-compat com default OFF: golden 3/3, suíte 342
> (+4 testes, backend `token_overlap` sem modelo). **A ATIVAÇÃO (ON) por especialidade é o rollout
> seguinte** — valida contra base ouro (pulmão/tireoide) / estabilidade (hepato) por piloto, nunca liga
> global. Reutiliza `_semantic_term_finding` p/ mapear termo→finding.
| **4** | Reordenar cascata (measure/ordinal antes do juiz; juiz penúltimo) — flag por especialidade | comportamental (opt-in) | base ouro | médio-alto |

> **STATUS F4 (2026-07-19): CAPACIDADE IMPLEMENTADA localmente** — branch `refactor/f4-pipeline-order`
> (de origin/hml=0.5.7 → **v0.5.8**). Flag `nlp.pipeline_order: "target"` (default "legacy" = ordem
> atual). Ordem alvo: `findings → score → expand_semantic → apply_rads → measure → calibrate →
> decide_llm → vet → finalize` — evidência dura (rads/measure) ANTES do juiz; **LLM-juiz por último**
> vendo o `fl` já ajustado; vet fecha. Reordena os MESMOS objetos Step (gates preservados) via
> `RULE_PIPELINE_TARGET`. Dependências respeitadas (calibrate antes de decide_llm; measure antes de
> vet). Byte-compat default legacy: golden 3/3, suíte 346 (+5). **A ATIVAÇÃO ("target") por
> especialidade é o rollout** — validado vs base ouro, junto com a F3 (o fluxo coerente completo).
> **Todas as capacidades da arquitetura alvo (F1–F4) estão implementadas.**
| **5** | Performance: paralelizar/cachear chamadas LLM (o caso 1h29) | otimização | golden + benchmark | opcional |

Racional da ordem: fundações byte-compat primeiro (1,2); mudanças de comportamento por último (3,4),
cada uma opt-in e validada contra base ouro; performance só se o operacional exigir.

> **STATUS F2 (2026-07-17): fatias 1 e 2 FEITAS localmente** — branch `refactor/f2-config-ordinal`
> (stacked sobre F1 → **v0.5.5**). **Fatia 1 — RADS → `ordinal_extraction` com alias** (chave de config
> canônica + aliases de API `extract_ordinal_*`/`ordinal_promoted_systems`; `rads_extraction` segue como
> alias legado). **Fatia 2 — validação simétrica**: `config_loader` valida a FORMA dos blocos antes
> não-validados (llm_router, quantitative_criteria, document_vet, segmentation, text_pipeline,
> feature_flags, findings_regex, findings_exclusion_terms = mapping; negation_phrases,
> findings_ignore_sections = list[str]). Conservador (só tipo, não valores). **Byte-compat verificado
> com os 5 configs `.py` reais dos pilotos** (birads/hepato/pirads/tirads/pulmão, extraídos da
> plataforma — todos seguem carregando); golden 3/3, suíte 334. **PENDENTE: vocabulário uniforme**
> (renomear chaves p/ convenção única) — fatia própria, churn alto/ROI baixo, toca os configs dos
> pilotos. Não renomeados: módulo/funções internas e campos de saída no lake (MLOps). tireoide não tem
> config `.py` (WIP notebook) — validar quando existir.
>
> **STATUS F0 (base ouro): PULMÃO FEITO** — harness `baseohro_pulmao.py` (TP126/FP1/FN0/TN1727;
> recall 1,0 · precisão 0,9921 · MCC 0,9958). Faltam os outros 5 pilotos (precisam dos gabaritos).

---

## 5. Fase 1 — escopo fechado

> **STATUS (2026-07-16): IMPLEMENTADA localmente** — branch `refactor/f1-llm-connection` (base
> `origin/hml`=v0.5.2 → v0.5.3), byte-compat. Gates verdes: golden 3/3, suíte **318** (+7 testes do
> resolver), ruff+mypy. Novo em `llm_router_backend`: `llm_connection_defaults` · `default_llm_model`
> · `resolve_llm_call_cfg` (conexão única + herança de modelo, com fallback legado por consumidor).
> Ligado em `decide_llm_http`, `quantitative` e `rads_extraction._llm_connection_cfg`. **Pendente PR**
> (sem push até autorização). Configs dos pilotos inalteradas (compat). Retomar: revisar/push → F2.

**Objetivo:** separar **conexão** de **uso** do LLM e habilitar **multi-modelo com herança**, sem
mudar nenhuma decisão (byte-compat).

Passos:
1. Branch a partir de `origin/hml` (nome no kickoff, ex.: `refactor/f1-llm-connection`).
2. Schema `llm.connection` (base_url/token/timeout/retry) + `llm.model` default.
3. Helper `resolve_model(step_cfg, llm_cfg)` = `step.model or llm.model`.
4. `config_loader`: **normaliza** config atual (conexão inline no `llm_router`) → forma nova (compat).
5. Hub recebe `connection + model resolvido`; `rads`/`quantitative`/juiz consomem via resolução.
6. **Gate:** golden 3/3 (tirads/hepato/pirads) + suíte + import smoke.
7. Bump de versão + `uv.lock` no **mesmo** commit.
8. PR (só com autorização explícita); título+descrição+tag.

**Critério de aceite:** golden diff=0, suíte verde, configs dos 6 pilotos carregam sem alteração
(compat), e é possível declarar `model` por etapa.

---

## 6. Invariantes (não-negociáveis)

- Agnóstico/config-in · byte-compat + opt-in (default seguro) · menos-é-mais (1 fonte de verdade).
- LLM-juiz por último · decisão POR-ACHADO · LLM extrai valor / código aplica limiar.
- LGPD in-tenant (LLM via endpoint interno; sem PHI para fora).
- Escopo DS = `nlp_engine` + configs clínicos; I/O/lake/runner = MLOps.
- git push/PR só com autorização explícita.
