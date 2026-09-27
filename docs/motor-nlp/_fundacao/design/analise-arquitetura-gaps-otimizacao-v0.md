# Análise de arquitetura — motor NLP clínico: como se conversa, gaps, otimização (v0)

**Data:** 2026-07-14 · **Baseado em** revisão de código + cruzamento com o mapeamento anterior (docs
`_fundacao/`, `tireoide/mapa-gaps`, `pulmao/`) — considerado, **não tomado como verdade**.

> **DIVISÃO DE RESPONSABILIDADE (2026-07-14):** **DS é dono do `nlp_engine`** (a lib do motor) **+ os
> configs clínicos por especialidade** (`ntb_ia_*_config.py`). **`fabrica-ia-lib` (data_manager, batch,
> adapters) e a plataforma/runner são do MLOps.** Portanto o **roadmap DS (§5) foca SÓ no `nlp_engine`**;
> tudo de fabrica_ia/plataforma está isolado em **§3.4 + §5-HANDOFF como recomendação pro MLOps**, não
> como trabalho de DS. Os ajustes que fizemos no runner nesta sprint (injeção de base_url quando há
> `quantitative_criteria`; fix do `limit_rows`) foram **stopgaps** p/ destravar o pulmão — devem ser
> revisados/assumidos pelo MLOps (idealmente via um perfil `llm_measure`, §5-HANDOFF).

---

## 0. Sumário executivo — os 7 achados que importam

1. **Refactor DecisionState pronto, mas NÃO integrado.** A linha de release (0.4.0/0.4.1, pulmão)
   roda o **`engine.py` monolítico** (cascata com **7 mutações de `fl` no mesmo escopo**). O refactor
   (`decision_pipeline.py`, steps puros, golden diff=0) está só em `refactor/decision-state-pipeline`.
   Cada feature nova na linha monolítica aumenta a divergência. **Decisão estratégica: integrar o
   refactor antes de continuar empilhando features.**
2. **Duplicação real na wheel** (contra "menos é mais"): helpers idênticos em 3-5 módulos; **parse-JSON
   do LLM reimplementado 4×**; 2 tabelas de unidade; 2 remoções de acento; 3 noções de "fronteira de
   sentença"; 2 singletons spaCy; 2 fallbacks de versão divergentes (`0.5.6` vs `0.0.0`).
3. **`nlp_router` sobrecarregado como "pool de credenciais"** — quantitative e rads puxam base_url/token
   dele via `_shared_llm_defaults`. Conexão LLM não tem bloco próprio → responsabilidades misturadas.
4. **Semântica não vira finding mensurável** — o match semântico eleva score/fl mas não entra em
   `summary_compact`/`n_positive_spans`; VET e gate-coordenado não o enxergam. Inconsistência entre
   campos do output. (É o que a Etapa 2 do refactor endereça, opt-in.)
5. **Acoplamento `nlp_engine` ↔ `fabrica_ia` com 4 pontos frágeis** (todos fora do nosso escopo de
   edição, mas material p/ o time): `engine_version` gravado na Delta é o do **fabrica_ia** (não o do
   motor); `apply_runtime_profile` manipula chaves internas do `nlp_cfg`; **não há perfil "só medição
   LLM"** (base_url só em llm_http → gambiarra no runner); `column_map` aplicado em 2 lugares.
6. **Custo/latência do LLM:** N critérios = N chamadas HTTP **sequenciais** por laudo, sem batch/cache/
   paralelismo (o 1h29 de junho). Determinismo: ±2 relevantes entre runs idênticos (temp=0 não é 100%
   reprodutível); **buraco de determinismo** no fallback RADS (não fixa temperature).
7. **Config surface confusa:** 2 bandas de incerteza distintas (`embeddings.ambiguity_band` 0.3–0.7 vs
   `llm_router.uncertainty_band` 0.35–0.65); negação configurável em 3 lugares; validação assimétrica
   (`config_loader` valida embeddings/rads a fundo, mas NÃO llm_router/quantitative/document_vet).

---

## 1. Como o sistema se conversa (camadas)

```
[Gold corporativo]  gold_corporativo_ia.corporativo.tb_gold_mov_exame (+ mov_paciente)
      │  (data_manager.GoldDataManager.get_data — fabrica_ia)
      ▼
[ENTRADA]  run_entrada: get_data → extract_laudo(proced_lista_exames) → gold_filter → column_map
      │            → write_staging (motor_entrada, Delta)
      ▼
[PROCESS]  run_process: read_staging → rows_in (colunas FIXAS no motor_gold) → run_engine
      │            → ClinicalNlpEngine.process(rows, nlp_cfg)   ◄── nlp_engine (o MOTOR)
      │            → validate_engine_output_row → write motor_saida (Delta)
      ▼
[HOMOLOG] motor × legado (skip se legacy.enabled=False) → detail/summary + métricas MLflow
      ▼
[MONITORING] metrics_table (não-fatal)      [DISTRIBUIÇÃO] OneDrive (opt-in)
```

- **Duas libs, papéis distintos.** `nlp_engine` = o motor (decisão sobre `nlp_cfg`, agnóstico).
  `fabrica_ia` = acesso ao Gold + orquestração batch + monitoring. **Sem imports cruzados** (diretriz).
- **Runner = Composition Root** (plataforma): widgets, ambiente, instala 2 wheels, MLflow, carrega
  config, aplica perfil, injeta base_url. É onde o "encanamento de runtime" vive (correto).
- **Especialidade = config.** birads/pirads/tirads/hepato/transplante_pulmao são só `ntb_ia_*_config.py`.
  Perfis: hepato=`llm_http`; demais=`rule_only`; tirads/pulmão usam `quantitative_criteria`.

## 2. O fluxo de decisão (cascata) e onde cada coisa age

Ordem no `engine.process` (monólito atual) — **cada linha pode mudar `fl`**:

| # | Step | Muda `fl`? | Config-gate |
|---|---|---|---|
| 1 | `extract_rads_summary` | (só audita; promove no #8) | `rads_extraction.enabled` |
| 2 | segmentação + `process_rule_based` | define n_pos → | `feature_flags.rule_engine` |
| 3 | `fl_relevante_from_counts` | **define fl** (n_pos>0) | sempre |
| 4 | semantic (`semantic_evidence`) | **0→1** (fallback/hybrid) | `embeddings.use_embeddings` |
| 5 | `confidence_calibrated_meta` | só score (label hybrid_calibrated) | `feature_flags.calibrated_hybrid` |
| 6 | `llm_router_step` | keep/demote/promote na banda | `llm_router.enabled` |
| 7 | `rads_only` | **zera fl** | `relevance_mode=rads_only` |
| 8 | promoção RADS | **0→1** | `relevance_mode≠normal` |
| 9 | `process_quantitative_criteria` | promote(0→1)/gate(1→0) via LLM | `quantitative_criteria` |
| 10 | `_document_vet` | **1→0** (achado leve) | `document_vet.enabled` |

**Invariante declarado (respeitado): decisão POR-ACHADO** — negação/exclusão por finding; agregação OR;
único demoter de documento é o VET, conservador. **LLM-juiz por último, só na banda.** Extração LLM só
devolve valor+evidência; limiar em **código** (determinístico).

## 3. Gaps (por categoria + severidade)

### 3.1 Estruturais (alto impacto)
- **[ALTA] Refactor não integrado** (achado #1). O monólito com 7 mutações de `fl` é o maior risco de
  regressão/manutenção — é o motivo declarado do refactor, que já existe e passou golden-diff-0 mas não
  foi mergeado. Enquanto isso, 0.4.0/0.4.1/pulmão empilham na linha monolítica.
- **[ALTA] Semântica não é finding mensurável** (achado #4). `fl=1` só-embedding tem `summary` vazio →
  VET e gate-coordenado cegos a ele; quantitativa não mede achado semântico. Confirma o gap que o design
  §6b/Etapa 2 antecipa. Endereçar SÓ como Etapa 2 opt-in (byte-compat), validado vs base ouro.
- **[MÉDIA] "3 vazamentos" do roadmap-consolidacao** — (a) semântica promove e o juiz não vê; (b) achado
  negado dispara sem vet; (c) gate só veta dimensional. O refactor+VET final os endereça; medir na base
  ouro se ainda ocorrem pós-refactor.

### 3.2 Duplicação / acoplamento (menos-é-mais) — **no nosso escopo (nlp_engine)**
- Helpers `_as_mapping/_as_str_list/_clip01/_in_band` idênticos em engine/llm_router/decision_pipeline
  (+ `_as_mapping` em quantitative/rads) → **`_util.py`**.
- **Parse-JSON-LLM tolerante reimplementado 4×** (llm_router `_parse_llm_json`, quantitative
  `parse_extraction`+`parse_qualitative`, rads `_parse_rads_llm_category`) com regex divergentes
  (`\{[^{}]*\}` vs `\{.*\}`) → **1 helper** em `llm_router_backend`.
- `_safe_format_template` (router) ≡ `_safe_format` (rads) → 1.
- 2 tabelas de unidade (`to_plain._UNITS` vs `quantitative._canon_unit`) e 2 remoções de acento
  (`norm.norm` vs `quantitative._strip_accents`) → fonte única.
- 3 "fronteiras de sentença" (rads regex, rule_engine spaCy, semantic split) + 2 detecções de
  linha-cabeçalho (`rule_engine._HEADER_LINE_RE` vs `by_headers.HEADER_RX`).
- 2 singletons spaCy blank-pt (`engine._SEG_NLP`, `rule_engine._NLP`) → 1 recurso.
- 2 fallbacks de versão (`engine._package_version`→"0.5.6" vs `__init__`→"0.0.0").
- `config_loader` importa `LLM_FALLBACK_TRIGGERS` de `rads_extraction` (validador depende do domínio) →
  mover a constante p/ lugar neutro.

### 3.3 Config surface
- 2 bandas de incerteza distintas (semantic vs router) sem relação declarada.
- Negação em 3 lugares (`negation_phrases`/`_expressions`/`_window`/`_direction` + `rads_extraction.negation`).
- **`llm_router` = pool de credenciais** compartilhado (quantitative/rads via `_shared_llm_defaults`) →
  extrair um bloco **`llm_connection`** (base_url/token/timeout) separado do router de relevância.
- `organs` vs `all_organs` duplicados no config normalizado.
- Validação assimétrica: `config_loader` NÃO valida `llm_router`/`quantitative_criteria`/`document_vet`/
  `segmentation`/`feature_flags` → erros só em runtime.

### 3.4 Plataforma / wiring — **fora do nosso escopo (fabrica_ia); recomendações p/ o time**
- **`engine_version` na Delta = `fabrica_ia.__version__`** (run_engine default), não o do motor →
  proveniência errada na persistência (MLflow diz 0.4.1, tabela diz 0.5.8).
- **Sem perfil "só medição LLM"** — `base_url` só em `llm_http`; runner compensa injetando quando há
  `quantitative_criteria` (`setdefault`, frágil). Perfil `llm_measure` de 1ª classe resolveria.
- `apply_runtime_profile` (fabrica_ia) manipula chaves internas do `nlp_cfg` do motor (acopla ao contrato).
- `column_map` aplicado em 2 lugares (entrada p/ gold; adapters p/ legado) → ponto-de-verdade duplo.
- Catálogo/schema precisam **pré-existir** (setup só `CREATE TABLE`); `diamond_transplante_pulmao` não
  provisionado (usamos `diamond_ia_hml` em HML).
- Bug latente: `llm_api_key_env` DEVE ser exatamente `"DATABRICKS_TOKEN"` senão o token não popula e o
  `fallback_policy=positive_in_band` promove tudo (FP em massa). Está correto hoje, mas frágil.
- `limit_process_rows` fatiado em Python pós-`collect()` (não empurra `.limit()` ao read).

### 3.5 LLM (custo/latência/determinismo)
- N critérios dimensionais = N chamadas HTTP **sequenciais** por laudo; RADS fallback = 1 POST por
  alias. Sem batch/paralelo/cache (decisão de spec: sem cache, para não duvidar da origem do valor).
- Determinismo: ±2 relevantes entre runs idênticos (temp=0 não é perfeito). **Buraco:** RADS fallback
  não fixa `temperature`.

### 3.6 Clínico (aguardando médico — não são bugs)
- Tireoide: G-P1 (negação distante), G-INFRA1 (LLM pula ~82 medidas), G-INFRA4 (condicionar por exame),
  G-P3 (promoção sem achado). Ver `tireoide/mapa-gaps`.
- Pulmão: **gap de recall narrativo** — laudos de gravidade sem número ("obstrutivo acentuado", CVF em
  litros) escapam do numérico (~21+ FN severos). **Carolina optou por V1 SÓ quantitativo** (receio de FP
  na fila). Um critério `kind: qualitative` (já existe na lib) resolveria — adiado.

## 4. Otimização de fluxo (custo · latência · reprodutibilidade)

| Alavanca | Ganho | Custo/risco | Escopo |
|---|---|---|---|
| Integrar o refactor DecisionState | manutenção; habilita Etapa 2; observabilidade (`active_step_names`) | rebase das features sobre ele; re-validar golden 3/3 | nlp_engine |
| Consolidar duplicação (util/parse-JSON/unidade/acento) | menos-é-mais; menos divergência silenciosa | baixo (byte-compat + testes) | nlp_engine |
| Extrair `llm_connection` do `llm_router` | coesão; config mais clara | migração de config (alias compat) | nlp_engine + configs |
| Pré-compilar regex por lote (no `ctx`) | latência CPU do rule/rads | baixo | nlp_engine |
| Paralelizar/cachear chamadas LLM | custo+latência (1h29→min) | **só se operacional exigir** (menos-é-mais); rever "sem cache" da spec | nlp_engine |
| `.limit()` no read do process (não pós-collect) | latência em test_mode | baixo | fabrica_ia (recomendação) |
| Perfil `llm_measure` / desacoplar base_url | remove gambiarra do runner | fabrica_ia (recomendação) | fabrica_ia |
| `engine_version` correto na Delta | rastreabilidade | fabrica_ia (recomendação) | fabrica_ia |

## 5. Recomendações priorizadas (roadmap) — **DS = só `nlp_engine`**

**P0 — já, alto valor, baixo risco (nlp_engine):**
1. **Decidir a integração do refactor DecisionState** (merge/rebase). É o fork estratégico — evita
   divergência crescente entre a linha monolítica (release) e a modular (refactor).
2. **Consolidar duplicação** num `_util.py` + 1 parse-JSON-LLM (byte-compat, golden 3/3, testes).
   Antes de qualquer coisa: **checar a lib** (foi o achado — muita coisa já existe, só está duplicada).

**P1 — médio prazo, no escopo:**
3. **Extrair `llm_connection`** (base_url/token/timeout) do `llm_router` (com alias compat) — resolve a
   confusão do "pool de credenciais" e é a base p/ o perfil de medição.
4. **Validação de config simétrica** no `config_loader` (llm_router/quantitative/document_vet/segmentation).
5. **Etapa 2 (opt-in): semântica→finding mensurável** — validar vs base ouro; NUNCA junto do refactor puro.
6. Pré-compilar regex por lote; fixar `temperature=0` no fallback RADS.

**§5-HANDOFF — MLOps (NÃO é trabalho DS; só sinalizar):**
7. `engine_version` na Delta = versão do **nlp_engine** (não fabrica_ia) — hoje grava a versão errada.
8. Perfil `llm_measure` (LLM só medição, sem embeddings/relevância) — elimina o stopgap de injeção de
   base_url que DS pôs no runner p/ o pulmão; formaliza no `apply_runtime_profile`.
9. `column_map` num único ponto; `.limit()` no read (não pós-collect); provisionamento de catálogo/schema.
10. `apply_runtime_profile` manipula chaves internas do `nlp_cfg` do motor — contrato acoplado (avaliar
    quem valida o schema do config do motor).

**Clínico (aguardando médico — decisões de negócio, não técnicas):** políticas do pulmão (grave-narrativo,
qualitativo) e os gaps do tireoide (G-P1/G-INFRA1/4).

> **Nota de fronteira:** os configs `ntb_ia_*_config.py` moram no repo da plataforma mas são **conteúdo
> DS** (léxico/critérios clínicos). A lógica de **runner/batch/data_manager** é MLOps. O contrato entre
> os dois é o `nlp_cfg` (dict) + o schema da row — DS garante o motor; MLOps garante a entrega do dict e
> das rows. Mudanças no contrato exigem alinhamento DS↔MLOps.

## 6. Confirmações / divergências vs o mapeamento anterior
- **Confirmado:** os 3 designs (DecisionState, age-conditioning, runner-llm), a spec quantitativa, os
  gaps do tireoide e do pulmão, os princípios (agnóstico/config-in, menos-é-mais, byte-compat, LLM-juiz-
  último, por-achado, LGPD in-tenant, 3 libs sem imports cruzados).
- **Adiciono (não estava explícito nos docs):** a duplicação concreta na wheel (parse-JSON ×4, helpers,
  unidades, acentos, sentença, spaCy) — o "menos é mais" tem alvos concretos; o `llm_router` como pool
  de credenciais; a validação de config assimétrica; o `engine_version` errado na Delta; a divergência
  release-monólito vs refactor-modular (o maior risco estrutural hoje).
- **Nuance:** o roadmap-consolidacao dizia "reordenar não corta FP — o ganho é o VET final". A auditoria
  do pulmão confirma o oposto-complementar: no **quantitativo puro**, o VET não é o gargalo (0 FP); o
  gargalo é **recall** de padrões não-numéricos. Ou seja, a prioridade FP-vs-recall depende da
  especialidade (tireoide=precisão/FP; pulmão=recall).
</content>
