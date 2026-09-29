# Decisão de arquitetura — injeção de `base_url`/token do LLM para a camada quantitativa (runner e2e)

**Data:** 2026-07-14 · **Escopo:** `nlp_engine` + runner e2e (`ntb_ia_motor_e2e.py`) · **Status:** aplicado
(commit `0d2a53d`, branch `feature/transplante_pulmao`). **Para revisão do time (MLOps/plataforma).**

## Contexto / problema
A especialidade **Transplante de Pulmão (V1)** é **quantitativa pura**: a relevância vem só de limiares
numéricos (VEF1/CVF/DLCO em % do previsto) na camada `quantitative_criteria` do motor
(`on_met: promote`). Não queremos **embeddings** (ruído, custo) nem o **llm_router de relevância** —
apenas o **extrator quantitativo**, que usa o LLM só para **extrair a medida** (valor+unidade), enquanto
o **código** aplica o limiar.

O extrator quantitativo precisa de **dois valores de RUNTIME**, que **só o contexto de execução**
(notebook/cluster) conhece e que **não podem** vir do config (são específicos do workspace/sessão) nem
ser hardcoded:
- `llm_router.base_url` = `https://<workspace-host>/serving-endpoints`
- token de acesso no ambiente (`DATABRICKS_TOKEN`, via `ensure_databricks_token_env`)

**O gargalo:** o `apply_runtime_profile` (biblioteca `fabrica-ia-lib`, `nlp_platform/batch/adapters.py`)
só propaga `base_url`/token para o `nlp_cfg["llm_router"]` no perfil **`llm_http`** — que **força
`use_embeddings=True`** e liga o **llm_router de relevância**. Não existe hoje um perfil "LLM só para
medição, sem embeddings/relevância".

## Alternativas consideradas
| Opção | Por que NÃO |
|---|---|
| Config-only (base_url/token no `.py` do config) | Valores de runtime/workspace → hardcode; quebra entre dev/hml/prd; token em config = exposição de segredo. |
| Alterar `apply_runtime_profile` (fabrica-ia-lib) | Fora do nosso escopo (lib de outro time). |
| `llm_http` + `uncertainty_band` impossível + `similarity_threshold`≈1.0 | Funciona, mas embeddings ainda **computam** a cada laudo (desperdício) e é config "hack". |
| Fallback ambiente no `nlp_engine` via **Databricks SDK** (`WorkspaceClient` autoresolve host+token) | Acopla o `nlp_engine` ao Databricks (hoje é agnóstico de plataforma, testável sem Bricks). |

## Decisão (A) — estender a injeção de contexto no runner
Injetar `base_url`/`api_key_env` no `nlp_cfg["llm_router"]` **também quando há `quantitative_criteria`**,
em qualquer perfil — reusando o mecanismo que o runner **já executa** para `llm_http`. Assim uma
especialidade quantitativa-pura roda `profile=rule_only` (sem embeddings, sem llm-router-relevância) e o
extrator quantitativo ainda alcança o LLM.

**Racional:** injetar contexto-de-runtime no config é, por definição, papel da **camada de runtime**
(o runner). Não é um hack — é a **mesma responsabilidade** que o runner já cumpre para `llm_http`
(linhas ~409 base_url e ~497 token). Estamos apenas estendendo o gatilho de `profile=="llm_http"` para
`profile=="llm_http" OR tem quantitative_criteria`.

## Mudança (código)
`apps/databricks/nlp_engine/ntb_ia_motor_e2e.py`, logo após `apply_runtime_profile`:

```python
nlp_cfg = apply_runtime_profile(_conf["nlp"], cfg.runtime)
# A camada quantitativa usa o LLM APENAS p/ EXTRAIR a medida — independe do perfil e de embeddings.
# Garante base_url + token do LLM ao extrator mesmo FORA do llm_http (ex.: especialidade
# quantitativa-pura em rule_only), SEM ligar embeddings nem o llm_router de relevância.
_precisa_llm = cfg.runtime.profile == "llm_http" or bool(nlp_cfg.get("quantitative_criteria"))
if _precisa_llm:
    _lr = nlp_cfg.setdefault("llm_router", {})
    _lr.setdefault("base_url", llm_base_url)                      # setdefault: não altera llm_http
    _lr.setdefault("api_key_env", cfg.runtime.llm_router.api_key_env)
    ensure_databricks_token_env(dbutils, _lr["api_key_env"])
    print("llm base_url (quant/llm_http):", nlp_cfg.get("llm_router", {}).get("base_url"))
```

## Compatibilidade / impacto
- **Byte-compat:** `setdefault` **não sobrescreve** `llm_http` (onde o `apply_runtime_profile` já pôs o
  base_url); especialidades **sem** `quantitative_criteria` não entram no ramo → comportamento idêntico.
- **Reusável:** qualquer especialidade quantitativa futura herda isso automaticamente (config-in: basta
  ter `quantitative_criteria`).
- **Não acopla o `nlp_engine`** a Databricks; **não** toca `fabrica-ia-lib`.

## Recomendação de longo prazo para o time (fora do nosso escopo)
Avaliar, na `fabrica-ia-lib` (`apply_runtime_profile`), **desacoplar a injeção de `base_url`/token do
perfil** (propagar em qualquer perfil) — ou introduzir um perfil de 1ª classe tipo `llm_measure`
("LLM só para medição, sem embeddings/relevância"). Isso tornaria o ajuste do runner desnecessário e
padronizaria o caso quantitativo para todas as especialidades. Dono: time da `fabrica-ia-lib`/MLOps.

---

# ADENDO — 2026-07-29 · a costura se perdeu na migração; decisão revista (lib `0.6.4`)

**Status:** aplicado na `nlp-engine` `0.6.4` (local, sem push).

## O que aconteceu
A plataforma nova (`fabrica-ia-nlp-platform`) **não portou** esta injeção. Verificado nas três branches
(`hml`, `feature/pipelines`, `feature/app`), inclusive nos commits de 2026-07-29: nenhuma escrita em
`os.environ` além de `PIP_CONFIG_FILE`, nenhum `spark_env_vars`, nenhum `base_url`. A própria doc deles
admite em `boas-praticas/05-llm-embeddings-e-custo.md` §4.8 (*"este runner não injeta o `base_url`… o
runner legado fazia isso explicitamente"*), e `docs/TODO.md` §3 mantém o item do token **aberto**.

Efeitos colaterais medidos:
- `llm_router.enabled` tem default `false` na lib e o runner novo **ignora `runtime`** — a config de
  hepatologia deles só declara `enabled` em `runtime.llm_router` → **o piloto rodou sem juiz LLM**;
- `plataform/README.md:51` ainda **afirma** que a injeção acontece (resíduo do repo antigo), o que
  contradiz o §4.8 e induz ao erro.

## O que mudou na premissa
A alternativa *"fallback de ambiente via Databricks SDK"* foi **rejeitada** acima por acoplar o
`nlp_engine` ao Databricks. O head confirmou em 2026-07-29 que **a lib é consumida exclusivamente no
Databricks**, o que remove o custo dessa objeção. Some-se: a plataforma nova **já usa `databricks-sdk`**
(`plataform/observability/ntb_ia_ml_run.py:43-45`, import lazy dentro de `try`) — o padrão já é deles.

## Decisão revista (B) — fallback nativo NA LIB, como última camada
Precedência na resolução da conexão: **(1) config → (2) env var → (3) `databricks-sdk`**.

- Camada 3 só age quando 1 e 2 não resolvem → **aditiva**; a Decisão (A) acima continua válida e tem
  precedência onde o runner a implementa (byte-compat com os pilotos homologados).
- **Memoizada no módulo, inclusive a falha** — `call_openai_compatible_chat` roda **por laudo** e o
  construtor do SDK pode fazer I/O de rede (confirmado: bloqueia offline). Sem cache, um lote de
  milhares de laudos pagaria isso por chamada.
- Memoiza o `Config`, **não** o token: `authenticate()` devolve headers frescos, evitando expiração no
  meio do lote.
- **Import lazy em `try`** + `databricks-sdk` **não declarado** como dependência (vem pré-instalado no
  DBR) → a lib segue importável e a suíte roda **sem** o SDK (testes injetam um fake em `sys.modules`).
- **Opt-out** `native_auth_fallback: false` preserva o erro explícito.

**Custo aceito:** a agnosticidade *de execução* é relativizada (há um caminho que só funciona no
Databricks), mas a agnosticidade *de import e de teste* é preservada. Registrado aqui para não parecer
contradição com a tabela de alternativas acima — é a **mesma** alternativa, reavaliada sob premissa nova.

## Consequência prática
Das 3 travas para usar LLM na plataforma nova, **2 saem do caminho crítico do MLOps**:

| Trava | Onde resolve agora |
|---|---|
| `base_url` ausente | ✅ lib `0.6.4` (camada 3) |
| token não populado | ✅ lib `0.6.4` (camada 3) |
| `llm_router.enabled` default `false` | ✅ nossa config (explicitar) |

Segue **pendente de validação real em `dev` no Databricks** — o SDK não é testável fora do Bricks; a
suíte cobre a lógica com fake. Teste de aceitação (o que a doc deles prescreve): `llm_called` no
`exm_laudo_resultado` + medidas na trilha (`emit_decision_trail: true`).

O pedido ao MLOps deixa de ser bloqueio e passa a informativo: corrigir `plataform/README.md:51`,
incluir `enabled: True` no exemplo do §3.2, e ciência de que o piloto de hepatologia rodou sem juiz.
