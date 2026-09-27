# Nota técnica — LLM não é alcançável no runner da `fabrica-ia-nlp-platform`

**Para:** time MLOps / plataforma · **De:** time DS (motor NLP) · **Data:** 2026-07-28,
**revisada 2026-07-29**
**Escopo:** `fabrica-ia-nlp-platform`, branch `hml` (verificado até commit `9a8be1a`, 29/07 09:34)
**Objetivo:** destravar a migração das 3 configs homologadas (tirads, hepatologia, transplante_pulmao).
**Precedente:** [`doc-decisao-runner-llm-quantitativo-v0.md`](../design/doc-decisao-runner-llm-quantitativo-v0.md) (2026-07-14).

---

## ✅ O que mudou nesta revisão — a documentação que vocês pediram está pronta

Na agenda de 28/07 vocês apontaram que a doc da lib não permitia compreender os métodos para executar
os ajustes. **Entregue na `nlp_engine` 0.6.4** (mergeada na `hml` da `nlp-engine-lib`):

| Entrega | Onde |
|---|---|
| **Contrato de conexão LLM** — os 3 requisitos de runtime, tabela de **erros exatos** e teste de aceitação | `docs/REFERENCIA-PARAMETROS.md` **§7.1** |
| Ponto de entrada rápido (mesma tabela, resumida) | `README.md` → seção *LLM router* |
| `llm_router_backend` completo — mapa do módulo por grupo funcional + *"o que esta camada NÃO faz"*; 0 elemento sem docstring | docstrings do módulo |
| Convenção **Google** (`Args`/`Returns`/`Raises`/`Example`) — a mesma que vocês usam — com **gate no CI** (`ruff D`) | `pyproject.toml` |
| Os 10 steps da cascata documentados, com as armadilhas no ponto de uso | `decision_pipeline.py` |

Corrigimos também uma frase **nossa** que induzia ao erro exatamente aqui: a REFERENCIA anunciava
`base_url` como *"injetado pelo runner"* — a suposição falsa que este documento trata.

**Consequência:** a documentação **deixou de ser o gargalo**. O que resta é a mudança no runner
(requisitos 2 e 3 abaixo), e a §7.1 é a referência que a descreve com precisão.

---

## Resumo em 3 linhas

No runner atual da plataforma nova, **nenhuma chamada de LLM acontece** — nem o juiz (`llm_router`) nem o
extrator quantitativo. São **3 travas independentes**; duas são verificáveis só lendo o repositório.
**A trava 1 é nossa** (config) e **já foi endereçada**; as travas **2 e 3 são do runner** — a 3 depende de
uma decisão de vocês que **já está aberta no `docs/TODO.md` §3**.

> Consequência imediata, e o motivo desta nota: **o piloto de hepatologia rodou sem o juiz LLM.**
> Não é regressão que estamos introduzindo — é diagnóstico do estado atual.

---

## Trava 1 — `llm_router.enabled` (definitiva, sem ressalva)

A lib decide em `decision_pipeline.py:112`:

```python
v = cfg.get("enabled", False)      # default FALSE
```

No repositório antigo quem ligava era `apply_runtime_profile` (`fabrica-ia-lib`), a partir de
`runtime.profile: 'llm_http'`. **O runner novo ignora o bloco `runtime` inteiro** — e a documentação de
vocês confirma isso explicitamente (`boas-praticas/01-fundamentos-e-limites.md:106`):

> «`runtime.*` | ninguém lê; o que vale é `nlp.llm_router`»

E em `plataform/config/speciality/ntb_ia_hepatologia_config.py`, `nlp.llm_router` **não tem `enabled`**
(tem `mode`, `provider`, `model`, `uncertainty_band`, `fallback_policy`, prompts). O `enabled: True`
existe apenas em `runtime.llm_router` — que não é lido.

→ **O router nem tenta chamar o LLM.** O juiz nunca rodou nesse piloto.

### Origem provável: armadilha na própria documentação
O exemplo canônico de `boas-praticas/05-llm-embeddings-e-custo.md` §3.2 **não inclui `enabled`**. Quem o
seguir ao pé da letra fica com o router desligado silenciosamente. (O `enabled: True` só aparece em
`boas-praticas/03-calibracao-do-lexico-e-regras.md` §4.7.)

**Sugestão:** acrescentar `'enabled': True` ao exemplo do §3.2.

---

## Trava 2 — `base_url` (vocês já documentaram)

`boas-praticas/05-llm-embeddings-e-custo.md` §4.8 é explícito:

> «Este runner **não injeta** o `base_url` do serving LLM. O runner **legado** fazia isso explicitamente
> para o caso quantitativo em perfil `rule_only`. Consequência: se o `nlp_engine` não conseguir resolver o
> endpoint (via `llm_router.base_url` ou a env var `DATABRICKS_SERVING_ENDPOINT_BASE`), o extrator
> quantitativo **não extrai nada** — e como `fallback_policy` é `keep_current`, isso passa
> **silenciosamente**.»

Confirmado na lib (`llm_router_backend.py:311,318`):

```python
raw_base = cfg.get("base_url") or os.environ.get("DATABRICKS_SERVING_ENDPOINT_BASE", "")
...
if not base or not model_chain:
    return "", "missing_base_url_or_model"
```

Afeta **juiz e extrator**.

---

## Trava 3 — token na env var (a única que depende de vocês)

`llm_router_backend.py:315`:

```python
api_key = os.environ.get(key_env, "").strip() or os.environ.get("DATABRICKS_TOKEN", "").strip()
```

No repositório inteiro, a **única** escrita em `os.environ` é `PIP_CONFIG_FILE`
(`plataform/config/ntb_ia_dependencies.py:111`). Não há `spark_env_vars` em nenhum arquivo de `jobs/`.

**Por que não resolvemos na config:** a config de especialidade é carregada via
`dbutils.notebook.run()` (`ntb_ia_loader.py:73`) — **processo separado**. `os.environ` definido nela não
propaga; só o JSON do `dbutils.notebook.exit` atravessa. E devolver o token *dentro* do JSON faria a
credencial entrar em `config_loader.config` → params do MLflow, telemetria e saída persistida. Não é
opção.

### ❓ Pergunta objetiva (é só isto que precisamos de vocês)

1. **A cluster policy `000ABC123DEF4567`** (`jobs/ambientes/hml.json`) define `spark_env_vars` com
   `DATABRICKS_TOKEN` e/ou `DATABRICKS_SERVING_ENDPOINT_BASE`? Não conseguimos ver — a policy vive no
   workspace, fora do repo.
2. Se **não**: qual mecanismo vocês preferem, dado que o próprio `docs/TODO.md` §3 registra a preferência
   por *«secret scope / identidade do job»* e alerta contra *«variável de ambiente estática de longa
   duração no cluster»*? O job já roda como `conta-servico-ia@rededor.com.br`.

Concordamos com a ressalva do TODO §3 — **não** estamos pedindo PAT estático em env var de cluster.

---

## Sugestão de correção (4 linhas, com precedente)

O mecanismo já existe e já foi decidido em 2026-07-14
([`doc-decisao-runner-llm-quantitativo-v0.md`](../design/doc-decisao-runner-llm-quantitativo-v0.md)) — só não foi
portado para a plataforma nova. Em `plataform/ntb_ia_motor_e2e.py`, após montar o `nlp_cfg`:

```python
from fabrica_ia.nlp_platform.batch.adapters import ensure_databricks_token_env

# host do próprio workspace → serving endpoints (mesma derivação do runner legado)
workspace_host = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().value()
)

# A camada quantitativa usa o LLM APENAS p/ EXTRAIR a medida — independe do perfil e de embeddings.
_precisa_llm = bool(nlp_cfg.get("llm_router", {}).get("enabled")) or bool(nlp_cfg.get("quantitative_criteria"))
if _precisa_llm:
    _lr = nlp_cfg.setdefault("llm_router", {})
    _lr.setdefault("base_url", f"https://{workspace_host}/serving-endpoints")
    _lr.setdefault("api_key_env", "DATABRICKS_TOKEN")
    ensure_databricks_token_env(dbutils, _lr["api_key_env"])   # já existe em fabrica-ia-lib
```

⚠️ `api_key_env` **precisa ser exatamente** `"DATABRICKS_TOKEN"`: o `ensure_databricks_token_env` só
popula o `os.environ` com esse nome (`adapters.py:205`). Com outro valor o token nunca é setado e toda
chamada falha com `missing_env` — que, com `fallback_policy: positive_in_band`, viraria **falso positivo
em massa**.

`ensure_databricks_token_env` (`fabrica_ia/nlp_platform/batch/adapters.py`) usa o token **da sessão**
(identidade do job), não um PAT estático — alinhado ao TODO §3. `setdefault` preserva o que a config
declarar. Especialidades sem LLM não entram no ramo → comportamento inalterado.

> Nota: o gatilho aqui é `llm_router.enabled` **ou** presença de `quantitative_criteria` — porque o
> extrator quantitativo **não** depende de `enabled`. Nossa config de pulmão roda com
> `llm_router.enabled: False` e ainda assim precisa do LLM só para extrair a medida.

---

## O que nós assumimos do nosso lado (não precisa de vocês)

**Já feito** — `nlp_engine` 0.6.4, mergeada na `hml`:
- documentação completa do contrato de conexão (§7.1) e do `llm_router_backend`;
- gate `ruff D` (convenção Google) no CI, para a doc não regredir.

**Ao migrar as 3 configs** para `plataform/config/speciality/`:

| Ajuste | Motivo |
|---|---|
| `nlp.llm_router.enabled: True` explícito | `runtime.profile` não é lido pela lib (**trava 1**) |
| `nlp.llm_router.api_key_env: 'DATABRICKS_TOKEN'` — movido de `runtime` | §3.2 da doc de vocês |
| `transplante_pulmao`: `data.filters.gold_query` → `data.filters.gold_filter.keywords` | `ntb_ia_gold_filters.py:45` só lê `gold_filter`. Porta é exata: mesma coluna (`proced_descricao`) e mesmo `expr` (`rlike '(?i)%value%'`) |

Sobre `base_url` (**trava 2**), seguimos a decisão de vocês:
- **(a)** declarar `nlp.llm_router.base_url` derivado em runtime na config — caminho que o §4.8 admite; ou
- **(b)** vocês exportarem `DATABRICKS_SERVING_ENDPOINT_BASE` / injetarem no runner — **nossa
  preferência**, porque agrupa `base_url` e token na mesma camada de runtime e mantém a config
  declarativa. É também o que nosso doc de 14/07 concluiu: *injetar contexto-de-runtime no config é
  papel da camada de runtime.*

### Avaliamos resolver na lib e decidimos NÃO fazer

Chegamos a implementar um **fallback nativo** (a lib resolvendo host+token via `databricks-sdk`, no
mesmo padrão de import lazy que vocês já usam em `observability/ntb_ia_ml_run.py:43-45`). Está pronta e
testada, mas ficou **parqueada** por três razões:

1. injetar contexto-de-runtime é papel da camada de runtime — o princípio é o do nosso doc de 14/07;
2. **mascararia o gap**: a injeção existia no runner legado e se perdeu na migração; se a lib compensar
   em silêncio, a próxima plataforma perde de novo e ninguém percebe, porque "funciona";
3. **auditabilidade** — resolver a credencial pela identidade *ambiente* faz um run no workspace errado
   ter sucesso silencioso. Num pipeline clínico com LGPD, falhar alto é a propriedade desejável.

Se vocês preferirem que a lib assuma isso, é só dizer: a implementação existe e sai em uma versão.

---

## Impacto se migrarmos antes da definição

As 3 configs homologadas **dependem de LLM**:

| Config | Uso de LLM | Efeito sem LLM |
|---|---|---|
| `hepatologia` | juiz na banda `[0.35, 0.65]` | perde o desempate; decisão fica só regra+embeddings |
| `tirads` | extrator de medida (nódulo/cisto **≥ 1 cm**) | `require_measure` não atendido → **rebaixa** achados válidos |
| `transplante_pulmao` | **toda** a relevância (`quantitative_criteria`) | **tudo irrelevante, silenciosamente** |

Ou seja: migrar antes produziria comportamento **diferente do homologado**, e no caso do pulmão o
resultado seria zero relevantes sem erro visível. Por isso preferimos alinhar o token primeiro.

---

## Validação combinada (o teste que vocês mesmos prescrevem)

`05-llm-embeddings-e-custo.md` §4.8: rodar em `dev` e conferir **`llm_called`** no
`exm_laudo_resultado`, com `emit_decision_trail: True` para ver as medidas na trilha. Faremos isso nas 3
configs e devolvemos o resultado.

---

## Achado secundário (não bloqueia, mas convém saber)

`plataform/ntb_ia_tirads_config.py` e `ntb_ia_transplante_pulmao_config.py` estão na **raiz** de
`plataform/`, mas o loader resolve caminho fixo (`ntb_ia_loader.py:73`):

```python
self.config = self.load(f"./config/speciality/ntb_ia_{self.specialty}_config")
```

→ Com `specialty=tirads` hoje o resultado é `RuntimeError`. Esses dois arquivos são **código morto**, e
ambos estão em versão **anterior** à nossa homologada (`v22.7` vs `v22.11-v2`; `0.1.1` vs `0.1.2`).
Vamos publicar as versões homologadas em `config/speciality/`. **Podemos remover as da raiz?**
