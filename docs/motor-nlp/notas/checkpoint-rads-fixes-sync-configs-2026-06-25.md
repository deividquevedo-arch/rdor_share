# Checkpoint — xxRADS: fixes (™ + legenda) + sync repos + configs .py do runner (2026-06-25)

Documento para retomar sem perder contexto. Sucede `checkpoint-rads-expansao-2026-06-23.md`.

---

## ★ ATUALIZAÇÃO (continuação 2026-06-25) — lote A/B/C + critério BI-RADS + llm_fallback

Após a homologação manual do TI-RADS (ver `homologacao-manual-tirads-parcial-v0.md`) e o CSV
consolidado (`bancada/divergencias_consolidado.csv`, 280 div.), evoluímos as configs para **v4**.

**1. Critério BI-RADS = legado (decisão de negócio):** `fl_relevante=1` SÓ p/ cat ≥ 4; **achados
clínicos desativados** (`findings: {}`, reversível — léxico preservado em comentário). A/B vs legado:
match **0,738 → 0,999** (FP 261 → 0). Os 261 FP eram 100% `decision_source=rule` (achados benignos
cat 2/3 + procedimentos). NÃO reativar achados até a navegação definir.

**2. Lote A/B/C (determinístico, config-only, zero regressão):**
- **A — negação não apaga grau RADS:** `rads_extraction.negation.tokens: []` nos 3 sistemas (grau é
  asserção; `"sem calcificações (TIRADS 4)"` não nega). Recupera FN do TI; 0 regressão BI/PI.
- **B — composto `"N e M"`:** pattern extra captura o 2º número (`"TI-RADS 3 e 4"` → max 4). PI/TI.
- **C — palavra `"Categoria/Cat"`:** tolerada entre alias e número no grupo de palavra. PI/TI
  (BI-RADS já trata via alias `categoria`).

**3. Métricas finais (pós-lote):**
| Sistema | exact-match | FP | FN | match relevância |
|---|---|---|---|---|
| BI-RADS | 0,9955 | 0 | 1 | **0,999** (= legado) |
| PI-RADS repr | 0,9951 | 1 | 1 | 0,998 |
| PI-RADS strat | 0,9948 | 0 | 1 | 0,9961 |
| TI-RADS repr | 0,9928 | 1 | **0** | **0,999** |
| TI-RADS strat | 0,9854 | 0 | 3 | 0,9897 |

(TI strat FN=3: 2 são caso **D** = erro da referência, romano `TIRADS I` + ref super-agregou; 1 é
`categoria+FN` ainda não revisado.)

**4. llm_fallback de SUPORTE (staged, isonomia c/ hepato):** trigger `alias_without_category` — só
dispara se o alias está presente e o regex NÃO resolveu a categoria. **ADITIVO** (nunca altera a
extração regex; só recupera FN — ex.: romano, cauda longa). Conexão p/ Databricks **Haiku 4.5**
(`model=databricks-claude-haiku-4-5`, `api_key_env=DATABRICKS_TOKEN`; base_url vem do runner).
**`enabled:false`** (staged). **Validado LOCAL com a OpenAI do usuário** (`gpt-4o-mini`):
`"(TIRADS I)" → TR1`. Backend `call_openai_compatible_chat` (`base_url` + `v1/chat/completions` +
`api_key_env`). Local override: `base_url=https://api.openai.com`, `model=gpt-4o-mini`,
`OPENAI_API_KEY` (via env; key guardada só no scratchpad da sessão).

**🔐 SEGURANÇA:** a chave OpenAI foi colada no chat → **usuário deve ROTACIONAR** (está no histórico).
Nunca escrita em config/commit; só no scratchpad temporário + referenciada por env var.

**Commits locais (sem push):** plataforma `c7b17c1` (critério BI + A/B/C) · `95315e3` (llm_fallback) ·
lib `5fc763c` (testes, suite 164). Tudo em `feature/rads-config-homolog` (plataforma) e
`release/v0.1.1` (lib).

**Próximos:** (a) resolver `# CONFIRMAR` dos `.py` (column_map / `data.legacy` refs do lake PI/TI);
(b) E2E em HML; (c) **ligar `llm_fallback` (enabled:true)** após validar em HML com Haiku 4.5;
(d) revisar o 1 `categoria+FN` residual do TI strat. TR6-biópsia segue em aberto.

---

## 1. O que foi feito nesta sessão

1. **Avaliação dos CSVs de divergência** — 34 divergências PI/TI classificadas. Descobertas:
   - Super-agregação vem de **legendas/tabelas que enumeram TODAS as categorias** (1..5), não de prosa ambígua.
   - A **referência do lake também super-agrega** (mesmo bug) → alguns "FN/divergências" são erro da referência, motor correto.
2. **Fix B — marca registrada ™→"TM"** (config-only): `PI-RADSTM 4`/`TI-RADSTM: 4` quebravam o regex (FN total). Inserido `(?:TM)?` no pattern. Configs v2.
3. **Fix A — filtro de legenda** (lib, opt-in, default off): `aggregation_legend_filter` — corrida contígua estritamente ascendente ≥`min_run`(4) iniciando na cat mínima = legenda → excluída de `max_by_system` E da promoção `fl_relevante`; auditável em `rads_legend_mentions`. **BI-RADS byte-idêntico** (filtro off, provado via stash). Configs v3.
4. **Sync dos 4 repos** (ver §3).
5. **CSV consolidado** das validações: `nlp-engine-lib/bancada/divergencias_consolidado.csv` (280 divergências; gerado por `bancada/consolidate_divergencias.py`, gitignored).
6. **Configs .py do runner** para PI/TI (ver §4).

### Métricas A/B finais (motor vs referência)
| Fatia | exact-match | FP | divergências |
|---|---|---|---|
| pirads repr | 0.9899 → **0.9951** | 4 → **1** | 7 → **3** |
| pirads strat | 0.9946 → **0.9948** | 1 → **0** | 3 → **1** |
| tirads repr | 0.9648 → **0.9890** | 5 → **1** | 11 → **5** |
| tirads strat | 0.9706 → **0.9801** | 1 → **0** | 13 → **9** |
| **BI-RADS (vs LEGADO)** | categoria **0.9955** | FP_vs_legado **261** | 262 |

> BI-RADS: 261 FP_vs_legado são **100% `decision_source=rule`** (motor de regras mais sensível que o legado em benignos cat 2/3 e procedimentos), **não** a extração RADS. É diferença de definição de "relevante", não bug.

## 2. Commits (todos LOCAIS, sem push)

- **nlp-engine-lib** (`release/v0.1.1`): `53a9931` (teste ™), `56a6966` (Fix A filtro de legenda). Gates verdes (ruff/mypy/**159 testes**).
- **fabrica-ia-plataforma** (`feature/rads-config-homolog`): `8c85bf1` birads grafia · `8f24236` PI/TI enable · `c0e9a25` fix ™ · `f243052` filtro legenda v3 · `1d10c3f` WIP preservado · `24bd506` **configs .py PI/TI**.

## 3. Estado dos repos (pós-sync)

| Repo | Branch atual | Estado | Nosso trabalho |
|---|---|---|---|
| **nlp-engine-lib** | `release/v0.1.1` | Fixes A+B commitados, sem push | — |
| **fabrica-ia-lib** | `hml` (limpa, = origin/hml) | referência de componentes (`data_manager` íntegro) | arquivado em `feature/nlp-engine-rads-extraction` |
| **fabrica-ia-plataforma** | `branch-from-versao-alpha` = espelho do time (HEAD `09cb2d8`) | nosso trabalho isolado | `feature/rads-config-homolog` (rebasada sobre a base nova) |
| **Projects** (rdor_share) | `main` | checkpoints/notas | — |

Decisões: git push/PR só com autorização explícita. fabrica-ia-lib deixou de ser fonte do motor (canônico = nlp-engine-lib).

## 4. Configs .py do runner (E2E HML)

**NÃO há mais gerador** (`tools/nlp_config` saiu). Os `.py` são fonte-de-verdade, escritos à mão copiando `apps/databricks/nlp_engine/ntb_ia_template_config.py`. O runner `ntb_ia_motor_e2e.py` carrega via `dbutils.notebook.run("ntb_ia_{specialty}_config")` (retorna o dict por `dbutils.notebook.exit(json.dumps(CONFIG))`).

**Nomenclatura RADS** (decisão do usuário): specialty = `birads`/`pirads`/`tirads`; catalog `diamond_{birads,pirads,tirads}`; tabelas `tb_diamond_mod_{birads,pirads,tirads}_*`.

Arquivos em `apps/databricks/nlp_engine/` (branch `feature/rads-config-homolog`):
- `ntb_ia_birads_config.py` (já existia; v2)
- `ntb_ia_pirads_config.py` — **novo** (v3, fix ™ + filtro legenda)
- `ntb_ia_tirads_config.py` — **novo** (v3, fix ™ + filtro legenda, TR6)

Validados local: exec + `json.dumps` + `config_loader.load` + smoke de extração (legenda completa filtrada → achado prevalece; ™ capturado).

### ⚠️ Pendências `# CONFIRMAR` antes de rodar E2E em HML
1. **`data.column_map`** — alinhei ao birads (gold compartilhado). Validar nomes físicos das colunas no gold de próstata/tireoide.
2. **`data.legacy.{entrada_ref,saida_ref}`** — usei `diamond_{pirads,tirads}.{pirads,tirads}.tb_diamond_mod_{pirads,tirads}_{entrada,saida}` (do checkpoint anterior). Se o `saida_ref` não tiver schema compatível com `run_homolog`, trocar `legacy.enabled=False` (runner força `fonte_staging=motor_gold` e pula a homolog).
3. Nota: os YAMLs `configs/nlp/{prostata,tireoide}` ainda têm catalog `diamond_{prostata,tireoide}` (nomenclatura de órgão) — são só referência; o `.py` (RADS) é a fonte-de-verdade.

## 5. Resíduo técnico em aberto (menor, decisão própria)
- **Negação falso-positivo**: `"sem calcificações (TIRADS 4)"` nega o grau indevidamente (janela=5, token "sem").
- **Algarismos romanos**: `TI-RADS V` não capturado (`roman_to_arabic` off no tireoide).
- **`output_invariants`** do canônico ainda não valida o novo campo `rads_legend_mentions` (follow-up).
- **TR6 por biópsia**: em aberto, **sem definição de negócio** — manter extensível, não implementar.

## 6. Como retomar
```bash
# bancada A/B (lib release/v0.1.1 + plataforma feature/rads-config-homolog):
cd nlp-engine-lib; VENV=../fabrica-ia-lib/.venv/Scripts
$VENV/python.exe bancada/run_ab_rads.py pirads repr   # | strat ; tirads repl|strat
$VENV/python.exe bancada/run_ab_birads.py sample_birads_repr2.jsonl
$VENV/python.exe bancada/consolidate_divergencias.py  # gera divergencias_consolidado.csv
# validar os configs .py (exec + config_loader): ver §4.
```
Próximos: resolver os `# CONFIRMAR` (§4) → rodar E2E em HML (runner `ntb_ia_motor_e2e`, widget specialty=pirads|tirads); avaliar CSV consolidado com o time; depois merge `feature/rads-config-homolog` → `branch-from-versao-alpha`; e merge nlp-engine-lib → hml + publicar wheel.
