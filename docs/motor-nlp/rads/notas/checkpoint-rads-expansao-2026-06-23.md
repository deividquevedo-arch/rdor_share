# Checkpoint — xxRADS: BI-RADS consolidado + expansão PI/TI-RADS (2026-06-23)

Documento para retomar sem perder contexto. Sucede `checkpoint-birads-bancada-ab-2026-06-19.md`.

## 1. Onde tudo vive

| Item | Local |
|---|---|
| **Lib do motor (canônica)** | `nlp-engine-lib/` (`release/v0.1.1`), pacote `src/nlp_engine/nlp_engine/`. Sem `.venv` próprio → usa `../fabrica-ia-lib/.venv/Scripts`. |
| **Configs (fonte)** | `fabrica-ia-plataforma/configs/nlp/{mama,prostata,tireoide}/config.yaml`. BI-RADS também em `.py` no runner. |
| **Runner E2E** | `fabrica-ia-plataforma/apps/databricks/nlp_engine/ntb_ia_motor_e2e.py` (widget `specialty`). |
| **Bancada (PHI, gitignored)** | `nlp-engine-lib/bancada/` — `run_ab_birads.py`, `run_ab_rads.py`, `pull_rads_sample.py`, `sample_*.jsonl`, `divergencias_*.csv`. |
| **Referências do lake** | BI-RADS: `diamond_birads.birads.tb_diamond_mod_birads_{entrada,saida}` (`vl_proced_birads`). PI/TI: `diamond_pirads.pirads` / `diamond_tirads.tirads` (`tb_diamond_mod_{pirads,tirads}_{entrada,saida}`, categoria nas colunas `pirads`/`tirads`, escala -1..6). |

## 2. BI-RADS — CONSOLIDADO e COMMITADO

- **Fixes (só no pattern, sem mudar a lib):** tolerância a grafia (`BR-RADS`/`BRADS`/R duplicado) e separador com `()` (recuperou um **BI-RADS 6 de carcinoma**). Config `0.1.0-birads-rads-v2`.
- **Métricas (categoria, vs legado):** repr **99,9%** / estrat **99,6%** (motor) vs 99,6% / 96,1% (legado). Motor vence 12 de 14 desacordos. Relatório: `Relatorio-homologacao-birads-bancada-v1.md`.
- **Super-agregação:** 2 heurísticas tentadas (exclusão por contexto; scoping por conclusão) — **ambas regrediram** → revertidas. **Limitação aceita** (2 casos; motor 99,8%+).
- **4 ids corrompidos pelo Excel:** recuperados (todos `igual`); CSV agora grava `id_exame` como `="<id>"` (Excel-safe).

**Commits locais (sem push):**
- `fabrica-ia-plataforma` `d572a49` — config birads (grafia + parênteses).
- `nlp-engine-lib` `f8ad8d4` — bancada CSV Excel-safe + `.gitignore` PHI.
- `nlp-engine-lib` `9c2cfe4` — footer Fix B (preserva conclusão/BI-RADS; afeta todas especialidades).
- `Projects` `3ea9d59` + `5ee2675` — relatório BI-RADS + registro das tentativas.

## 3. Expansão PI/TI-RADS — FEITO (NÃO commitado ainda)

- **Patterns evoluídos** (grafia, parênteses, zero à esquerda, prefixo TR, captura `\d` p/ auditar fora-de-faixa) nos YAMLs `prostata`/`tireoide`, `enabled: true`.
- **Teste sintético** `nlp-engine-lib/tests/nlp_engine/test_pirads_tirads_synthetic.py` — **12 passed** (inclui TR6).
- **Amostras reais do lake** (via `pull_rads_sample.py`, Statement Execution API): `sample_{pirads,tirads}_{repr,strat}.jsonl` (~2,5k laudos, gitignored).
- **A/B rodado** (`run_ab_rads.py`): concordância de categoria **PI ~99% / TI ~96–97%**; CSVs de divergência prontos p/ avaliação clínica. Relatório: `Relatorio-homologacao-pirads-tirads-bancada-v0.md`.
- **Referência do lake = classificador independente** (não é o motor — 0/53.426 com assinatura nlp-engine).
- **Erro dominante:** super-agregação de **legendas/tabelas de categorias** embutidas (mais frequente que no BI-RADS).

**Decisões clínicas** (`decisoes-expansao-rads-pi-ti-v0.md`):
1. **TR6 válido** (câncer confirmado por biópsia) → no config tireoide. TR6 inferido de biópsia (sem "6" escrito) = **achado clínico/léxico**, não extração RADS.
2. **Cat 3** não promove (default), **configurável por especialidade** (navegação decide).
3. **Cat 0** = "termo sem número / não relevante" — não é anomalia; fora do exact-match (acionável = 1–6).

## 4. Estado git — NÃO commitado (expansão)

- `fabrica-ia-plataforma` (`branch-from-versao-alpha`): `?? configs/nlp/{prostata,tireoide}/config.yaml` (na verdade modificados; ver git status — mama/prostata/tireoide untracked).
- `nlp-engine-lib` (`release/v0.1.1`): `?? tests/nlp_engine/test_pirads_tirads_synthetic.py`; bancada gitignored.
- `Projects` (`main`): notas novas (relatório PI/TI, decisões, draft LLM, este checkpoint).

## 5. Próximos passos

1. **Commitar a expansão** (YAMLs prostata/tireoide + teste sintético + notas). [feito ao fim de 23/06? conferir log]
2. **Avaliação clínica** dos CSVs de divergência (BI/PI/TI).
3. **Super-agregação** (legendas/tabelas) — solução transversal: LLM de desambiguação em shadow (`draft-llm-desambiguacao-categoria-rads-v0.md`) OU scoping de conclusão com validação rigorosa.
4. **Léxico de findings** do tireoide p/ TR6 inferido por biópsia.
5. **Configs `.py` do runner** p/ PI/TI (hoje só YAML) → rodar E2E em HML (entrada do lake → process → homolog).
6. **Política de promoção cat 3** por especialidade (alinhar com navegação).

## 6. Como retomar (comandos)

```bash
cd nlp-engine-lib
VENV=../fabrica-ia-lib/.venv/Scripts
# gates da lib:
$VENV/python.exe -m pytest tests/nlp_engine -q ; $VENV/ruff.exe check src/nlp_engine ; $VENV/mypy.exe
# bancada A/B (re-roda sobre amostras locais):
$VENV/python.exe bancada/run_ab_birads.py sample_birads_repr2.jsonl
$VENV/python.exe bancada/run_ab_rads.py pirads repr   # | strat ; tirads repr|strat
# re-puxar amostra do lake (PHI -> bancada/):
$VENV/python.exe bancada/pull_rads_sample.py pirads repr
#   API: MSYS_NO_PATHCONV=1 databricks api post /api/2.0/sql/statements -p adb-2013197995950192 (warehouse 7faeb23ac32d8fbf)
```
```
Referência clínica das escalas: docs/motor-nlp/doc-regras-clinicas-rads-v0.md
```
