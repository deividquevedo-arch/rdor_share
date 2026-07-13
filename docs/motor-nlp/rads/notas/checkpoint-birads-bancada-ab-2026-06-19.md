# Checkpoint — xxRADS / Piloto BI-RADS / Bancada A/B (2026-06-19)

Documento para retomar sem perder contexto. Resume onde tudo vive, o que foi feito, o estado de cada repo e os próximos passos.

## 1. Onde tudo vive (mudança importante)

A lib do motor **migrou** de `fabrica-ia-lib` para **`nlp-engine-lib`** (repo dedicado). Versão reiniciou em **0.1.0**; branch de dev **`release/v0.1.1`**.

| Item | Local |
|---|---|
| **Lib do motor (canônica)** | `nlp-engine-lib/` — pacote `src/nlp_engine/nlp_engine/` (import `nlp_engine.nlp_engine.X`). Sem `.venv` próprio → uso o `.venv` do `fabrica-ia-lib` (Python 3.12.10, pytest/ruff/mypy + deps). |
| **Lib legada (não usar p/ motor)** | `fabrica-ia-lib/` (pacote `fabrica_ia.nlp_engine`, era 0.5.x) |
| **Runner + configs** | `fabrica-ia-plataforma/apps/databricks/nlp_engine/` — runner genérico `ntb_ia_motor_e2e.py` (widget `specialty` → carrega `ntb_ia_{specialty}_config.py`). Configs agora são **`.py`** (não mais YAML; YAML em `configs/nlp/` é referência histórica). |
| **Bancada A/B local** | `nlp-engine-lib/bancada/` (gitignored — contém PHI) |
| **Legado BI-RADS** | `algoritmos/birads/model/ntb_ia_predicao.py` (`generate_birads`: janela ±3 palavras, máximo, regra do 9, limpa citações PT) |

## 2. O que foi feito

1. **xxRADS portado para `nlp-engine-lib`** (Fases 0–3 + audit fields) — commit **`41afd4b`** em `release/v0.1.1`. Extractor regex config-driven, integração no `engine.py` (promoção opt-in de `fl_relevante`, `decision_source=rads_promotion`), LLM fallback `alias_without_category`, campos auditáveis (`rads_mentions`, `rads_max_by_system`, `rads_invalid_candidates`, `rads_llm_errors`). 141 testes + ruff + mypy verdes.
2. **Config BI-RADS criada**: `apps/databricks/nlp_engine/ntb_ia_birads_config.py` (`specialty_id="birads"`, catalog `diamond_birads`, legacy `diamond_birads.birads.tb_diamond_mod_birads_{entrada,saida}`, `rule_only`).
3. **Bancada A/B** (`bancada/run_ab_birads.py`): lê config `.py` (stub de `dbutils`), roda `ClinicalNlpEngine` sobre laudos reais do lake e compara com `vl_proced_birads` do legado. Exporta `compare_*.json` + `divergencias_*.csv` (colunas `laudo_limpo` via `to_plain` + `laudo_cru`).
4. **Amostras do lake** (Databricks CLI v0.299.2, perfil `adb-2013197995950192`, warehouse `7faeb23ac32d8fbf`; via Statement Execution API — **usar `MSYS_NO_PATHCONV=1`** no Git Bash senão o path `/api/...` é mangled):
   - `bancada/sample_birads.jsonl` — estratificada (40/categoria × 8 = 320)
   - `bancada/sample_birads_repr.jsonl` — representativa (1000, maio/2026, prevalência ~8,1%)
5. **Homologação parcial manual** do usuário: `bancada/homol_parc - divergencias_sample_birads_repr.csv` — coluna `homologação`: 226 `igual`, **31 `legado`** (motor errou), 3 `motor` (motor certo, legado errou), 1 `??`.
6. **Gaps mapeados e corrigidos** a partir dos 31 `legado` (ver seção 4).

## 3. Sobre os dados (fatos confirmados)

- Laudos no lake (`proced_laudo_exame`) vêm em **mojibake** (UTF-8-como-Latin-1). O motor corrige internamente via `to_plain`→`ftfy` (899/910 `ç`, 442/446 `®` reparados). A extração roda sobre **texto limpo** — pipeline padrão.
- `proced_laudo_limpo` (saída do legado) é **destrutivamente processado** (minúsculas, sem pontuação, stopwords removidas) → **não serve** como input do motor. A entrada `proced_laudo_exame` é a mais completa.
- Legado **envia todos** os exames, incl. `vl_proced_birads = -1` (= "nenhuma categoria BI-RADS encontrada"; 98% dos -1 nem citam BI-RADS). Relevante do legado = `vl_proced_birads >= 4`.

## 4. Gaps corrigidos (motor BI-RADS)

**Fix A — pattern no config** (`configs/nlp/mama/config.yaml` + `ntb_ia_birads_config.py`), 6 melhorias:
1. Subcategoria `\b` — `\s?[ABC]` engolia a 1ª letra da palavra seguinte ("Categoria 2 **A**CR"→inválido).
2. Modalidade opcional `(US|USG|MG|RM|TC|ECO)` entre alias e número ("BI-RADS US 2").
3. `0*` — zero à esquerda ("Categoria 02"→2, antes 0).
4. `\b` após romanos — "BI-RADS v2025" não vira 5.
5. `=` no separador ("BIRADS= 3").
6. **Separador não cruza quebra de linha** (`(?:[^\S\n\r]|[:.=°º®ª-])*`) — evita capturar cabeçalho de seção romano ("...BI-RADS\n\n**V**. Observações"→5).

Pattern final:
```
(?:BI[- _]?RADS|BIRADS|categoria)(?:[^\S\n\r]|[:.=°º®ª-])*(?:(?:US|USG|MG|RM|TC|ECO)\b(?:[^\S\n\r]|[:.=°º®ª-])*)?0*(\d(?:\s?[ABC]\b)?|(?:iv|vi|v|i{1,3})\b)
```

**Fix B — bug na lib** (`nlp-engine-lib/src/nlp_engine/nlp_engine/text_pipeline/footer.py`): a remoção de "referência bibliográfica" casava **qualquer** "referência" com `.*$` (DOTALL) e **apagava a CONCLUSÃO** (e o BI-RADS) quando havia "com referência de estabilidade" no corpo. Restrito a seção de referências (seguida de `:` ou "bibliográfica"). Afeta todas as especialidades → suíte completa rodada: **156 passed**, ruff + mypy verdes.

## 5. Resultado da bancada (pós-fix)

| Métrica | Baseline | **Final** |
|---|---|---|
| Categoria exact-match (representativa, 891 pares) | 0.9815 | **0.9955** |
| Categoria exact-match (estratificada) | 0.923 | **0.948** |
| FN vs legado (representativa) | 1 | **1** (e é erro do legado) |
| 31 casos `legado` anotados | — | **27 corrigidos, 0 divergentes, 4 ids inacessíveis** |
| llm_called / invariant_errors | 0 / 0 | 0 / 0 |

- **FP vs legado ≈ 261 (~26%)**: NÃO é bug de categoria — é **diferença de escopo de relevância** (motor marca por achado *ou* BI-RADS≥4; legado só BI-RADS≥4). **Decisão de produto pendente** (avaliar caso-a-caso se são TPs perdidos pelo legado).
- **4 ids inacessíveis**: o Excel reformatou o `id_exame` numérico ao salvar o CSV anotado — reconferir.

## 6. Estado git (tudo NÃO commitado, sem push)

- `nlp-engine-lib` (`release/v0.1.1`): `M src/nlp_engine/nlp_engine/text_pipeline/footer.py` (Fix B); `bancada/` (gitignored). Commit `41afd4b` (xxRADS) já feito.
- `fabrica-ia-plataforma` (`branch-from-versao-alpha`): `?? apps/databricks/nlp_engine/ntb_ia_birads_config.py`; `?? configs/nlp/mama/` (config.yaml com Fix A); `configs/nlp/{prostata,tireoide}` também untracked.

## 7. Próximos passos

1. **Decisão de relevância `birads`**: alinhar ao legado (só BI-RADS≥4) ou manter ampla (achados) — decidir após revisão clínica (cientista-médico) de um lote dos FP BI-RADS 2/3.
2. **Revisão médica** dos casos `igual`/`??` (a homologação atual é manual do usuário, sem revisão do time clínico).
3. **Commit**: Fix B (footer) na lib + config birads no plataforma (aguardando autorização).
4. **Generalização** (Fase 5): PI-RADS (`prostata`) e TI-RADS (`tireoide`) — configs YAML já existem; aplicar os mesmos aprendizados de pattern.
5. **Wheel**: só publicar `nlp-engine` (0.1.1) após bancada validada.

## 8. Como retomar a bancada

```bash
cd nlp-engine-lib
VENV=../fabrica-ia-lib/.venv/Scripts
$VENV/python.exe bancada/run_ab_birads.py sample_birads_repr.jsonl   # representativa
$VENV/python.exe bancada/run_ab_birads.py sample_birads.jsonl        # estratificada
# gates da lib:
$VENV/python.exe -m pytest tests/nlp_engine -q ; $VENV/ruff.exe check src/nlp_engine ; $VENV/mypy.exe
```
Re-puxar amostra do lake: ver query em `bancada/run_ab_birads.py` / histórico; usar `MSYS_NO_PATHCONV=1 databricks api post /api/2.0/sql/statements -p adb-2013197995950192 ...`.
