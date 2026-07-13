# Nota — Paridade BI-RADS: motor RADS vs legado (v0)

**Status:** bench sintético verde; validação HML pendente (shadow).
**Vinculado a:** `doc-plano-implementacao-rads-extraction-v0.md` (Fase 4), `doc-regras-clinicas-rads-v0.md`, `doc-playbook-global-evolucao-paridade-v0.md`
**Config piloto:** `fabrica-ia-plataforma/configs/nlp/mama/config.yaml`
**Legado:** `algoritmos/birads/model/ntb_ia_predicao.py` → coluna `vl_proced_birads` / `birads`

---

## 1. Objetivo

Comparar extração **BI-RADS** do motor (`rads_max_by_system.bi_rads` / promoção) com o legado (`max` de números na janela ±3 palavras), classificando divergências antes de shadow em HML.

---

## 2. Bench automatizado (sintético, sem PHI)

Arquivo: `fabrica-ia-lib/tests/nlp_engine/test_birads_parity_synthetic.py`

| Caso | Legado (aprox.) | Motor | Classificação |
|---|---|---|---|
| BI-RADS 4 | 4 | 4 | Paridade |
| BI-RADS 2 + 5 | 5 | 5 | Paridade (max) |
| BI-RADS IV | 4 | 4 | Paridade (romano) |
| BI-RADS 4 A | 4 | 4A | Melhoria (subcategoria) |
| BI-RADS 9 + 4 | -1 ou 4* | 4 | Melhoria (regra do 9 auditada) |
| BI-RADS descartado | conta número | não promove | Melhoria (negação) |

\* Legado trata "9" com regra especial quando há 2 números.

**Gate local:** `pytest tests/nlp_engine/test_birads_parity_synthetic.py` — 55+ testes na suíte `tests/nlp_engine`.

---

## 3. Protocolo HML (próximo passo)

1. **Freeze baseline** — amostra anonimizada de `diamond_birads.birads.tb_diamond_mod_birads_saida` + `vl_proced_birads`.
2. **Rodar motor** com `configs/nlp/mama/config.yaml` (`rads_extraction.enabled: true`) via `nlp_platform.batch.run_homolog`.
3. **Comparar** por `id_exame`: `vl_proced_birads` vs `rads_max_by_system.bi_rads` (normalizar 4A→4 se necessário para comparação grossa).
4. **Classificar** divergências: `bug` | `melhoria_negacao` | `melhoria_subcategoria` | `melhoria_regra_9` | `escopo`.
5. **Gate produção shadow:** 2 rodadas estáveis (`doc-playbook-global-evolucao-paridade-v0.md`).

---

## 4. Decisão preliminar

- **Promover shadow em HML** com `promote_categories >= 4` após rodada HML.
- **Não fazer cutover** da API legado até sign-off clínico (S08) e match_rate estável no homolog.
- Negação e subcategorias 4A/4B/4C são **melhorias esperadas**, não regressões.

---

## 5. Referências SQL (HML)

```sql
-- Cohort motor x legado (ajustar catalog/schema/ambiente)
SELECT
  m.id_exame,
  l.vl_proced_birads AS legado_birads,
  get_json_object(m.exm_laudo_resultado, '$.rads_max_by_system.bi_rads') AS motor_birads
FROM {motor_saida} m
INNER JOIN diamond_birads.birads.tb_diamond_mod_birads_saida l
  ON m.id_exame = l.id_exame
WHERE m.dt_execucao = '{data}'
LIMIT 500;
```
