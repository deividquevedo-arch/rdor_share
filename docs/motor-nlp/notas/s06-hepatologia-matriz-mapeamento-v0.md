# S06 — Matriz de mapeamento legado vs motor (Hepatologia) (v0)

## Escopo

- Especialidade: `hepatologia`
- Backlog: `S03 T03.3` (config YAML), com ciclo de medicao `audit` + `compare`
- Objetivo: rastrear por versao quais metodos/regras existem no legado e no motor, quais gaps permanecem e qual decisao de aceite para cada divergencia.

## Fontes e versoes

| Camada | Fonte/Artefato | Versao/Referencia | Contrato relevante |
|---|---|---|---|
| Legado (gold) | `_local_samples/gold/hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv` | rerodada local 2026-04-23 | `flgRelevante` (`TRUE/FALSE`) |
| Legado (diamond) | `_local_samples/diamond/tb_diamond_mod_hepatologia_saida.csv` | baseline Diamond 2026-04-23 | `fl_relevante` (`true/false`, `1/0`) |
| Motor | `configs/hepatologia/config.yaml` | `0.1.0-piloto` (baseline), `0.1.1-iter1` (iteracao atual) | `findings`, `findings_regex`, negacao, `target_organs` |
| Pipeline compare | `scripts/audit_engine_from_csv.py` + `scripts/compare_hepatologia_audit_vs_legacy.py` + `scripts/audit_legacy_compare.py` | atual | join por `id_exame`; legado S/N por `expected_encaminhamento` ou `flgRelevante` |

## Mapeamento de metodos disponiveis

| Tema | Legado | Motor atual | Gap / observacao |
|---|---|---|---|
| Label binario | `fl_relevante`/`flgRelevante` | `fl_relevante` (saida engine) | Alinhado no compare |
| Lexico focal hepatico | presente no export com analise em blocos | `findings` + `findings_regex` | Pode gerar FP com termos genericos |
| Negacao | embutida no pipeline legado | `negation_phrases` + `negation_window=7` | Alinhado baseline Fase 1 |
| Semantica (embeddings) | possivel em grupos 1b/2 | nao ativo na Fase 1 | Dependencia Fase 2 (S09) |

## Gates de aceite (A/B/C)

- **Gate A (quantitativo minimo):** nao piorar baseline global (`match_rate`) e reduzir erro total (FP+FN) ou justificar clinicamente.
- **Gate B (incerteza legado):** divergencia pode ser aceita sem contar regressao quando houver evidencias de possivel FP/FN legado.
- **Gate C (governanca):** todo desvio relevante fica classificado em bucket com decisao explicita (`aceite`, `pendente clinico`, `reverter`).

## Buckets de divergencia (Hepato)

- `fp_motor_lexico_generico`
- `fp_motor_contexto_extra_orgao`
- `fn_motor_gap_semantico`
- `possivel_fp_legado`
- `possivel_fn_legado`

## Baseline Diamond congelado

- Arquivo: `_local_samples/exports/hepatologia_compare_matrix_diamond.json`
- Resultado: `n_joined=100`, `n_match=89`, `n_mismatch=11`, `match_rate=0.89`
- Matriz: TP=1, FN=1, FP=10, TN=88
- Distribuicoes: legado (`N`=98, `S`=2), motor (`N`=89, `S`=11)

## Iteracao 1 (0.1.1-iter1) — hipotese

- Reduzir FP por termos genericos:
  - remover `nodulo`/`nódulo` soltos
  - remover `esteatose` solta; manter variantes hepaticas
- Reduzir FN focal:
  - adicionar `colecao_hepatica` / `abscesso hepatico`

## Evidencia da iteracao

Preencher apos rerodada com o mesmo pipeline da baseline:
- `build` (amostra Diamond padrao)
- `audit`
- `compare`
- delta vs baseline (`match_rate`, TP/FN/FP/TN, decisao A/B/C)

