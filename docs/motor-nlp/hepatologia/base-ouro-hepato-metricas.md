# Base ouro — Hepato (Fase 0)

> Status: **parcial** — sem coorte balanceada recuperada ainda. Harness live pronto; o número
> representativo do hepato exige run **E2E com LLM** (o LLM é o driver principal deste piloto).

## Fontes disponíveis (locais)
- `hml_interna_hepato_pre - dia4.csv` (73 laudos) — homologação online do time + Carol. Veredito
  `homologação`: MOTOR(48)/VERDADEIRO(10) = motor certo · LEGADO(12)/FALSO(3) = motor errado.
  Derivado: **57 relevantes + 16 não-relevantes** (gold permanente por id_exame). Tem `laudo_preview`.
- `dev_tbl_gold_modelo_hepatologia_{entrada,retorno}.csv` (100 cada) — retorno tem `achadoRelevante`
  do board (50 "Sim tem doença" + 32 "Sim mas não tem doença fígado" + 16 "Não" + 2 vazio). **⚠️
  entrada e retorno NÃO casam por idExame** (0 overlap; janelas/execuções distintas) → não dá p/
  juntar laudo+gabarito automaticamente.

## Ressalvas (por que não é coorte)
- **Board-pre enviesado**: 72/73 são `fl_motor=1` (amostra de DIVERGÊNCIAS motor×legado) →
  especificidade/recall não-representativos; serve p/ PRECISÃO/FP.
- **"pre"** (motor antigo) — o hepato **evoluiu muito** desde então (config super otimizado hoje).
- `laudo_preview` pode truncar a conclusão.

## Medições
| Fonte do `fl` | TP | FP | FN | TN | Precisão | Recall | Obs |
|---|---|---|---|---|---|---|---|
| Motor rule_only (SEM LLM, local) | 11 | 1 | 46 | 15 | 0,917 | **0,193** | **artefato**: hepato depende do LLM router |
| Produção E2E (com LLM) | — | — | — | — | (pendente run) | | |

**Por que rule_only não vale p/ hepato:** no board-pre, 66/73 relevantes vieram de
`llm_router_llm_positive`. A REGRA sozinha pega só 11/57 → recall 0,19 é artefato de rodar sem o
componente principal. (No TI-RADS a regra já traz o recall; no hepato quem decide é o LLM.) Por
isso **NÃO congelamos baseline rule_only aqui** — seria enganoso.

## Harness Fase 0
`.claude/jobs/56d76a3e/tmp/baseohro_hepato.py` — gold = veredito do board por id_exame.
- **`--fl-csv <path>` (E2E, único representativo):** passar `id_exame,fl` de um run real com LLM
  (config hepato atual) → precisão/recall vs board. Congelar `EXPECTED_PROD` então.
- default (rule_only): só diagnóstico do núcleo; NÃO é baseline do hepato.

## Próximo passo
- Rodar E2E do hepato (config atual, com LLM) → medir vs board (mostra a evolução vs "pre").
- Idealmente recuperar a **coorte balanceada** (base ouro coorte via tb retorno + laudos casáveis)
  p/ recall/especificidade representativos. Não-bloqueante (decisão do head).
