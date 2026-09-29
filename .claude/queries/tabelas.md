# Registro de tabelas e versões declaradas

> **Este arquivo é a metade que dá sentido às queries.** O sinal 3 compara o que rodou contra o
> que está declarado aqui. Sem declaração, não há do que divergir.
>
> ⚠️ **Manter atualizado no dia em que uma versão sobe em produção.** Registro desatualizado é
> pior que registro nenhum: gera alarme falso, e alarme falso é o que faz alguém desligar o alarme.
>
> Preenchido e verificado em **2026-09-25**, com run real das duas pontas.

## Produção — `diamond_fabrica_ia`

Padrão da tabela: `diamond_fabrica_ia.<linha>.tb_mod_diamond_<linha>_saida_v0`.

| linha | `config_version` | `engine_version` fixado | volume/dia | taxa típica |
|---|---|---|---|---|
| `hepatologia` | `0.1.13-hep-emb-volume` | `0.12.3` | ~4.500 | 1,3% a 1,4% |
| `cancer_rim` | `0.6.0-cancer_rim` | `0.12.3` | ~4.600 | 0,2% a 0,5% |
| `reumatologia` | `0.1.0-reumatologia` | `0.12.3` | ~3.400 | 0,2% a 0,4% |
| `tirads` | `0.8.0-tirads` | `0.12.3` | ~1.600 | 5,0% a 6,0% |
| `transplante_pulmao` | `0.1.2-pulmao-failover` | `0.12.3` | ~180 | 4% a 6% |
| `cancer_estomago` | `0.6.9-cancer_estomago` | `0.12.3` | ~175 | **0,83%** em 14 dias |

## Homologação — `diamond_fabrica_ia_hml`

Três linhas rodam aqui e **não existem em produção**: `doenca_inflamatoria_intestinal`,
`ateromatose_coronariana`, `tumor_osseo`.

| linha | `engine_version` | observação |
|---|---|---|
| `cancer_colon` | 🔴 **varia** | `0.15.3` em 24/09, `0.15.4` em 25/09 — a definição usa `${nlp_engine_version}` |
| demais | `0.12.3` | estável |

---

## Exceções conhecidas — o que NÃO é anomalia

⚠️ **Sem isto, o sinal 1 produz alarme falso.** Medido em 23–25/09.

| linha | entrega com `n_positive_spans = 0` | por quê |
|---|---|---|
| `transplante_pulmao` | **100%** das entregas | a config tem `findings: {}` — não existe caminho léxico, e toda relevância vem de `quantitative_promote` |
| `tirads` | algumas, com `decision_source: ordinal_promotion` | categoria RADS é achado clínico declarado, exemptada de propósito |

🔴 **O que É defeito:** `decision_source` igual a `llm_router_llm_positive` ou `hybrid_calibrated`
com zero span — o `[P0-29]`, card `283648`. Medido na hepatologia: **5 em 192 entregas** em três
dias. A `0.14.0` corrige, e não está adotada.

**Antes de chamar de anomalia, levantar o `decision_source`:**

```sql
SELECT get_json_object(exm_laudo_resultado,'$.decision_source') fonte, count(*)
FROM <tabela> WHERE fl_relevante = 1
  AND cast(get_json_object(exm_laudo_resultado,'$.n_positive_spans') AS int) = 0
GROUP BY 1
```

---

## Armadilhas de execução destas queries

- 🔴 **Não rodar em paralelo.** Várias consultas concorrendo no mesmo warehouse fazem tabelas que
  respondem em 10s estourarem em 40s. Em 25/09 isso me fez reportar *"SEM RESPOSTA"* para três
  linhas que estavam perfeitamente saudáveis.
- ⚠️ **Timeout curto mente.** `cancer_estomago`, `cancer_rim` e `reumatologia` levam **~70s**;
  `tirads` leva 10s. Usar 180s e rodar em série.
- ⚠️ **Filtro com data literal** (`dt_execucao_modelo >= TIMESTAMP '...'`) é sensivelmente mais
  rápido que `to_date(...) >= current_date() - N`.
