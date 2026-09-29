# P0-29 — o juiz promove sem evidência de regra: a medição

> 16/09/2026 · card `283648` — *Fabrica IA/NLP Engine - [P0-29] Impedir que o juiz LLM promova sem
> evidência de regra* · pré-condição que a `0.14.0` exigia, aberta desde 21/08 sem número.

**Fonte:** tabelas de saída em `diamond_fabrica_ia`, janela de **30 dias** (17/08 a 16/09/2026),
seis linhas em produção. População: `fl_relevante = 1` **e** `llm_called = true` **e**
`n_positive_spans = 0` — laudo entregue como relevante, com o juiz acionado, sem nenhum span
positivo de regra.

---

# 1. O número

| linha | juiz chamado | chamado e relevante | **sem evidência de regra** | chamado e rebaixado |
|---|---|---|---|---|
| **hepatologia** | 2.225 | 2.003 | **118** | 222 |
| cancer_estomago | 27 | 24 | **0** | 3 |
| cancer_rim | 16 | 16 | **0** | 0 |
| reumatologia | 0 | — | — | — |
| tirads | 0 | — | — | — |
| transplante_pulmao | 0 | — | — | — |

🔴 **O defeito é real, está ativo, e é de uma linha só.** As três linhas com juiz desligado não
entram; as duas com juiz ligado e banda calibrada dão **zero**.

# 2. Os 118 se partem em dois casos com causas diferentes

| `decision_source` | n | o que é | janela |
|---|---|---|---|
| `llm_router_llm_positive` | **36** | o juiz respondeu *relevante* sem evidência | **31/08 a 16/09, corrente** |
| `llm_router_llm_fallback` | 82 | **erro de transporte** virou entrega pela política de falha | 21/08 (73) e 26/08 (9) |

## 2.1 🔴 Os 36 são o P0-29 puro, e continuam acontecendo

Distribuição diária: 8 em 31/08, depois 1 a 6 por dia útil, **2 hoje (16/09)**. Ritmo de
aproximadamente **um laudo por dia** entregue ao negócio sem nenhuma evidência determinística.

**O score de todos fica entre 0,367 e 0,566.**

🔴 **E é exatamente aí que está a causa.** O teto **analítico** do score de um laudo sem achado é
**0,597** — sem span positivo, o score de regra é no máximo `0.35`, e o composto calibrado não
ultrapassa aquele valor. A hepatologia declara `uncertainty_band: [0.35, 0.65]`: **o piso da banda
está abaixo do teto**, então laudo sem achado nenhum **chega ao juiz** e pode sair promovido.

✅ **As duas linhas com zero provam a correção pelo mesmo mecanismo.** No câncer de estômago a banda
é `[0.60, 0.97]` — piso **acima** do teto analítico —, e o resultado é **0 em 27 chamadas**. No
câncer de rim, `[0.75, 0.95]`, **0 em 16**.

## 2.2 Os 82 são o incidente do 403, e expõem a política de falha

Todos em 21/08 e 26/08, com `llm_error` = `http_403 {"error_code":403,"message":"Invalid access to
Org: ..."}`. **Desde 02/09 há zero falhas em 1.371 chamadas** — o PR 7135 resolveu, e isso se
confirma aqui.

🔴 **Mas a lição não expirou com o incidente.** No dia do 403, **1.371 laudos foram entregues como
relevantes pela falha**, dos quais 82 sem nenhuma evidência. A causa é
`fallback_policy: positive_in_band`, que em `_llm_failure_fallback_fl` retorna `1`
incondicionalmente — o docstring da própria lib avisa: *"é por aqui que uma falha de conexão vira
resultado de aparência normal"*.

🔴 **E essa política vem do bloco `runtime`.** A config da hepatologia declara
`fallback_policy: 'keep_current'` em `nlp.llm_router` e **`'positive_in_band'` em
`runtime.llm_router`** — e o `runtime` sobrepõe. **O que executa é o `positive_in_band`.**
É o mesmo item já registrado no documento de alinhamento, agora com consequência medida.

# 3. O que isso fecha e o que abre

✅ **Fecha a pré-condição da `0.14.0`.** O card `283648` pedia medição por linha antes de
implementar. Ela existe: **118 em 30 dias, 36 correntes, uma linha, causa atribuída.**

✅ **Confirma que a blindagem é da lib, não da config.** Hoje o invariante depende de cada
especialidade escolher um piso de banda acima do teto analítico. Duas escolheram, uma não — e não
há aviso quando não se escolhe.

🟡 **Reforça dois itens já abertos, sem abrir card novo:**

- o bloco `runtime` — agora com efeito clínico medido, não só divergência de declaração;
- o card `283644` — *[NLP Engine] Juiz LLM ligado por contorno não documentado em hepatologia e
  transplante_pulmao* — que é o mesmo par de linhas.

⚠️ **O que esta medição NÃO diz:** se os 36 estavam clinicamente certos. A ausência de evidência de
regra é violação de arquitetura independentemente do acerto — mas dimensionar o custo de remover
exige olhar os 36 laudos contra o critério da hepatologia.

---

## Consulta

```sql
SELECT get_json_object(exm_laudo_resultado,'$.decision_source') src, count(*) n,
       round(min(confidence_score),3) smin, round(max(confidence_score),3) smax
FROM   diamond_fabrica_ia.hepatologia.tb_mod_diamond_hepatologia_saida_v0
WHERE  dt_execucao_modelo >= current_date() - INTERVAL 30 DAYS
  AND  fl_relevante = 1
  AND  get_json_object(exm_laudo_resultado,'$.llm_called') = 'true'
  AND  cast(get_json_object(exm_laudo_resultado,'$.n_positive_spans') AS INT) = 0
GROUP BY 1
```

⚠️ **`llm_router_mode = 'llm'` não serve para isolar** — é o modo do roteador e sai em 100% dos
laudos da linha, com ou sem chamada. Quem separa é `llm_called`.
