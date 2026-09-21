# Validação da `0.14.0` em ambiente — `cancer_rim`, 21/09/2026

> **Delta zero em 4.172 laudos reais.** E, ao contrário da `0.13.0`, **zero era o resultado
> esperado**: esta coorte é **controle**, não medição da correção.

🔴 **O que este run prova e o que NÃO prova, dito antes do número:**

| prova | não prova |
|---|---|
| a `0.14.0` instala, roda e **não regride** o que já funcionava | que a correção do `[P0-29]` funciona |
| a guarda **não toca** decisão com evidência de régua — 13 promoções do juiz preservadas | o tamanho do impacto em produção |
| os campos novos não quebram o contrato de saída | a contabilidade de tokens da camada quantitativa |

**Quem prova a correção é o golden** (102 rebaixados, zero acrescidos) e os 5 testes do defeito.
**Quem fecha o `CA5` é a hepatologia** — §5.

---

## 1. O desenho

| | |
|---|---|
| linha | `cancer_rim`, config `0.6.0-cancer_rim` |
| ambiente | dev, `diamond_fabrica_ia_dev` |
| janela | **07/08/2026** — **a mesma coorte da `0.13.0`**, de propósito |
| coorte | **4.172 laudos**, mesmos `id_exame` nos dois lados |
| variável | **só a versão da lib** — `0.13.0` contra `0.14.0` |
| baseline | run de 14:53 (já gravado) · comparado | run de 21:08 |
| execução | `databricks jobs submit` por **CLI**, run `25032614221567`, cluster `ic-fabrica-ia-dlq` |

ℹ️ **A baseline não precisou ser re-executada** — as 4.172 linhas com `engine_version = 0.13.0`
já estavam na tabela de saída desde a validação da versão anterior. Meia corrida a menos.

## 2. Pré-condição

| | resultado |
|---|---|
| laudos processados | **4.172 de 4.172**, sem duplicata, config idêntica |
| **`[sentence_transformers]`** | **4.172 de 4.172** |
| `token_overlap` · `FileNotFoundError` | **0** · **0** |
| chamadas ao juiz | **15**, dos dois lados |

## 3. O resultado — zero divergência em onze campos

| camada | campos | divergências |
|---|---|---|
| decisão | `fl_relevante` · `findings` · `findings_spans` · `findings_match` · `confidence_score` · `exm_laudo_texto_tratado` | **0** |
| trilha | `decision_source` · `semantic_score` · `n_positive_spans` | **0** |
| agregado | 14 relevantes · 4.172 pares | idênticos |

**Zero rebaixados, zero acrescidos.**

## 4. 🔴 Por que zero era o esperado — e o que isso ainda vale

O caminho que a `0.14.0` corrige **não foi percorrido nesta coorte**, e os números dizem por quê:

| | `0.13.0` | `0.14.0` |
|---|---|---|
| laudos com `fl_relevante = 1` e `n_positive_spans = 0` | **0** | **0** |
| promoções semânticas (`semantic_promoted`) | **0** | **0** |
| guarda agiu (`*_unarbitrated`, `*_without_rule_evidence`) | **0** | **0** |
| critérios quantitativos no payload | **0** | **0** |

**A linha é controle por construção:** declara `similarity_threshold: 0.92` contra um máximo
observado de **0,9168**, e `uncertainty_band: [0.75, 0.95]` contra o teto analítico de **0,597**
para laudo sem achado. Era exatamente o que a medição de 16/09 já dizia — **zero em 16**.
**E não tem critério quantitativo**, então o segundo item da versão também não é exercitado aqui.

🟢 **O que sobra é um controle negativo que vale:** **13 promoções `llm_router_llm_positive`** e
**2 `llm_router_llm_negative`** atravessaram a guarda **intactas**, porque todas têm
`n_positive_spans > 0`. A exemção "promoção com evidência não é tocada" está correta em dado real,
não só em teste.
⚠️ **Sem essa checagem, o zero seria medição vazia** — indistinguível de um run que não instalou a
versão nova.

## 5. O que falta, e onde ele está

**O `CA5` do card `283648` — *[P0-29] Impedir que o juiz LLM promova sem evidência de regra* —
exige a coorte que CONTÉM a população.** Ela é a **hepatologia**, 31/08 a 16/09: **36 casos
correntes**, score 0,367 a 0,566, com `uncertainty_band: [0.35, 0.65]` cujo piso fica **abaixo** do
teto analítico.

⚠️ **E essa medição não é um run grande, é um run DIRIGIDO.** A janela cheia são ~17 dias a ~12 mil
laudos/dia — cerca de 200 mil laudos, e `limit_rows` não isola coorte (card `298596`). O desenho
tem de partir dos `id_exame` dos 36, não da janela inteira.

## 6. Ressalvas

- **Uma linha, um dia, e a linha errada para o defeito.** O valor deste run é de não-regressão.
- **O campo `llm_promoted` NÃO é emitido no payload** — é estado interno, lido pela guarda. Quem
  torna a via do juiz isolável é o `decision_source` (`llm_promotion_without_rule_evidence`).
  Só `semantic_promoted` vai para a saída, e **condicionalmente**, quando verdadeiro.
- **A contabilidade de tokens da camada quantitativa segue sem prova em ambiente.** O
  `llm_prompt_tokens` presente em 4.172 laudos é o do **juiz**, que já existia na `0.13.0`.
  Provar o item novo exige linha com critério quantitativo — TI-RADS ou transplante.
