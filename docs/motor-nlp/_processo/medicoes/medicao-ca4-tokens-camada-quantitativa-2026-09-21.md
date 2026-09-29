# `CA4` do `283648` — tokens da camada quantitativa, medidos em ambiente

> **162 de 162 chamadas passaram a ter contabilidade. Eram ZERO.** E a decisão não mudou em
> nenhum dos 120 laudos — o item é puramente aditivo.

- **Card:** `283648` — *Fabrica IA/NLP Engine - [P0-29] Impedir que o juiz LLM promova sem evidência de regra*
- **Data:** 2026-09-21 · **Run:** `130126794270375`, dois braços sequenciais no mesmo cluster

---

## 1. Por que a linha é `tirads`

O `cancer_rim` e a hepatologia **não exercitam a camada quantitativa** — a primeira validação deu
zero critérios quantitativos no payload, e a segunda também. Medir o item ali seria medição vazia.

O TI-RADS é a linha que usa o LLM na **extração de medida**, com o juiz desligado. Em produção,
na janela de 15 a 21/09: **8.381 laudos com bloco `quantitative`, 740 com `llm_called` por
critério, e ZERO com token registrado**. É exatamente o buraco que a `0.14.0` fecha.

⚠️ **O `llm_called` daqui NÃO é o de topo.** O campo existe no topo (o juiz) **e** por critério
(a extração de medida). Mesmo nome, níveis diferentes — leitura por `LIKE` em SQL encontra os dois.

## 2. O desenho

Coorte pelo **predicado**, não por janela: 120 laudos com chamada ao LLM na camada quantitativa,
lidos da saída de produção. Os dois braços rodaram o mesmo notebook, no mesmo cluster, sobre os
mesmos `id_exame`, com a versão da lib como única variável.

## 3. O resultado

| | `0.13.0` | `0.14.0` |
|---|---|---|
| laudos · critérios quantitativos | 120 · 1.080 | 120 · 1.080 |
| critérios **com chamada ao LLM** | **162** | **162** |
| critérios **com token registrado** | **0** | **162** |
| `llm_prompt_tokens` (soma) | 0 | **158.141** |
| `llm_completion_tokens` (soma) | 0 | **18.929** |
| entregues (`fl = 1`) | **59** | **59** |

✅ **Cobertura de 162 em 162 — 100% das chamadas.**
✅ **Zero mudança de decisão:** 59 entregues dos dois lados. O item é aditivo, como declarado.

**Por chamada: 976,2 tokens de prompt e 116,8 de completion**, 1.093 no total; 177.070 tokens nos
120 laudos.
ℹ️ **Observação, não conclusão:** esse número por chamada ficou próximo dos **1.004,1** medidos no
caminho do juiz da hepatologia em 10/09. A SPEC alertava que estimar por regra de três não se
sustenta, porque os laudos de TI-RADS vão de ~1.500 a 815 KB — a proximidade sugere que o **prompt
é truncado** e o tamanho do laudo não governa a contagem linearmente. **Não é base para estimar
outras linhas**: continua valendo medir.

## 4. 🟢 Controle positivo que não estava previsto

**Um** laudo dos 120 sai com `fl_relevante = 1` e `n_positive_spans = 0` — nos **dois** braços — e
seu `decision_source` é **`ordinal_promotion`**, com `llm_called` falso e `semantic_promoted` falso.

**A guarda o deixou intacto, que é exatamente o desenho:** a categoria RADS **é** achado clínico
declarado, então é evidência própria e está na lista de exemções, junto com `ordinal_only` e
`quantitative_promote`.

Isso fecha um risco que os testes cobriam mas nenhuma medição em ambiente tinha tocado: a guarda
**não** rebaixa promoção ordinal.

## 5. Estado do critério

| critério | estado |
|---|---|
| **`CA4`** | ✅ **atendido** — a camada quantitativa registra tokens em 162 de 162 chamadas, e a soma é conferível |

⚠️ **Artefato de bancada a apagar quando o card fechar:**
`diamond_fabrica_ia_dev.tirads.tb_bancada_tokens_v0`.
