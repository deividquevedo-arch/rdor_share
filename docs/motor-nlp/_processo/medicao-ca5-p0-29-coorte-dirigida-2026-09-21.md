# `CA5` do `283648` — A/B dirigido na coorte que contém a população

> **A `0.14.0` remove 4 dos 41. Não 41.** A guarda funciona exatamente como foi escrita — e a
> medição expôs que **o `CA1` e o `CA2` da SPEC se contradizem**, e a implementação seguiu o `CA2`.

- **Card:** `283648` — *Fabrica IA/NLP Engine - [P0-29] Impedir que o juiz LLM promova sem evidência de regra*
- **Data:** 2026-09-21 · **Run:** `470903865428642`, dois braços sequenciais no mesmo cluster

---

## 1. O desenho — A/B por `id_exame`, não por janela

A população do defeito é ~1 laudo por dia útil em três semanas. Rodar a janela custaria ~200 mil
laudos para medir algumas dezenas, e **`limit_rows` corta DEPOIS da união da fila** (card `298596`),
então a coorte ficaria fora do corte e o run fecharia em sucesso sem tocá-la.

A coorte é definida **pelo predicado do defeito**, lido da saída de produção:

```
fl_relevante = 1
AND n_positive_spans = 0
AND decision_source = 'llm_router_llm_positive'
AND dt_execucao_modelo >= '2026-08-31'
```

**41 laudos** — eram 36 em 16/09; a população cresceu com o tempo, como esperado de defeito corrente.
Os dois braços rodaram o **mesmo notebook**, no mesmo cluster, sobre os **mesmos 41 `id_exame`**,
com a **versão da lib como única variável**. Nenhum texto de laudo foi copiado para a bancada.

## 2. Pré-condição — o braço baseline REPRODUZIU o defeito

| | `0.13.0` |
|---|---|
| laudos | **41** |
| entregues (`fl = 1`) | **41** |
| **`fl = 1` com `n_positive_spans = 0`** | **41 de 41** |
| juiz chamado · erros | 41 · **0** |
| semântica com modelo real | **41 de 41** |
| `decision_source` | `llm_router_llm_positive` em 41 de 41 |

🟢 **A coorte contém a população e o defeito se reproduz integralmente.** Sem isto, qualquer
resultado do outro braço seria indistinguível de coorte vazia.
ℹ️ A não-determinação do juiz **não se materializou**: ele promoveu os mesmos 41.

## 3. O resultado

| | `0.13.0` | `0.14.0` |
|---|---|---|
| entregues | 41 | **37** |
| `fl = 1` sem evidência de régua | 41 | **37** |
| guarda agiu (`llm_promotion_without_rule_evidence`) | — | **4** |
| guarda agiu (`semantic_promotion_unarbitrated`) | — | **0** |
| **acrescidos** | — | **0** |

🔴 **A remoção é de 4 em 41 — 9,8%.**

## 4. Por que 37 sobrevivem, e é POR DESENHO

O corte é exatamente o `similarity_threshold: 0.78` da hepatologia:

| grupo | `semantic_score` | o que aconteceu |
|---|---|---|
| **37 mantidos** | **0,795 a 0,995** | ≥ limiar → **a semântica promoveu**; o juiz foi chamado, respondeu sem erro e confirmou → `juiz_arbitrou` → **a guarda EXEMPTA** |
| **4 revertidos** | **0,686 a 0,773** | < limiar → a semântica não promoveu; **o juiz** levou `fl` de 0 para 1 → `llm_promoted` → **revertidos** |

`semantic_promoted` saiu **37 de 41** no braço da `0.14.0` — o campo novo isolou a via **sem delta
entre runs**, que é o `CA3`. Foi ele que permitiu diagnosticar isto em uma consulta.

## 5. 🔴 A SPEC se contradiz, e a medição foi quem mostrou

| critério | texto | atendido? |
|---|---|---|
| **`CA1`** | *"Nenhum laudo sai com `fl_relevante: 1` e `n_positive_spans: 0`, **por nenhuma via**"* | **NÃO** — 37 saem |
| **`CA2`** | *"A via B exige arbitragem do juiz **independentemente da banda**; com o juiz desligado, ela não entrega"* | **SIM** — arbitragem ocorreu, então entrega |

**Os dois não podem valer ao mesmo tempo.** O `CA1` diz que parecença nunca sustenta entrega; o
`CA2` diz que parecença arbitrada sustenta. A implementação seguiu o `CA2`, e o comentário no
código declara a escolha.

⚠️ **A exemção foi introduzida para satisfazer `test_juiz_responde_e_decide_normalmente`, da
`0.11.0` — e esse teste afirma menos do que se supôs.** Ele verifica que o `decision_source` **não
é** `semantic_promotion_unarbitrated`; **não** verifica que o laudo é entregue. Um rótulo distinto
para "arbitrado, mas sem evidência de régua" satisfaria o teste **e** reverteria os 37.
**Ou seja: a escolha atual não foi imposta pelo teste — foi uma decisão de desenho tomada ao
satisfazê-lo.**

## 6. A pergunta que decide, e ela não é de engenharia

**Similaridade semântica confirmada pelo juiz conta como evidência para entregar?**

- **Não conta** — a invariante declarada é *o juiz filtra, nunca cria relevância*, e a própria
  tabela do `step_guard_evidence` classifica *parecença não é achado* e *opinião do LLM não é
  achado*. Duas não-evidências somadas seguem não sendo evidência. **Remove 41 de 41.**
- **Conta** — a cascata desenhada é régua → semântica **alarga** → juiz **estreita**; confirmada a
  arbitragem, a promoção passou pelo filtro que devia passar. Reverter esvazia o `decision_mode:
  hybrid` nas linhas sem casamento léxico. **Remove 4 de 41.**

🔴 **Consequência para o negócio é diferente nas duas**, e a escolha é de régua clínica.

## 7. O que a medição fecha e o que não fecha

| critério | estado |
|---|---|
| `CA1` | 🔴 **não atendido** como redigido — depende da §6 |
| `CA2` | ✅ atendido |
| `CA3` | ✅ atendido — `semantic_promoted` isolou a via numa consulta |
| `CA4` | ⚠️ **sem prova em ambiente** — a hepatologia não tem critério quantitativo |
| `CA5` | ✅ **coorte contém a população e o delta está enumerado** |
| `CA6` | ✅ **zero acrescidos** nesta coorte |
| `CA7` | 🟡 pendente — a lista de discordâncias só vai ao negócio depois da §6 |

## 8. Ressalvas

- **A coorte é definida pelo defeito**, então mede remoção; **ganho fora dela não é observável
  aqui**. Quem cobre isso é a não-regressão do `cancer_rim` (4.172 laudos, zero acrescidos).
- **A config carregada é a da `hml`** (`0.1.13-hep-emb-volume`), e os embeddings rodaram com
  **modelo real** nos dois braços — em produção a mesma linha cai em `token_overlap` em 99,3% dos
  laudos. **O A/B é válido** (as duas pontas usam a mesma config), mas **o número não reproduz
  produção**: com `token_overlap` os `semantic_score` seriam outros e a partição 37/4 mudaria.
- **Artefato de bancada a apagar quando o card fechar:** notebook
  `plataform/ntb_ia_bancada_p0_29` no workspace e tabela
  `diamond_fabrica_ia_dev.hepatologia.tb_bancada_p0_29_v0`.
