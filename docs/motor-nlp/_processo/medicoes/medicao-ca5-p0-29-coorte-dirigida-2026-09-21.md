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

---

# ADENDO (mesmo dia) — 🔴 a leitura acima estava lendo o cenário ERRADO

## 9. Produção não roda com modelo de embeddings, e isso INVERTE o resultado

Ao dimensionar a mudança de limiar, o dado apareceu:

| | `semantic_score` dos mesmos 41 | a semântica promove |
|---|---|---|
| **produção, hoje** — `token_overlap` em **41 de 41**, zero com modelo real | mediana **0,667** | **4** |
| **esta bancada** — modelo real em 41 de 41 | 0,795 a 0,995 | **37** |

🔴 **A bancada mediu o estado FUTURO, não o atual.** A ressalva da §8 estava escrita, mas a
consequência não: ela **inverte** o número.

- **Hoje, como produção executa:** a semântica não alcança o limiar, quem promove é o **juiz**,
  `llm_promoted` fica verdadeiro e **a guarda reverte ~37 dos 41**. A `0.14.0` é eficaz.
- **Depois que o card `305810` fechar** e os embeddings passarem a funcionar: a semântica promove,
  o juiz confirma, e **37 escapam pela exemção**.

✅ **Então o `CA1` não é dívida corrente — é uma armadilha que ARMA quando os embeddings forem
corrigidos.** Resolver antes do gatilho existir é mais barato e não tem janela de dano.
ℹ️ Consistente com a contagem em produção: em **25.809 laudos** da semana de 15 a 21/09, **zero**
na faixa `[0,78 · 0,92)` — com `token_overlap` quase nada alcança 0,78.

## 10. A alavanca de config, medida — limiar `0,78` contra `0,92`

Run `527741631034186`, **mesma coorte de 41**, modelo real, **única variável o limiar**.

| braço | entregues | `semantic_promoted` | guarda agiu |
|---|---|---|---|
| **0,78** (config atual) | **37** | 37 | **4** |
| **0,92** (referência do `cancer_rim`) | **8** | 8 | **33** |

🟢 **De 4 para 33 removidos — 80,5% da população — sem tocar uma linha da lib.**
A previsão feita pela distribuição de score (29 dos 37 abaixo de 0,92, logo 4 + 29 = 33) bateu
**exatamente** com o medido. Os 8 restantes têm similaridade ≥ 0,92 e arbitragem confirmada.

⚠️ **Armadilha que quase engoliu a medição:** a config declara
**`similarity_threshold_by_model`**, que **vence** o valor de topo quando o modelo casa. A
sobreposição precisou remover esse bloco e confirmar com `assert` — sem isso o run reportaria 0,92
e executaria 0,78, e o resultado sairia idêntico ao outro braço sem nenhum sinal de erro.

🔴 **O que esta medição NÃO responde:** quantas promoções semânticas **legítimas** o limiar de 0,92
removeria na linha inteira. A coorte aqui é definida pelo defeito — só contém laudos **sem**
evidência de régua. Medir a perda exige janela completa **com modelo real**, e isso só faz sentido
depois do `305810`.

## 11. 🔴 Dívida encontrada: `emit_as_finding` DESARMA a guarda

`embeddings.emit_as_finding` (`semantic_expand.py:447`) é **opt-in, default `False`**, e **inerte
nas sete configs** — duas declaram `False` explicitamente, cinco não declaram.

**Ligá-la desligaria a invariante do `[P0-29]` na linha**, em silêncio: ela **incrementa
`n_positive_spans`** (`decision_pipeline.py:590-594`), e a primeira linha da guarda é
`if st.fl != 1 or st.n_pos > 0: return`. Match semântico vira span positivo, a guarda retorna sem
agir, e nada registra que isso aconteceu.

**A causa é que `n_positive_spans` passaria a significar duas coisas:** evidência de régua **ou**
parecença. É exatamente a distinção que esta versão existe para preservar.

**Condição prévia a qualquer ativação:** a guarda precisa contar **span de régua separado** do span
emitido pela semântica. Enquanto isso não existir, a chave não deve ser ligada em nenhuma linha.

---

# ADENDO 2 (22/09) — 🔴 A CAUSA É OUTRA: a régua estava cega por SEGMENTAÇÃO

## 12. O que a medição dos termos casados revelou

O run `222180888862290` persistiu **qual termo da régua casou e com que trecho**. O resultado
derruba a leitura dos adendos anteriores.

| score | termo da régua | trecho do laudo |
|---|---|---|
| **0,995** | `hepatopatia crônica` | **"Hepatopatia crônica"** |
| 0,948 | `doença hepática` | "- Doença hepática gordurosa" |
| 0,934 | `doença hepática` | "Doença hepática metabólica" |
| 0,773 | `circulação colateral` | **"Circulação colateral periesplênica"** |
| 0,752 | `doença hepática` | "Esteatose hepática" |

🔴 **Não é sinônimo reconhecido por parecença — é o termo LITERAL da régua.** A régua deveria
tê-lo encontrado por casamento de texto, e não encontrou.

## 13. A causa, provada

| | |
|---|---|
| a camada semântica recebe | `st.treated` — **o laudo tratado INTEIRO** (`decision_pipeline.py:558`) |
| a régua recebe | o texto **segmentado** |
| dos 44 casos do `[P0-29]` | **44 de 44 têm perda de segmentação** |
| cobertura mínima observada | **0,006** — a régua viu **0,6%** do laudo |

**A hepatologia é a ÚNICA das sete linhas com `segmentation.mode: auto`** — as outras seis usam
`full_doc`. E é a única com `similarity_threshold: 0.78`; as demais vão de 0,80 a 0,92.

🟢 **Isso explica a concentração que a medição de 16/09 atribuiu à banda.** A banda explica o juiz
ser **alcançado** (piso 0,35 abaixo do teto analítico 0,597). Ela **não** explica a ausência de
evidência — quem explica é a segmentação. As duas são complementares, e a raiz é a segunda.

## 14. As três conclusões que caem

1. 🔴 **Subir o limiar para 0,92 apagaria achado LITERAL.** A recomendação do adendo 1 está
   **cancelada**. Ela nasceu de analogia com o `cancer_rim`, não de evidência: o que precisava ser
   olhado era **o que casou**, e não o número.
2. 🔴 **A guarda da `0.14.0`, aplicada à hepatologia hoje, remove VERDADEIRO POSITIVO** — 2 de 2
   nesta coorte (`Circulação colateral periesplênica`, `Esteatose hepática`). A lógica da guarda
   está correta; **a premissa é que falha**: `n_positive_spans = 0` significa *"a régua não achou"*
   e está sendo lido como *"não há achado no laudo"*. Com 99% do texto descartado, a inferência não
   se sustenta.
3. ✅ **O card `300202` deixa de ser higiene e passa a ser a causa raiz do `283648` na hepatologia.**

🟢 **Nada disso causou dano em produção**, porque **nada está pinado** — as seis linhas rodam
`0.12.3` e a decisão de 21/09 segura o pin até a `0.15.0`. Foi exatamente o que essa decisão comprou.

## 15. A conclusão NÃO transfere para as outras linhas

**Ateromatose usa `full_doc`** (config `0.2.3`), e lá a régua enxerga o documento inteiro. As 33
promoções semânticas de 44 medidas no `0.2.1`, e o juiz acionado em 6.111 de 7.500 no `0.2.0`,
são **promoção sem evidência de verdade** — a guarda da `0.14.0` está certa naquele caso.
ℹ️ A linha já desligou a semântica na `0.2.2`, por decisão registrada.

**Ou seja: a `0.14.0` tem alvo real; ele só não é a hepatologia.**

## 16. Ordem correta da passada única da hepatologia

🔴 **`segmentation.mode: full_doc` PRIMEIRO, guarda de evidência depois.** Invertido, a guarda
rebaixa o que a régua deveria ter achado, e o efeito seria lido como "a correção funcionou".
