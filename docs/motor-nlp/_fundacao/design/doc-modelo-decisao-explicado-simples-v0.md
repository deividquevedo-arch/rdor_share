# Como o motor decide — versão simplificada

Companheiro didático de [`doc-modelo-decisao-matematico-motor-v0.md`](doc-modelo-decisao-matematico-motor-v0.md).
Mesmo conteúdo, **sem formulas e maior didática**, com um exemplo passo a passo.

---

## 1. A pergunta que o motor responde

> "Este laudo tem algum achado de fígado/vias biliares que vale a pena um humano olhar?"

Resposta = **Sim (1)** ou **Não (0)**. Isso é o `fl_relevante`.

Junto vem um **número de 0 a 1** (`confidence_score`) que é só **"quão forte é a pista"** — quanto maior, mais o motor está convencido. **Não** é "probabilidade médica"; é uma nota interna.

---

## 2. A ideia geral (analogia do detetive)

O motor é um detetive com **4 ajudantes**, que opinam em ordem. Cada um pode reforçar ou mudar a conclusão:

| Ajudante | O que faz | Analogia |
|----------|-----------|----------|
| **1. Regras** | Procura palavras-chave (esteatose, cálculo, nódulo...) perto da palavra do órgão, e checa se não está negado | Lê o laudo com um **marca-texto** |
| **2. Embeddings** | Vê se a frase **parece** com um achado, mesmo escrito diferente | Reconhece "sinônimos" por **semelhança** |
| **3. Calibração** | Junta tudo numa nota final de 0 a 1 | Faz a **média ponderada** das opiniões |
| **4. LLM** | Só entra **se a nota ficou "em cima do muro"** | Chama um **especialista** para o desempate |

---

## 3. Como cada cálculo é feito (sem fórmula)

### Ajudante 1 — Regras
- Conta quantos achados **válidos** existem (`n_pos`). Um achado só conta se:
  1. a palavra do achado aparece (ex.: "esteatose"),
  2. está **perto** da palavra do órgão (ex.: "fígado"),
  3. **não** está negado (ex.: NÃO vale "ausência de cálculos").
- Também conta os **negados** (`n_neg`).
- Nota das regras (política atual):
  - achou pelo menos 1 → **0,9**
  - só achou negações → **0,35**
  - não achou nada → **0,0**

### Ajudante 2 — Embeddings (semelhança)
- Transforma frases em "coordenadas" e mede o quanto a frase do laudo **se parece** com os achados conhecidos. Resultado: número de 0 a 1 (`s_sem`).
- No modo atual (**híbrido**), a nota vira uma mistura: **70% regras + 30% semelhança**.

### Ajudante 3 — Calibração (a nota final)
- Pega a nota híbrida e a semelhança e faz uma **mistura fixa** (62% / 38%), com pequenos bônus (cada achado a mais sobe um pouquinho) e pequenas penalidades (negações descem um pouquinho).
- O resultado é o `confidence_score` que você vê na tabela.

### Ajudante 4 — LLM (o desempate)
- Só é chamado **se a nota ficou entre 0,35 e 0,65** (a "zona de dúvida").
- Aí o modelo de linguagem lê o trecho e responde só `{"relevante": true/false}`.
- Isso pode **mudar** o Sim/Não — é por isso que vários dos 22 casos viraram "Sim" mesmo sem palavra-chave exata.

> Se a nota já é alta (ex. 0,9) ou baixa (ex. 0,1), o LLM **nem entra** — o motor já tem certeza.

---

## 4. Exemplo completo (passo a passo)

**Laudo (resumido):**
> "Fígado com leve aumento difuso da ecogenicidade. Vesícula sem cálculos. Não há dilatação das vias biliares."

*(números abaixo são ilustrativos, para ensinar a mecânica)*

| Passo | O que acontece | Resultado |
|-------|----------------|-----------|
| **1. Regras** | "aumento difuso da ecogenicidade" não bate **exatamente** na lista → `n_pos = 0`. "sem cálculos" e "não há dilatação" são negações → `n_neg = 2`. | nota regras = **0,35** |
| **2. Semelhança** | A frase do fígado **parece** com "esteatose" (achado conhecido) → `s_sem = 0,75` | — |
| Híbrido | 70% de 0,35 + 30% de 0,75 = 0,245 + 0,225 | **0,47** |
| **3. Calibração** | mistura (62% de 0,47 + 38% de 0,75) menos penalidade das 2 negações | ≈ **0,51** |
| Zona de dúvida? | 0,51 está **entre 0,35 e 0,65** → **chama o LLM** | sim |
| **4. LLM** | Lê o trecho, entende que "aumento da ecogenicidade" sugere esteatose → responde `relevante: true` | **fl = 1** |
| **Saída** | `fl_relevante = 1`, `confidence_score ≈ 0,51`, `decision_source = llm_router_llm_positive` | ✅ |

**Tradução:** as regras sozinhas diriam "não" (não achou a palavra exata). A semelhança levantou a suspeita. Como ficou "em cima do muro", o LLM desempatou para **Sim**.

> É exatamente esse caminho que explica os casos da homologação onde **motor=1 e legado=0**: o legado (que só usa o "marca-texto") não pegou; o motor pegou via semelhança + LLM.

---

## 5. Outro exemplo (decisão sem LLM)

**Laudo:** "Sinais de esteatose hepática." (escrito igualzinho à lista)

| Passo | Resultado |
|-------|-----------|
| Regras | acha "esteatose hepática" perto de "hepática", não negado → `n_pos = 1` → nota **0,9** |
| Nota final | calibra para ~**0,93** |
| Zona de dúvida? | 0,93 **não** está entre 0,35 e 0,65 → **não chama LLM** |
| Saída | `fl = 1`, `decision_source = hybrid_calibrated` |

Aqui o motor teve certeza sozinho. Rápido e barato (sem chamada externa).

---

## 6. Resumindo em uma frase

> O motor soma **pistas de palavra** + **pistas de semelhança** numa nota de 0 a 1; se a nota for clara, decide sozinho; se ficar na dúvida, chama o LLM para desempatar.

---

## 7. O que isso **não** é (cuidado)

- O `confidence_score` **não** é "X% de chance de doença". É uma **nota de evidência**.
- Os limites (0,35–0,65, 70/30, etc.) são **escolhas de ajuste**, não verdades estatísticas — por isso a validação com equipe médica é que diz quem está certo.
- Concordar/discordar do legado mede **alinhamento entre sistemas**, não acerto clínico absoluto.
