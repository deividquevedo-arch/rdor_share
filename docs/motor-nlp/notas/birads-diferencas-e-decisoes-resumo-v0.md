# BI-RADS — Motor Novo vs. Sistema Antigo: o que muda e o que precisamos decidir

**Para quem é este documento:** qualquer pessoa, técnica ou não. A ideia é explicar, em linguagem simples, o que encontramos ao comparar o **motor novo** (extração de NLP) com o **sistema antigo (legado)** na leitura de laudos de mama, e quais **decisões** dependem de gente (não de código).

_Data: 2026-06-20 · Frente: Piloto BI-RADS_

---

## 1. O contexto em uma frase

O **BI-RADS** é uma nota de 0 a 6 que o médico coloca no laudo de mama para indicar o nível de suspeita de um achado. Tanto o sistema antigo quanto o motor novo tentam **ler o laudo e descobrir essa nota automaticamente**. Estamos comparando os dois para garantir que o novo é, no mínimo, tão bom quanto o antigo — e onde ele já é melhor.

**Como medimos:** rodamos os dois sobre os **mesmos laudos reais** e comparamos resultado a resultado.

---

## 2. O placar atual (o quanto eles concordam)

| O que medimos | Antes dos ajustes | **Depois dos ajustes** |
|---|---|---|
| Concordância da nota BI-RADS (amostra de ~890 laudos) | 98,15% | **99,55%** |
| Casos onde o motor errava (anotados por revisão manual) | 31 | **27 corrigidos** · 4 a reconferir |

**Leitura simples:** depois dos ajustes, o motor novo concorda com o antigo em **praticamente todos** os casos — e em vários onde discordava, **o motor estava certo e o antigo errado**.

---

## 3. As diferenças que encontramos

Dividimos em três grupos: **(A) onde o motor era pior** (já corrigido), **(B) onde o motor é melhor**, e **(C) a diferença de fundo que ainda precisa de decisão**.

### A) Onde o motor errava — JÁ CORRIGIDO

Eram detalhes de "leitura" do texto. Exemplos no dia a dia:

1. **"Categoria 2 ACR"** — o motor grudava o "A" de "ACR" no número e lia errado. _Corrigido._
2. **"BI-RADS US 2"** — o motor não esperava a sigla do exame (US) no meio. _Corrigido._
3. **"Categoria 02"** — o zero à esquerda virava nota 0. _Corrigido._
4. **Um bug mais sério:** quando o laudo tinha a palavra "referência" no corpo (ex.: "com referência de estabilidade"), o sistema **apagava a conclusão inteira** do laudo por engano — e junto ia o BI-RADS. _Corrigido._ (Esse afetava **todas as especialidades**, não só mama.)

> **Resultado:** todos esses pontos foram ajustados e os testes automáticos passaram (156 testes verdes).

### B) Onde o motor novo é MELHOR que o antigo

Aqui não é "erro a corrigir" — é vantagem real do motor:

- **Subcategorias** (4A, 4B, 4C): o antigo só enxerga "4"; o motor distingue **4A, 4B, 4C**, que têm significado clínico diferente.
- **Negação**: se o laudo diz "achado **descartado** BI-RADS 4", o antigo ainda contava o número; o motor entende que foi descartado.
- **Regra do "9"** e citações de referência bibliográfica: o motor trata melhor esses casos.

### C) A grande diferença de fundo — PRECISA DE DECISÃO

Esta é a mais importante e **não é bug**. É uma diferença de **critério**:

- **Sistema antigo:** só considera o exame "relevante" se tiver **BI-RADS 4 ou maior**.
- **Motor novo:** considera relevante se tiver **um achado clínico OU BI-RADS 4+**.

Por causa disso, o motor "marca" cerca de **26% a mais de exames** que o antigo (os chamados "falsos positivos vs. legado"). Mas a palavra "falso" aqui é enganosa: **podem ser casos verdadeiros que o sistema antigo deixava passar**. Só a revisão clínica pode dizer.

---

## 4. As decisões que precisamos tomar (e quem decide)

Estas decisões **não são técnicas** — dependem de critério clínico e de produto:

| # | Decisão | Quem decide | Por que importa |
|---|---|---|---|
| 1 | **Critério de relevância**: o motor deve seguir o antigo (só BI-RADS ≥ 4) ou manter o critério mais amplo (achado OU BI-RADS ≥ 4)? | Cientista-médico / time clínico | É a diferença de fundo do item 3-C. Decide se aquele "26% a mais" é ganho ou ruído. |
| 2 | **Revisão clínica de uma amostra** dos casos extras (BI-RADS 2 e 3) | Time clínico | É o que confirma se os "falsos positivos" são, na verdade, acertos do motor. |
| 3 | **Revisar os casos "iguais"** — a homologação atual foi manual; falta o aval do time clínico | Time clínico | Garante que a concordância de 99,55% também está clinicamente correta. |

---

## 5. O que falta no lado técnico (sem decisão pendente)

Itens que seguem assim que houver autorização — não dependem de decisão clínica:

- **Reconferir 4 laudos** cujo identificador foi embaralhado pelo Excel ao salvar a planilha.
- **Salvar (commit) as correções** já feitas (aguardando sua autorização — política do projeto é não publicar nada sem OK explícito).
- **Estender para outras especialidades:** próstata (PI-RADS) e tireoide (TI-RADS), usando os mesmos aprendizados.

---

## 6. Resumo em 3 frases

1. O motor novo já **concorda com o antigo em 99,55%** dos laudos e, onde discordava por erro, **foi corrigido**.
2. A única diferença grande que sobra **não é erro** — é um **critério mais amplo** do motor, que pode estar capturando casos reais que o antigo perde.
3. O próximo passo está **com o time clínico**: decidir o critério de relevância e revisar uma amostra — só depois disso o motor é validado para avançar.

---

_Fonte: `checkpoint-birads-bancada-ab-2026-06-19.md` e `s11-birads-paridade-rads-v0.md`._
