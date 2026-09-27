# Validação da `0.13.0` em ambiente — `cancer_rim`, 21/09/2026

> **Delta zero em 4.172 laudos reais, catorze campos.** A primeira vez que a camada semântica da
> linha rodou com o modelo de verdade, e a primeira vez que o Model do Unity Catalog foi exercitado
> em `hml`.

⚠️ **A validação rodou DEPOIS da promoção**, não antes. O PR 7362 (`hml → main`) fechou às **12:51**;
esta medição fechou às **15:13**. A recomendação escrita na própria descrição do PR era rodar antes.
Registrado como aconteceu, não como deveria ter sido. O resultado sustenta a promoção — mas a ordem
não foi a acordada.

---

## 1. O desenho

| | |
|---|---|
| linha | `cancer_rim`, config `0.6.0-cancer_rim` |
| ambiente | dev, `diamond_fabrica_ia_dev` |
| janela | **07/08/2026**, um dia |
| coorte | **4.172 laudos**, mesmos `id_exame` nos dois lados |
| variável | **só a versão da lib** — `0.12.3` contra `0.13.0` |
| baseline | 14:22 a 14:42 · comparada | 14:53 a 15:13 |

**Tudo o mais idêntico:** mesma branch da plataforma (`hml`), mesma config, mesmos widgets,
`reprocess_enable: true`, `limit_rows` vazio, `embedding_enable: true`.

## 2. Pré-condição — o que prova que o run mediu alguma coisa

| | resultado |
|---|---|
| laudos processados | **4.172 de 4.172**, sem duplicata |
| **`[sentence_transformers]`** | **4.172 de 4.172** |
| `token_overlap` | **0** |
| `FALLBACK:FileNotFoundError` | **0** |

🟢 **É a primeira evidência de que a camada semântica roda com modelo real nesta linha.** Em
produção ela vinha caindo em `token_overlap` — medido em 15/09: `FileNotFoundError` em 98,4% dos
laudos do `cancer_rim`. Sem esta checagem, um run que degradasse passaria como sucesso e a
comparação seria vazia.

## 3. O resultado

**Zero divergência**, comparando por `id_exame`:

| camada | campos | divergências |
|---|---|---|
| decisão | `fl_relevante` · `findings` · `findings_match` · `findings_spans` · `confidence_score` · `exm_laudo_texto_tratado` | **0** |
| trilha | `decision_source` · `semantic_score` · `n_positive_spans` · `n_negated_spans` · `segmentation_coverage` · `llm_called` · `summary_compact` · `decision_trail.steps.semantic` | **0** |

Agregados idênticos: 14 relevantes dos dois lados, 49 laudos com texto tratado vazio dos dois lados.

## 4. O que só este run podia provar

O golden local (2.920 linhas, `sha256` idêntico) não alcançava três coisas:

1. 🟢 **O singleton do spaCy sob o driver do Spark.** O card `253579` corrigiu uma **condição de
   corrida** — publicação do objeto antes do `add_pipe`, fazendo uma segunda thread receber pipeline
   sem sentencizer. `segmentation_coverage` idêntico em 4.172 de 4.172 é a evidência de que o
   pipeline chegou completo a todas as threads. Teste com 16 threads existe, mas não é o driver.
2. 🟢 **O modelo real de embeddings**, em vez de `token_overlap`.
3. 🟢 **O Model do Unity Catalog resolvendo em `hml`** — nunca exercitado antes.

## 5. O caminho até a coorte — e o que ele revelou

🔴 **O volume nunca foi da janela: era o resgate de pendentes.** A primeira tentativa mostrou
**60 mil laudos** para uma janela de 5 dias que tem 22.281. A causa está em
`nlp_ia_02_input.py`:

```python
df_pending = df_source.where(
    F.col(processed_flag).isNull() | (F.col(processed_flag) == F.lit(False))
)
```

**O resgate de pendentes não filtra por data.** Lê a tabela de entrada inteira e traz tudo que não
está `processado = true`, em qualquer execução e qualquer janela. E `include_pending` tem default
`True` e **não tem widget** para desligar.

Havia **43.036 pendentes** acumulados — 33.039 com flag nula (03 a 16/08) e 9.997 com `false`
(31/08 a 14/09), resíduo de execuções interrompidas.

**Contorno aplicado:** marcar todos como `processado = true` e usar `reprocess_enable`. Com a
entrada limpa, a fila do run passa a ser exatamente a janela — inéditos 0, pendentes 0,
reprocessados 4.191.

⚠️ **E `limit_rows` não serve para cortar volume aqui**: ele corta **depois** da união da fila, cuja
ordem é inéditos → pendentes → reprocessados. Com teto, a coorte a remedir fica fora do corte e o
run fecha em sucesso sem tocá-la. É o card `298596`.

ℹ️ **Isto é irmão do `298596` e merece entrar no mesmo card:** *o resgate de pendentes não filtra
por data, não tem widget, e o sintoma é volume inexplicado — não erro.*

## 6. Ressalvas

- **19 laudos da janela não foram processados** (4.172 de 4.191). Não invalida: a comparação é sobre
  a interseção, e o denominador é o mesmo dos dois lados.
- **13 laudos das 13:09** são resíduo de um run encerrado no meio e ficaram fora por filtro de
  horário.
- **Uma linha, um dia.** Não é amostra de todas as especialidades — é a linha que exercita a camada
  semântica com modelo real, que era o ponto cego.
