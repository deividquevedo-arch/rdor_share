# PRs da `0.15.4`

> Textos para abrir os dois PRs. Em cada um, a **descrição** é o bloco entre as linhas `---`.
> Medidos: **PR 1 = 3.491 caracteres**, **PR 2 = 1.875** — dentro do limite de 4.000 do time.
>
> Branch pushada: `fix/0.15.4-refutacao-independe-da-atribuicao`, commit `98561ee`, confirmada
> pela REF. **Tag `v0.15.4`** vai sobre o **merge do PR 1 na `hml`** — é a convenção do
> repositório (`v0.15.3` → `54e3a8c`, o merge na `hml`, não a promoção à `main`).

---

## PR 1 — `fix/0.15.4-refutacao-independe-da-atribuicao` → `hml`

**Título**

```
fix(0.15.4): a refutacao pelo maximo independe da atribuicao
```

**Revisores:** Diego, João e Gabriel — code review obrigatório em PR da lib, decisão de 21/08.

**Descrição:**

---

A `0.15.3` foi validada em coorte real (run `109956348322922`, 16.014 pares) e **acertou o alvo**: os **121** rebaixamentos do card `306034` seguem de pe (**121 de 121**, zero regressoes) e os **73** indevidos foram recuperados integralmente. Mas a medicao achou um defeito **da propria `0.15.3`**.

## O defeito

A guarda de categoria orfa protegia **mesmo quando o laudo ja refutava o criterio**:

| das 143 orfas com braco de comparacao | |
|---|---|
| **E** — maximo do documento >= limiar, protege com razao | **76** |
| **F** — maximo **refuta** (maior observado: 0,98 cm) | **65** |
| **F2** — o laudo nao mede nada | **2** |

**Quase metade das protecoes estava errada.** A regra que faltava ja existia desde a `0.15.1`, na docstring de `_sem_vinculo`: *se o maior nodulo do laudo nao alcanca o limiar, nenhuma alcanca*. O erro foi trata-la como propriedade **daquele ramo** em vez de propriedade **da decisao**.

## O que muda no processo, e e o ponto

Quatro versoes seguidas corrigiram **um sintoma cada**, sempre escrevendo a correcao dentro do ramo que errava — o mesmo padrao que a `0.14.0` ja tinha nomeado (*"invariante de saida se verifica na saida"*).

Esta parte de uma **tabela de estados completa, com contagem medida em cada celula**. As duas que deram zero (`D` erro de infra, `H` criterio composto) estao declaradas como **sem populacao nesta coorte**, cobertas por teste — celula sem contagem e celula nao verificada.

## As mudancas

| # | | |
|---|---|---|
| 1 | `_maximo_refuta` vira **funcao propria** | a pergunta ganha um lugar so, e todo ramo que nao verificou a consulta |
| 2 | o ramo orfao a consulta **antes** de proteger | fecha `F` |
| 3 | *"sem medida nenhuma"* rebaixa tambem no ramo orfao | fecha `F2` |
| 4 | **`measure_lesion_maximo_documento`** no payload | ver abaixo |

**O item 4 nao e higiene.** A `0.15.3` apagava o valor do extrator **antes de persistir**, e as 155 orfas sairam com `value` nulo nos dois bracos: o payload nao distinguia `E` de `F`. **O defeito era invisivel na saida, nao so no codigo.** Vai em campo proprio porque `value` significa *a medida da lesao julgada*.

## Evidencia

- **Golden contra a `v0.15.3`**, mesmo script dos dois lados: **ZERO promovidos**, 4 rebaixados, e sao exatamente `F` e `F2` nos dois perfis quantitativos. Os oito perfis sem camada quantitativa ficam **byte a byte identicos**.
- **Corpus do golden** ganhou um laudo de `F` e um de `F2` — sem eles a comparacao daria identico por medicao vazia.
- **Oraculo de mutacao: 12 mutantes, 12 mortos** (6 novos e os 6 da `0.15.3` reconferidos).
- **Adjudicacao por leitura** (`CA9`): `Formacao ... 1,6 cm (TI-RADS 4)` protege certo; `Categoria ACR TI-RADS: 4` com nodulos de 0,6 e 0,7 cm rebaixa certo. **O extrator acertou o maximo** — era premissa da regra e nao estava conferida.
- Gate de sete alvos: **1.315 testes, 88,32% por ramo**, `release-check` coerente.

## Contrato

`measure_lesion_maximo_documento` e campo novo. Entra no alinhamento unico com o MLOps, junto com `0.13.0` a `0.15.3`.

## Pendente

`CA8` — coorte real, que e o espelho: os 121 do `306034` nao se movem, os 76 de `E` seguem entregues, os 65+2 de `F`/`F2` passam a rebaixar. **Depende deste merge e do seguinte**, porque dev instala do feed de producao.

SPEC: `docs/spec-0.15.4-refutacao-independe-da-atribuicao.md`

Card `306034` — *[NLP Engine] TI-RADS entrega a medida do nodulo errado: nao existe vinculo*

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

## PR 2 — `hml` → `main`

**Título**

```
release(0.15.4): promove para a main e publica no feed de producao
```

**Descrição:**

---

Promove a `0.15.4` para a `main`, o que publica no feed `fabrica-ai` (producao).

## O que entra

Corrige defeito introduzido pela `0.15.3`, medido em coorte real: a guarda de categoria orfa protegia **mesmo quando o laudo ja refutava o criterio** — **65 de 143** orfas tinham o maior nodulo do documento abaixo do limiar (maior observado: 0,98 cm), e mais 2 nao mediam nada.

A regra passa a ser consultada por **todos** os ramos que nao conseguiram verificar, em vez de viver dentro de um deles:

> se o maior nodulo do laudo nao alcanca o limiar, **nenhuma lesao alcanca** — inclusive a categorizada, seja ela conhecida, nao atribuida ou orfa.

## Risco do merge

**Publicar nao e adotar.** As seis definicoes de job pinam `"nlp_engine_version": "0.12.3"` literal, entao nenhuma linha passa a executar a `0.15.4` com este merge.

Excecao conhecida: **`cancer_colon` declara `${nlp_engine_version}`** em vez do literal, e ja variou de versao tres vezes em tres dias em hml. Uma linha no `cancer-colon-batch.json`, alcada nossa, tratada em separado.

A promocao e o que permite **fechar o `CA8` em dev**, porque o cluster de dev resolve o `pip` contra o feed de producao.

## Evidencia

Golden contra a `v0.15.3`, mesmo script dos dois lados: **zero promovidos**, 4 rebaixados, e sao exatamente as celulas corrigidas. Oito perfis byte a byte identicos. **12 mutantes, 12 mortos.** Adjudicacao por leitura de laudo nas tres celulas.

Gate de sete alvos: **1.315 testes, 88,32% por ramo**.

## Contrato

`measure_lesion_maximo_documento` — campo novo, e e o que torna a decisao auditavel na saida. Entra no alinhamento unico com o MLOps no fecho da serie.

SPEC: `docs/spec-0.15.4-refutacao-independe-da-atribuicao.md`

Card `306034` — *[NLP Engine] TI-RADS entrega a medida do nodulo errado: nao existe vinculo*

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

## Depois dos dois merges

1. **Conferir a publicação no próprio feed** — API de packaging, não o deploy verde. Foi o que
   falhou na `0.12.2`, quando a publicação em prd foi pulada com os dois builds verdes.
2. **Criar e pushar a tag `v0.15.4`** sobre o merge do PR 1 na `hml`. Ficou esquecida na `0.15.3`
   e só o `release-check` acusou depois.
3. **Rodar o `CA8`** — spec lido do run `109956348322922`, com **uma** variável alterada
   (`nlp_engine_version` → `0.15.4`), diff do payload conferido antes de enviar.

**O aceite do `CA8`, escrito antes de medir:**

| | esperado |
|---|---|
| os **121** do card `306034` | seguem rebaixados — se algum voltar, a guarda foi longe demais |
| os **76** da célula `E` | seguem entregues |
| os **65 + 2** de `F` / `F2` | passam a rebaixar |
| promovidos `0 → 1` | **zero** |

⚠️ **Pré-condição impressa junto com o resultado:** se `measure_lesion_maximo_documento` não sair
no braço novo, o caminho não rodou e a medição é vazia.
