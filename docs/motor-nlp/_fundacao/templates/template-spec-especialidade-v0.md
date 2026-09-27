# Template de SPEC de negócio — v0

> **Isto não é um formato novo.** É a formalização do padrão que já emergiu em
> [`spec-negocio-tireoide-v3.md`](../../tireoide/spec-negocio-tireoide-v3.md) e
> [`spec-negocio-transplante-pulmao-v1.md`](../../pulmao/spec-negocio-transplante-pulmao-v1.md),
> mais quatro seções que faltavam e cuja ausência já custou retrabalho (§8, §9, §11, §12).
>
> **Nome do arquivo:** `spec-negocio-<linha>-v<N>.md`, na pasta da especialidade.
>
> **Esta é a SPEC VIVA — o documento de manutenção e evolução.** Ela **não** é por onde se começa:
> quase tudo aqui só existe depois de medir. O documento de entrada é o
> [briefing de negócio](template-briefing-negocio-v0.md), que nasce da conversa e **congela**.
>
> ```
> conversa de negócio → briefing-negocio-<linha>-v0   (congela, é o que foi pedido)
>        ↓ filtro confirmado · volumetria medida · gabarito definido · 1º run
> spec-negocio-<linha>-v1   (viva, evolui com as decisões)
>        ↓
> config X.Y.Z   (implementa)
> ```
>
> Só existe SPEC depois dos passos 1, 2, 3 e 6 do briefing. Antes disso há briefing e hipótese.

---

## Regra de vínculo SPEC ↔ config

**A SPEC versiona DECISÃO. A config versiona IMPLEMENTAÇÃO.**

| o que mudou | SPEC | `config_version` |
|---|---|---|
| decisão de negócio (entra/sai achado, muda limiar ou critério) | **sobe** | sobe MINOR |
| correção de régua sem mudar decisão (regex, sinônimo, defeito) | não muda | sobe PATCH |
| troca de plataforma sem mudar comportamento | não muda | sobe MINOR, régua byte-idêntica |

O cabeçalho da SPEC nomeia a **config**; a §12 lista todas as configs que a implementaram.
Uma SPEC pode ter várias configs; uma config aponta para **uma** SPEC.

---

# SPEC de negócio — Linha de cuidado \<NOME\> (V\<N\>)

**Versão:** X.Y · **Data:** AAAA-MM-DD · **Config:** `X.Y.Z-<schema>` · **Motor:** `nlp_engine >= X.Y.Z`
**Status:** discovery | especificado | implementado | homologado
**Fonte:** documento de negócio, com data e autor — ex.: *"spec fornecida pelo usuário (2026-07-14)
+ arquivo 'Palavras chave e exames'"*

## 1. Objetivo

O que a linha capta e para quê. Uma frase sobre o **princípio**, antes de qualquer lista de
palavras — a lista envelhece, o princípio não.

> *Ex. (ca-estômago): entra o laudo em que há decisão em aberto que a navegação pode acelerar e o
> paciente ainda não está capturado; achado descrito na conclusão do exame atual entra, doença
> citada em indicação, histórico ou nota não entra.*

## 2. Universo de exames

**Entram na seleção**, por nome de exame:

**Ficam fora, por decisão:** — com o motivo, não só a lista.

> Declarar aqui evita puxar modalidade inteira sem uso. Na tireoide, exames de sangue eram 71% da
> entrada gerando zero relevantes.

## 3. Régua

Por modalidade (imagem, sangue, endoscopia…) ou por versão, o que fizer sentido para a linha.

### 3.1 Critérios

### 3.2 O que NÃO conta

### 3.3 Origem de cada limiar / termo

> Seção herdada da tireoide V3 e **obrigatória**: para cada número ou palavra-chave, de onde veio —
> spec clínica original, decisão de reunião, faixa de referência de laboratório, medição no lake.
> É o que permite rever um critério anos depois sem reabrir a discussão inteira.

### 3.4 Valores censurados e implausíveis (quando houver medida)

> `< 0,01` não é zero. E ausência gravada como `0` satisfaz qualquer `<` e vira falso positivo em
> massa — 187 TSH/mês na tireoide.

## 4. Decisões fechadas

| # | decisão | quem | data | origem |
|---|---|---|---|---|

> Transcrever a resposta **literal** do negócio, e também **o que a pergunta enumerava** — sem isso
> a resposta fica ambígua depois. *"Somente as úlceras"* só é interpretável sabendo que a pergunta
> dizia *"úlceras (com e sem sinais de malignidade associados)"*.

## 5. Decisões abertas / GAPS

| # | pergunta | espera quem | desde | custo de não decidir |
|---|---|---|---|---|

## 6. Quem decide

| papel | quem | alcance |
|---|---|---|
| dono de negócio | | **escopo — a palavra dele vence** |
| apoio clínico | | baliza e sugere; não define escopo |
| ciência de dados | | régua, métrica, plataforma |

> ⚠️ Nomear. Quando duas vozes clínicas divergem, a SPEC diz de quem é a decisão. Sem isso a régua
> obedece quem falou por último, e a divergência só aparece na revisão do lote.

## 7. Mapeamento para o motor

Cada critério da §3 → a camada que o implementa (`findings`, `quantitative_criteria`,
`ordinal_extraction`, `llm_router`).

> **Não redeclarar aqui a cascata `regra → expansão → juiz`.** Ela é **invariante da lib**, não
> decisão de especialidade: juiz que decide sem evidência de regra é quebra de arquitetura, e nenhuma
> SPEC tem autoridade para relaxar isso. O que a SPEC herda dela é uma consequência prática, e essa
> sim precisa estar escrita: **o prompt só filtra — toda inclusão de escopo tem de virar achado de
> regra.**

**Banda de incerteza** — é o parâmetro de juiz que a SPEC fixa: define **quando** ele é chamado.
Justificar com número — medir o teto de score dos laudos **sem** achado e o piso dos **com** achado,
e cortar logo acima do teto sem achado. Revalidar sempre que mudar régua, pesos ou política de score.

## 8. Gabarito 🆕

- **Origem** (caminho exato) · **quem anotou** · **quando** · **sob qual versão desta SPEC** · **n**

> Anotação feita **antes** de uma decisão não é gabarito para a régua **de depois**. Sem essas
> cinco linhas o gabarito não pode aprovar versão. Planilha de negócio não é gabarito por si:
> cruzar por `id_exame`.

## 9. Baseline e volumetria 🆕

| | valor | como foi medido |
|---|---|---|
| janela | AAAA-MM-DD a AAAA-MM-DD (N dias) | |
| laudos processados | | |
| relevantes/dia | | |
| recall / precisão | | contra o gabarito da §8, **com o juiz ligado** |

> Preencher **antes** de mexer em qualquer coisa. Migração sem baseline não prova ganho.
> Todo item da §3.2 (o que não conta) deve trazer **o custo em laudos/dia** — "é comum demais"
> sem número é opinião.

## 10. Requisito de output e homologação

Formato do `findings` entregue ao negócio, colunas obrigatórias, e o fluxo de homologação
(lote, quem revisa, prazo).

## 11. Armadilhas verificadas 🆕

Todas já morderam neste projeto. Nenhuma dá erro: todas falham em silêncio.

- [ ] **cola de acento** — some o espaço antes de palavra acentuada (`deúlcera`); `\b` inicial não casa
- [ ] **laudo de uma linha** — quebra toda regra que opera por linha
- [ ] **`ignore_sections`** — `notas` não casa `Nota:`
- [ ] **gate de órgão** — derruba achado real; testar com controle positivo **e** negativo
- [ ] **dedup da entrada** — bloqueia janela em silêncio, run fecha em segundos com SUCESSO
- [ ] **`segmentation.mode`** — `auto` pode descartar CONCLUSÃO; medir `segmentation_coverage`
- [ ] **config engolida** — bloco desconhecido some sem log; todo teste atravessa o carregador
- [ ] **vocabulário estrangeiro** (migração) — régua legada pode carregar outra especialidade

## 12. Histórico SPEC ↔ config 🆕

| SPEC | config | data | decisão que mudou |
|---|---|---|---|
