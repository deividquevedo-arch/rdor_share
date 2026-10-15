---
tags: [randomizacao, captacao, criterios-de-aceite]
---

# Proposta de conciliação — história `303629`

> v0 — 15/10/2026. História `303629` — *[Financeiro] Refinamento Randomização e mensuração de
> conversão financeira da Captação*. Feature-mãe `307254` — *[Captação] Randomização algoritmos*;
> implementação na `321370` — *[NLP Platform] Aplicar randomização - Reumato e Tireoide*.
> Evidência medida em produção, 12 a 15/10/2026, no catálogo `mlops_fabrica_ia.operacao`.

## 1. Por que conciliar

A história e os critérios técnicos da implementação descrevem a mesma solução, com três
divergências de texto que impedem validar a história contra o que roda:

| divergência | onde |
|---|---|
| **percentual** — a descrição fala em 50 milhões de controle em 1 bilhão (5%); CA01 e CA13 pedem 10% | descrição × CA01, CA13 |
| **gravação** — CA09 pede consulta "sem gravar nada"; os critérios técnicos pedem o grupo registrado por CPF e por linha, de forma auditável | CA09 × critérios técnicos |
| **cronograma** — 28/09, 05/10 e 13/10 nos critérios técnicos; a produção começou em 12/10 nas duas linhas | critérios técnicos × operação |

## 2. Proposta por critério

| CA | texto atual (resumo) | proposta | evidência hoje |
|---|---|---|---|
| CA01 | ~10% controle | **fração parametrizável, 5% na fase de teste e 10% após decisão registrada**; a fração só sobe | base com 1.000.000.000 de CPFs, 50.001.012 em controle (5,0001%) |
| CA02 | regra determinística sobre o CPF, sem sorteio na identificação | mantém | grupo vem da base pré-gerada; 618 de 618 registros iguais à base |
| CA03 | mesmo CPF, mesma classificação | mantém | zero CPF com dois grupos; 28 CPFs em mais de um dia, todos estáveis |
| CA04 | mesmo grupo em qualquer linha | mantém, **com nota de verificação**: sem população até 15/10 | nenhum CPF apareceu nas duas linhas randomizadas |
| CA05 | mesmo grupo em qualquer data | mantém | mesma evidência do CA03 |
| CA06 | todo CPF tem grupo | mantém; **acrescentar a regra para paciente sem CPF** — hoje é enviado à navegação e registrado como `sem_chave` | zero registros sem CPF no período |
| CA07 | paciente novo já classificado | mantém | a base cobre todo o espaço de CPFs válidos |
| CA08 | base não é recalculada | **reescrever:** a base só é regerada quando a fração sobe; a regeneração é determinística e só move CPFs de navegação para controle | regra implementada; o código recusa queda de fração |
| CA09 | consulta sem gravação | **reescrever** (texto abaixo) | log por CPF, linha, data e motivo |
| CA10 | controle não vai à navegadora | mantém | zero controle enviado; view enviada sem nenhum CPF de controle |
| CA11 | navegação segue o fluxo normal | mantém | 216 exames de navegação enviados |
| CA12 | grupo identificável para análise | mantém | `tb_mov_randomizacao_log` |
| CA13 | proporção compatível com 10% | **"compatível com a fração vigente"**; acompanhada no relatório | 11 de 227 (4,85%) com fração de 5% |

**CA09 proposto:**

> *Dado um paciente identificado pelo modelo, quando o processo consultar sua classificação, então
> deverá recuperar o grupo da base pré-gerada, sem novo sorteio e sem alterar a base; a consulta é
> registrada em log por CPF, linha e data, para auditoria e mensuração.*

**Critérios a acrescentar:**

| novo | texto |
|---|---|
| CA14 — Contaminação | *Dado um paciente do grupo controle numa linha randomizada, quando ele for relevante em outra linha, então o caso deverá ser identificável no relatório.* |
| CA15 — Sem CPF | *Dado um paciente sem CPF válido, então ele segue para a navegação, é registrado como sem chave e fica fora da análise de impacto.* |
| CA16 — Escopo da conclusão | *O efeito será estimado de forma agregada entre as linhas randomizadas; nenhuma linha isolada sustenta conclusão própria.* |

## 3. Cronograma — registrar o que aconteceu

| marco do critério técnico | ocorrido |
|---|---|
| reumatologia em prd em 28/09, sozinha na 1ª semana | hml desde 30/09; **prd desde 12/10**, junto com TI-RADS |
| TI-RADS em prd em 05/10 | hml desde 07/10; **prd desde 12/10** |
| relatório semanal | relatório **diário** no Teams; o semanal é o card `358477`, em refinamento |
| 13/10: 10% e todas as linhas | **não executado**; segue 5% em duas linhas |

Proposta: substituir as datas fixas por **marcos condicionados** — duas semanas completas em prd
(até 25/10), relatório das duas semanas, decisão registrada de fração e de linhas.

## 4. Pré-condições antes de subir para 10% e liberar linhas

1. Conciliação acima aceita.
2. Formalização de LGPD do registro do grupo controle (`tb_mov_randomizacao_intervencao` guarda
   PII e laudo integral).
3. Decisão sobre contaminação entre linhas — ver o relatório.
4. Linhas a liberar listadas uma a uma; a fração não volta.
