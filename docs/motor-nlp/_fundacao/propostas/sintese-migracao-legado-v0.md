---
tags: [migracao, legado, sintese]
---

# Síntese — migração de Mama, Nódulo pulmonar, Nefro e Hepato

> 15/10/2026. Consolida, em uma página, a
> [[auditoria-legado-quatro-linhas-2026-10-14|auditoria]], o
> [[inventario-legado-quatro-linhas-2026-10-14|inventário]], o
> [[mapa-migracao-legado-quatro-linhas-v0|mapa]], o
> [[resumo-migracao-legado-quatro-linhas-v0|resumo]] e o detalhamento de
> [[entradas-processamento-saidas-legado-v0|entradas, processamento e saídas]].
> Card do plano: `303791` — *Plano de Migração algoritmos final*.

## Situação

As quatro linhas **rodam todo dia** no `workspace-analytics-bi-hml-02`, a partir dos repositórios
canônicos na branch `hml`, disparadas por um orquestrador. Nenhuma envia à Central de Captação.

| linha | o que é | volume (01/09–14/10) | destino hoje |
|---|---|---|---|
| **Mama** | categoria BI-RADS do laudo | 43.222 exames | Mule — Portal de Acolhimento |
| **Nódulo** | extração estruturada de nódulos | 52.870 exames | tabela lida pelo Salesforce |
| **Nefro** | cálculo de clearance sobre creatinina — **não é NLP** | 1.270.366 exames | view lida por consumidor não identificado |
| **Hepato** | motor já na plataforma; falta o **ciclo** | 193.230 exames | board médico → retorno → navegação por e-mail |

## Por linha

| | Mama | Nódulo | Nefro | Hepato |
|---|---|---|---|---|
| **entrada** | gold de exames (4 colunas de filtro, IMG/IMA) + paciente + **PACS**; 30 dias até D-1 | gold de exames (3 colunas de filtro) + paciente com **idade**; 30 dias | **laboratório FHIR** (TUSS 40301630) + sexo e idade; 5 dias; sem laudo | gold do motor + **retorno do board** |
| **processa** | categoria BI-RADS, sem legenda ACR, máximo do laudo | spaCy + 13 dicionários + 2 modelos; tamanho, local, tipo, classe, evolução, conduta | CKD-EPI | motor → board → retorno → transplante / onco + baixa complexidade |
| **saída** | **todos os exames** com categoria −1 a 6 (33 colunas) | um registro por exame com o nódulo descrito (37 colunas) | clearance 0,1–60, sem AMIL (24 colunas) | planilha do board, tabela de retorno, duas navegações |
| **régua na plataforma** | ✅ config pura | 🔴 exige lib | 🔴 não se aplica | ✅ pronta |
| **além da config** | envio Mule · view de todas as categorias · PACS · janela móvel · schema | lib por lesão · idade · tabela de entrega · modelos e 13 CSV em Volume · schema | processador não-NLP · entrada de laboratório · consumidor · validação clínica · schema | captura de retorno · navegação por fluxo · histórico (5.836) · SAS em secret |

## O que a `nlp_platform` precisa ganhar

| etapa | evolução | linhas |
|---|---|---|
| entrada | filtro em várias colunas + `tp_procedimento` | mama, nódulo |
| entrada | laudo por `laudo_transformado`, RTF como fallback | mama, nódulo, hepato |
| entrada | janela móvel por data com reprocessamento | mama, nódulo |
| entrada | segunda fonte (PACS) | mama |
| entrada | idade e sexo do cadastro para o processamento | nódulo, nefro |
| entrada | entrada de laboratório | nefro |
| processamento | processador não-NLP plugável | nefro |
| processamento | saída por lesão (lib) | nódulo |
| processamento | categoria ordinal como coluna, todos os exames | mama |
| view | projeção sem filtro de relevância, com colunas de contrato | mama, nódulo, nefro |
| envio | disparo + espera de confirmação (Mule) | mama |
| envio | tabela de entrega lida por sistema externo | nódulo, nefro |
| envio | captura de retorno + navegação a partir dele | hepato |
| orquestração | job de pós-processamento com agenda própria | hepato |
| artefatos | modelos e dicionários em Volume de prd | nódulo |

**Só uma evolução é de biblioteca** — a saída por lesão do nódulo. As demais são de plataforma.

## Ordem

1. **Hepato** — ciclo de retorno e navegação; metade já existe.
2. **Mama** — régua em config e run em dev em paralelo; envio quando o destino estiver decidido.
3. **Nefro** — decidir onde vive antes de portar.
4. **Nódulo** — medir quais das 37 colunas o Salesforce usa; até lá, permanece no legado.

## Decisões necessárias

| decisão | quem |
|---|---|
| destino de mama, nódulo e nefro: sistema atual ou Central | negócio |
| nefro dentro ou fora da plataforma | plataforma + DS |
| relevância da nefro: ≤ 60, ≤ 35, idade ≤ 80 | negócio |
| PACS na mama: mantém ou sai | negócio |
| colunas do Salesforce efetivamente usadas | negócio |

## Critério de corte, por linha

Plataforma em prd com perfil completo · janela em paralelo com delta por `id_exame` adjudicado ·
destino recebendo da plataforma, verificado do lado de lá · job do legado desligado no mesmo dia.

## Contexto (fora da nossa alçada)

- O orquestrador do workspace antigo não é visível; desligar o legado depende de quem o mantém.
- Envio à Central fora dos jobs desde 09/10 — incidente `371813` — *[Captação] Todas as linhas
  indisponíveis*.
- Fontes ainda sem identificação: carga do PACS e consumidores das saídas de nefro e nódulo.
