---
tags: [biopsias, motion-lis, industrializacao, plano]
---

# Biópsias — plano de industrialização (visão macro)

> v1 · 10/11/2026 · da régua validada ao envio diário em produção. Régua: [[biopsias-definicao-envio-v2|definição do
> envio]] e consulta `consulta-lista-colon-biopsias-v3.sql` (não-regressão 114/114 contra a homologação). Motor proposto:
> [[proposta-motor-regras-estruturadas-v0]]. Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados
> do Motion LIS disponíveis no Repositório Clínico*.

## 1. Ponto de partida

| fato | consequência |
|---|---|
| A plataforma NLP só processa **laudo da gold pelo motor**: view, trilha de qualidade e controle de carga nascem no runner (`boas-praticas/01` §5; spec 22 — a view é montada sobre a tabela de saída do motor) | a linha de biópsias **não cabe no runner atual**; contornar é vedado — POP-IA-08 §14 manda abrir demanda |
| **Envio** (OneDrive/API) e **randomização** dependem só de uma **view com nome e colunas do contrato** (`boas-praticas/10`; `docs/randomizacao/ativar-inativar-projeto.md`) | essa parte se reaproveita inteira — mesmo padrão das demais linhas |
| POP de padronização de objetos: *"um produto não lê a camada canônica direto"*, *"regra determinística não é modelo"*, *"em lote, a inferência é task de diamond"*; POP-IA-07 prevê o schema `motionlis` no Datahub | a fonte do Motion entra pela cadeia de camadas do Datahub; a decisão grava em diamond |
| Régua validada (v3) e unidade disponível no próprio Motion | a lógica de negócio está pronta; o trabalho é de engenharia e esteira |

## 2. Arquitetura

```
Motion LIS (origem) ─► bronze/silver motionlis (Datahub, cadeia de camadas)
                         │
                         ▼
        MOTOR DE REGRAS ESTRUTURADAS (2º runner da plataforma)      ← config por linha
          grava diamond_fabrica_ia.<linha>: saída no contrato da plataforma
          (funil por etapa · explicação por linha · dedup por pedido · janela por emissão)
                         │
                         ▼
        PLATAFORMA (reaproveitada): view de exportação ─► randomização (opcional) ─► OneDrive / API Tabia
          navegação dev/hml/prd · relatório · job com pause_status · controle de carga
```

O runner NLP não muda. A demanda à plataforma é permitir que view e envio partam de uma saída produzida pelo novo
runner.

## 3. Fases

| fase | entrega | depende de |
|---|---|---|
| **0 · alinhamento** | decisão de arquitetura e de contrato; demandas abertas (schema, grants, identidade de serviço) | agenda com plataforma, Datahub e negócio |
| **1 · SPEC** | SPEC do motor ([[proposta-motor-regras-estruturadas-v0]]) e da linha: regra v3, contrato de saída, dedup por pedido, janela, unidade/paciente, "paciente já navegado = anotar", randomização | fase 0 |
| **2 · motor + produto de dados (dev)** | runner mínimo com **paridade 100%** com a consulta v3; job diário após a carga do Motion (~05:30), idempotente | schema e grants |
| **3 · exposição** | view no contrato, navegação nos três ambientes, job `PAUSED`, `disabled` em hml, randomização se decidida | fase 2 + demanda à plataforma |
| **4 · validação** | paridade com a v3 · cruzamento com o histórico da navegação (Dash AP) em período longo · envio de teste ponta a ponta em hml | dados da navegação |
| **5 · produção** | promoção `hml → main`, job ativo, painel de acompanhamento (volume diário, % sem RC, % indefinido, duplicados) | aprovação da fase 4 |
| **6 · integração Tabia** | campos de destino, anotação para paciente já navegado | fase 5 |

## 4. Riscos conhecidos

- **Cronograma do refinamento** (desenvolvimento 09–13/11, industrialização 16–20/11) não se sustenta sem a decisão
  da fase 0.
- **Identidade de serviço** depende de chamado — abrir já.
- **Fonte**: o Motion está hoje em `especialistas.*`, fora da cadeia do Datahub; ou é ingerido (POP-IA-07), ou fica
  registrada a exceção.
- **~7% dos pedidos do Motion não chegam ao RC**; dos 15 casos de cólon/reto sem RC na janela homologada, 12 são de
  unidades navegadas. Paciente pelo cadastro do Motion exige decisão de LGPD/governança.
- **Laudo reemitido** muda a data de emissão: sem dedup por pedido, o paciente é enviado duas vezes.
- **Documentação da plataforma diverge entre si** (envio, randomização em PRD, nomes de catálogo) — decidir pelo código.

## 5. Decisões da fase 0

1. Motor de regras estruturadas como **2º runner da plataforma** (recomendado) ou consulta própria por linha?
2. Plataforma aceita **view com base produzida fora do motor NLP**?
3. Paciente só do RC (perde ~7%) ou também do Motion?
4. Randomização nesta linha?
5. Donos: código do motor · vocabulário compartilhado · fonte do Motion.
