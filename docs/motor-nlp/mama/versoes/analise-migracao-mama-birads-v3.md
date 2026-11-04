---
tags: [mama, birads, migracao-legado]
---

# BI-RADS (mama) — migração do legado para a plataforma NLP

> v3 · 29/10/2026 · alinhamento com a plataforma. Medições de 28/09 a 27/10/2026, só agregados, no código e nas
> tabelas que rodam em produção (`IAAzureDatabricksBiRads`, branch `hml`, `75a5898`).
> Cards: `303791` — *Plano de Migração algoritmos final* · `304135` — *[BI-RADS] Levantamento para Migração*.

## 1. Em resumo

- **A leitura do laudo está resolvida:** a régua migra por config, sem código novo na lib.
- **O que falta são três peças de plataforma**, fora do motor:

| | peça | por quê |
|---|---|---|
| **A** | **envio para a Mule** | a Mule lê do workspace antigo; a plataforma só envia à Central de Captação |
| **B** | **contrato de saída** | o BI-RADS entrega a categoria de todo exame, um paciente por dia |
| **C** | **fonte PACS** | complementa a gold com +11% dos casos BI-RADS ≥ 4 |

- **Quatro decisões do negócio vêm antes** (seção 6).

## 2. A linha em números (30 dias)

| | exames | BI-RADS ≥ 4 |
|---|---|---|
| gold (repositório clínico) | 26.966 | 1.731 |
| PACS (complemento) | 3.236 | 192 |
| **total** | **30.202** (~1.000/dia) | **1.923** (~64/dia) |

## 3. O que já está coberto

| | evidência |
|---|---|
| régua | config de `ordinal_extraction`, como o PI-RADS em produção; bancada: 99,9% de acerto da categoria (legado 99,6%) |
| filtro de entrada | 99,4% do filtro do legado (163 exames de diferença em 30 dias) |
| texto do laudo | 99,5% legível na gold |
| validação, notificação, dados do paciente | mecanismos existentes da plataforma |

## 4. As três peças que faltam

### A. Envio para a Mule

**Hoje:** o legado copia a saída para uma tabela no `hive_metastore` do workspace antigo
(`ia.tbl_gold_modelo_birads_complemento_v2_lake2`) e faz um `GET` na API da Mule, que **busca** essa tabela. O job
espera até 4 h o retorno da Mule em `logs.api_birads` e marca o exame como integrado.

**Na plataforma:** não há esse caminho. Ou a Mule passa a ler do catálogo novo (com disparo e retorno construídos
no `data_exchange`), ou a linha passa a enviar para a Central de Captação.

### B. Contrato de saída

**Hoje:** a categoria de **todo** exame (−1 a 6), **um registro por paciente por dia** (a maior categoria), e
descarte de registros sem os campos obrigatórios (nascimento, sexo, convênio, telefone, unidade, CNPJ…).

**Na plataforma:** a view entrega só relevantes e deduplica por paciente + data + procedimento. Precisa de uma view
com o contrato do BI-RADS.

### C. Fonte PACS

**Como o legado junta as duas fontes, a cada dia:**

```
gold do dia ──────────────────────────────┐
                                          ├──► entrada única ──► régua ──► views ──► envio
PACS do dia ─► texto (RTF/HTML) ─► tira o que já está na gold ─┘
                                   (por pedido, depois CPF, depois nome)
```

O que sobra do PACS entra marcado como `cod_origem = 'PACS'` e segue o mesmo caminho.
⚠️ O casamento por CPF e nome é **por paciente**: se o paciente tem qualquer exame de mama na gold no dia, o do PACS
é descartado.

**Para replicar na plataforma:**

| # | o quê | alçada |
|---|---|---|
| 1 | trazer o PACS para o catálogo novo — hoje só existe no workspace antigo, alimentado por ADF a partir de servidor on-premises | Engenharia de dados |
| 2 | segunda fonte no runner: ler o PACS, converter o laudo, mapear para o contrato de entrada e excluir o que já está na gold | Plataforma |
| 3 | identificador próprio para o exame do PACS, que não tem `id_exame` da gold | Plataforma |
| 4 | view de exportação que aceite linha sem par na gold (unidade e paciente vindos do PACS) | Plataforma |
| 5 | permissão para descriptografar CPF e nome no casamento (`security.prd.rdsl_decrypt`) | Plataforma / Segurança |

É a peça mais cara: o item 1 é pré-requisito e os itens 2 a 4 mudam runner e view.

## 5. Itens menores

| item | tamanho | alçada |
|---|---|---|
| laudo liberado com atraso — o legado pega pela data de liberação; a plataforma, pela data do exame | 1,7% dos exames | Plataforma |
| e-mail de inconsistências por unidade (Excel via Logic App) | fluxo auxiliar | Negócio + Plataforma |
| schema `birads` em dev, hml e prd | — | Fábrica |
| desligar o legado — acesso ao orquestrador do workspace antigo | — | Plataforma |
| config `birads`, bancada na lib `0.12.3`, run em dev | — | DS |

## 6. Decisões do negócio

1. **Destino:** manter a Mule ou ir para a Central de Captação? → define A.
2. **Contrato:** categoria de todo exame ou só relevantes? → define B.
3. **Relevância:** BI-RADS ≥ 4, ou também achado clínico (~26% a mais)? → config nos dois casos.
4. **PACS:** manter (+11% dos casos ≥ 4, custo da peça C) ou aceitar a perda?

## 7. Sequência

1. **Já:** config e bancada (DS) · schema (Fábrica).
2. **Após as decisões:** peças A, B e C, conforme o que for decidido.
3. **Paralelo em hml:** legado e plataforma na mesma janela, comparação por `id_exame`, sem envio duplo.
4. **Corte:** desligar o legado no orquestrador.

## 8. Não verificado

- Retorno da Mule (`logs.api_birads`): sem permissão de leitura.
- Se os exames do PACS chegam depois à gold — define se a perda sem o PACS é de prazo ou de cobertura.
