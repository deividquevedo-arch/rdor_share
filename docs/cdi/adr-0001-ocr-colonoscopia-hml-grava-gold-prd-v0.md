---
tags: [cdi, ocr, adr, datahub, colonoscopia]
---

# ADR 0001 — OCR de colonoscopia roda em HML e grava na gold corporativa de produção

| | |
|---|---|
| status | **proposto** — v0, 11/11/2026 |
| dono do processo | Datahub · OCR |
| repositório | `fabrica-ia-datahub`, branch `hml` (base `586f235`) · job `datahub-diamond-colon-colonoscopia` |
| destino sugerido | `docs/adr/0001-ocr-colonoscopia-hml-grava-gold-prd.md` no `fabrica-ia-datahub` |
| vigência | **provisória** — até a promoção do processo a PRD (cherry-pick avaliado para a `main`) |

## 1. Contexto

- Laudos de colonoscopia cujo texto na gold é só um aviso ("laudo em pdf", "acesse o viewer", "se encontra no
  sistema", vazio) chegam ao motor NLP sem conteúdo e são **descartados** — não entram na captação.
- O processo de OCR (N1–N8) recupera o texto do PDF pelo Viewershell e o grava de volta na gold. Foi validado em
  simulação e acompanhado por alguns dias.
- Levar o processo a PRD exige subir o framework do Datahub completo para a `main`, com avaliação e seleção pontual
  para cherry-pick, que não cabe no prazo atual — a esteira de PRD do Datahub está sem responsável disponível e o
  processo está atrasado.

## 2. Decisão

O job roda **no workspace de HML**, com a identidade `sp-fabrica-ia-nlp-platform-hml`, lendo e gravando na tabela de
produção `gold_corporativo_ia.corporativo.tb_gold_mov_exame`.

**Por que é seguro o bastante:** o processo só toca laudos que hoje **já são descartados**. Se o OCR produzir texto
ilegível, o motor segue descartando — o resultado para a captação é o mesmo de hoje. A escrita é protegida por hash
(só grava se o laudo da fonte não mudou desde a seleção) e tem rollback por time travel.

## 3. Escopo da escrita

| | |
|---|---|
| tabela | `gold_corporativo_ia.corporativo.tb_gold_mov_exame` |
| linhas | itens de `proced_lista_exames` de **colonoscopia** com aviso de PDF/viewer ou vazio, `num_pedido_integracao` de 16 dígitos, `dt_dia_exame` nos **últimos 3 dias** |
| campos | `laudo_transformado` **e** `laudo_original` do item — nenhum outro campo, nenhum outro item |
| volume | 20 a 38 itens elegíveis por dia útil (medido de 01 a 10/11/2026) |
| proteção | `MERGE` por `id_exame`; troca o item só se `sha2(laudo_transformado)` for igual ao da seleção |

**Por que os dois campos:** o runner da plataforma monta o texto do motor a partir de `laudo_original`
(`nlp_ia_02_input.py:511`; configs de PRD de `cancer_colon` e `doenca_inflamatoria_intestinal` mapeiam
`exm_laudo_texto` para esse campo). Gravar só `laudo_transformado` não muda nada na captação. Mudar o runner para ler
`laudo_transformado` alteraria as 13 linhas e dependeria de promoção da plataforma — fora do prazo.

## 4. Ajustes no processo que esta decisão exige

| # | ajuste | onde | quando |
|---|---|---|---|
| A | gravar o OCR também em `laudo_original` | N5, `ELEMENT_FIELDS` | antes de ligar |
| B | janela por data: `dt_dia_exame >= date_sub(current_date(), janela_dias)`, default 3 | N1 | antes de ligar |
| C | `fonte_exames = gold_corporativo_ia.corporativo.tb_gold_mov_exame` | N1, N5 e deriva | antes de ligar |
| D | grants para a SP pelo applicationId `1d37d7b8-d347-4eaa-95cf-0d0442173826`: `USE CATALOG`, `USE SCHEMA`, `SELECT, MODIFY` na tabela | administrador Databricks | antes de ligar |
| E | `raise` no fim da N5 quando houver falha de `MERGE` ou linha `FAILED`, depois de gravar a trilha | N5 | em seguida |
| F | agendamento diário às 02:00 (America/Sao_Paulo) | definição do job | em seguida |

**B é condição, não melhoria:** sem janela, o N1 seleciona os 500 menores AN da tabela inteira — medido em 11/11:
exames de 24/07/2025 a 24/04/2026, que o runner nunca relê.

## 5. Encaixe com a plataforma

| evento | horário (BRT) |
|---|---|
| carga diária da gold corporativa (`MERGE`, ~1,7 milhão de linhas atualizadas) | ~09:00 |
| OCR (ajuste F) | 02:00 |
| runner do `cancer_colon` | 04:00, seg–sex |

O runner processa os exames de **dois dias antes** (os exames de um dia só entram na carga das 09:00 do dia seguinte)
e não reprocessa `id_exame` já visto. Por isso o OCR precisa gravar **antes** da primeira leitura do runner sobre
aquele exame — a janela de 3 dias com execução às 02:00 cobre isso.

## 6. Consequências

**Esperadas**
- Laudos de colonoscopia hoje descartados passam a chegar ao motor com texto, nas linhas `cancer_colon` e
  `doenca_inflamatoria_intestinal`. Os que forem relevantes **passam a ser enviados à navegação** — é o ganho buscado.

**Aceitas**
- OCR **legível porém errado** pode virar entrega. A tese "o motor descarta" cobre texto ilegível, não texto
  plausível e incorreto. Mitigação: acompanhamento diário (§7) e critério de desligamento (§8).
- A carga diária pode devolver o aviso a um item já gravado. Sem efeito na captação (o runner já leu); o item volta à
  fila dentro da janela e é reprocessado. Custo limitado pela janela.
- Escrita em tabela de produção a partir de HML, fora da esteira de PRD. Provisório (§9).

## 7. Operação e acompanhamento

1. Primeira execução com `modo_simulacao=true`: a seleção deve trazer exames dos últimos 3 dias (dezenas), não 500.
2. Antes do primeiro `MERGE` real, registrar a versão Delta da tabela (`DESCRIBE HISTORY`).
3. Execução com `modo_simulacao=false`.
4. Diariamente: itens `UPDATED` × `FAILED` na trilha; na saída do `cancer_colon` e da DII, quantos desses `id_exame`
   chegaram com texto e quantos viraram relevantes.

## 8. Rollback e desligamento

- **Desligar:** pausar o job ou voltar `modo_simulacao=true`. Nada mais é gravado.
- **Reverter o que foi gravado:** restaurar os itens a partir da versão registrada (`VERSION AS OF`), só para os
  `id_exame` com `UPDATED` na trilha. **Janela de 7 dias** — a tabela roda `VACUUM` diário.
- **Critério para desligar:** falha sistemática de `MERGE`; texto do OCR divergente do PDF em amostra conferida;
  entrega à navegação contestada por erro de OCR; ou pedido da engenharia dona da carga.

## 9. Saída desta decisão

Vale até o processo ser promovido a PRD pela esteira do Datahub (`main` → `jobs/ambientes/prd.json`), com identidade
de serviço de PRD. Nessa promoção, revisar: os grants de `MODIFY` concedidos à SP de HML (revogar), a dupla escrita em
`laudo_original` × leitura do runner (alinhar com a plataforma) e o agendamento.

## 10. Ciência

| papel | ciência |
|---|---|
| dono do processo (Datahub · OCR) | |
| Ciência de Dados (linhas `cancer_colon` e DII) | |
| administrador Databricks (grants) | |
| engenharia dona da carga da `tb_gold_mov_exame` | |
