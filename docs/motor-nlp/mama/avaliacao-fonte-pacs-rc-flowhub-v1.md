---
tags: [mama, birads, migracao-legado, flowhub, fonte-de-dados]
---

# Mama (BI-RADS) — de onde vêm os exames: RC, PACS ou FlowHub

> v1 · 03/11/2026 · avalia a fonte de dados da migração depois do ponto levantado na daily: o FlowHub não teria todos
> os dados do RC e poderia elevar custos. Complementa [[analise-migracao-mama-birads-v3]] (peça C) e
> [[analise-flowhub-pacs-v1]]. Janela 28/09–27/10/2026; só contagens; chaves de exame usadas em memória, nunca exibidas.

## Em resumo

**O complemento PACS do legado é, na maior parte, um problema de tempo, não de fonte.** 89% dos exames que o legado
busca no PACS chegam ao RC depois — e são perdidos pela plataforma porque ela seleciona pela **data do exame** do dia
anterior, enquanto o legado seleciona pela **data de liberação do laudo**, numa janela de 30 dias.

**Caminho recomendado:** RC como fonte única + **seleção por data de liberação com janela de retroação** no runner.
Recupera **165 dos 192** exames BI-RADS ≥ 4 que hoje só o PACS traz (86%), sem fonte nova e sem custo do FlowHub.
O FlowHub fica como **complemento opcional** para os ~27 BI-RADS ≥ 4/mês que nunca chegam ao RC — decisão de negócio.

## 1. O que foi medido

| pergunta | resultado |
|---|---|
| **A.** o RC tem mamografia que o FlowHub não tem? | **não:** 25.278 de 25.287 exames do RC (99,96%) estão no FlowHub (casam por `AN` = `num_pedido_integracao`) |
| **B.** o FlowHub tem o complemento PACS do legado? | **sim:** 3.236 de 3.236 (100%), todos como mamografia |
| **B'.** esse complemento chega ao RC? | **2.894 (89%) chegam depois**; **342 (11%) nunca chegam** |
| atraso de chegada ao RC (dos 2.894) | 1.838 até D+1 · **1.010 entre 2 e 7 dias · 46 após 7 dias**; 2.601 com data de exame anterior à janela do legado |
| BI-RADS ≥ 4 no complemento | **192**: 165 nos que chegam depois · **27 nos que nunca chegam** (7 deles BI-RADS 6, câncer já diagnosticado) |
| atualização | **FlowHub em dia (03/11)**; gold parada desde 30/10 |
| custo de leitura (1 dia de mama) | FlowHub **9,1 s** × RC **1,8 s** — ~5×; a view junta 12 tabelas de streaming |
| campos para o envio | FlowHub tem CPF, sexo, nascimento e celular (`pacs_streaming.paciente_gold`); **não tem convênio, CNPJ da unidade nem regional** |

⚠️ O atraso usa `etl_data_atualizacao_carga_gold`, que é a **última** atualização do registro, não a primeira chegada:
é limite superior do atraso. A proporção pode ser menor; a conclusão (problema de tempo) não muda.

## 2. As três opções

| | A. RC + janela por liberação (**recomendada**) | B. RC + FlowHub como complemento | C. FlowHub como fonte única |
|---|---|---|---|
| o que muda | runner seleciona pela data de liberação do laudo, com retroação (ex.: 7 a 30 dias) e a deduplicação por `id_exame` que já existe | segunda fonte no runner + exclusão por AN + enriquecimento de paciente e unidade | trocar a gold pelo FlowHub |
| BI-RADS ≥ 4 do PACS recuperados | **165 de 192 (86%)** | 192 de 192 | 192 de 192 |
| exames a mais por mês | ~2.900 | ~3.240 | — |
| custo | leitura do RC numa janela maior (filtrada por mama, pequena) | +5× por leitura do FlowHub; dependência de outra equipe | idem B |
| campos do envio | completos (RC) | precisa de RC para convênio, CNPJ, regional | **inviável:** faltam convênio, CNPJ e regional |
| alcance | beneficia **todas as linhas** com laudo tardio (1,7% dos exames de mama; outras linhas a medir) | só mama | — |
| riscos | mudança no runner (plataforma); medir volume extra por linha | SLA da view desconhecido; a tabela de unidades do FlowHub expõe colunas de senha de banco (governança) | — |

## 3. Recomendação

1. **Opção A primeiro**, como evolução do runner (plataforma): seleção pela data de liberação do laudo com janela de
   retroação. Medir em dev o volume extra e confirmar que os 2.894 entram.
2. **Opção B só se o negócio quiser os ~27 BI-RADS ≥ 4/mês restantes** (7 deles BI-RADS 6). Custo e dependência
   são reais e o ganho é ~1,4% dos ~1.923 casos BI-RADS ≥ 4/mês da linha.
3. **Opção C descartada** — o FlowHub não traz convênio, CNPJ e regional, que o contrato de envio exige.
4. **Governança:** reportar a exposição de colunas de senha em `especialistas.pacs_streaming.unidade_gold`.

## 4. A confirmar

- o atraso real de chegada (primeira carga, não última atualização) — define o tamanho mínimo da janela;
- quem decide e implementa a seleção por liberação no runner (alçada da plataforma);
- se a mesma perda por laudo tardio aparece nas outras linhas (TI-RADS, reumatologia…), o que reforça a opção A.
