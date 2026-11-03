---
tags: [flowhub, pacs, fonte-de-dados, mama]
---

# FlowHub como fonte do PACS — análise

> v1 · 29/10/2026 · complementa a [[analise-migracao-mama-birads-v3|análise da migração do BI-RADS]], peça C.
> Só estrutura e contagens agregadas.

## Em resumo

**Sim: os dados do PACS já chegam ao workspace novo, via FlowHub, e casam com a gold pelo AN.** A peça C do
BI-RADS deixa de exigir que a engenharia de dados traga a view do workspace antigo.

## 1. O que é o FlowHub

Fluxo corporativo de replicação de tabelas dos sistemas de origem em tempo real (Kafka no Confluent Cloud, com
captura de mudança — colunas `Op`, `AWS_DMS_TIMESTAMP`, `offset`). Carrega, entre outros:

- **RIS/PACS** — ex.: tópico `prd.flowhub.ris.pacs.dbo.atendimento`, com o laudo em RTF (`laudo`) e HTML (`LaudoHTML`);
- **TASY** — atendimento e prescrição (schema `diamond_fabrica_ia_dev.flowhub`, só em dev).

Uso anterior na Fábrica: `fabrica-ia-plataforma/apps/databricks/dr_captacao/ntb_ia_dr_captacao.ipynb` (PR 6856,
07/2026) lia o tópico do PACS direto do Kafka — experimento, não processo em produção.

## 2. Onde o PACS já está, no workspace novo

| objeto | o que é |
|---|---|
| `especialistas.pacs_streaming.*_gold` (12 tabelas) | tabelas de streaming alimentadas pelo FlowHub: atendimento, paciente, procedimento, unidade, requisitante, status… |
| **`especialistas.gold.vw_pacsdor_informaticamedica_enterprise_repositorio_clinico`** | view consolidada sobre elas — **a equivalente da view PACS do legado** |

**Comparada à view do legado** (`hive_metastore.ia.vw_enave_df_historico_birads`): mesmas colunas de atendimento,
AN, exame, título, modalidade, solicitante, médicos, liberação e laudo (`LaudoRTF` ↔ `resultado_exame`, `LaudoHTML`).
**Faltam os enriquecimentos** que o legado acrescenta: CPF, CNPJ, regional, sexo e telefone.

## 3. Medido (28/09 a 27/10/2026, mamografias, sem biópsia)

| | |
|---|---|
| acesso | ✅ leitura liberada · atualizada até **29/10/2026** |
| mamografias liberadas | **29.651**, todas com laudo (RTF ou HTML) |
| casam com a gold por **`AN` = `num_pedido_integracao`** | **24.216 (81,7%)** |
| **sem par na gold pelo AN** | **5.435** — o complemento candidato |

O legado, depois de casar também por CPF e nome, fica com **3.236** — mesma ordem de grandeza. A diferença se explica
pelos dois casamentos a mais (por paciente), que esta medição não aplicou.

## 4. O que muda na peça C do BI-RADS

| item da v3 | antes | com o FlowHub |
|---|---|---|
| 1. trazer o PACS para o catálogo novo | engenharia de dados, a partir de servidor on-premises | ✅ **já existe** — consumir a view acima |
| 2. segunda fonte no runner | necessário | necessário — ler a view e excluir o que casa com a gold pelo AN |
| 3. identificador próprio | necessário | necessário — o `AN` serve de chave |
| 4. view de exportação para linha sem par na gold | necessário | necessário — e precisa dos enriquecimentos (CPF, CNPJ, regional, sexo, telefone) |
| 5. permissão de descriptografia | para casar por CPF e nome | só se o casamento por paciente for mantido |

## 5. A confirmar com o dono de `especialistas`

- se a view é **de produção** (SLA, atraso do streaming, quem mantém);
- de onde tirar CPF, CNPJ, regional, sexo e telefone — candidatos: `idMDM` / `Prontuario` contra a gold de pacientes;
- se vale manter o casamento por CPF e nome ou só pelo AN.
