---
tags: [biopsias, motion-lis, industrializacao, plano, arquitetura]
---

# Biópsias — arquitetura e plano de industrialização

> v2 · 10/11/2026 · acrescenta à [[plano-industrializacao-biopsias-v1|v1]] o desenho da arquitetura e a avaliação do
> envio que a plataforma já tem (API da Central de Captação e OneDrive). Motor: [[proposta-motor-regras-estruturadas-v0]].
> Régua: consulta `consulta-lista-colon-biopsias-v3.sql`. Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e
> mapeamento dos dados do Motion LIS disponíveis no Repositório Clínico*.

## 1. Arquitetura

![[arquitetura-biopsias-v1.png]]

Editável: `arquitetura-biopsias-v1.drawio`.

| camada | o quê | situação |
|---|---|---|
| **fonte** | Motion LIS (resultado, requisição → hospital) e RC (paciente) | Motion entra pelo Datahub (`motionlis`) — **a confirmar** |
| **regra** | **motor de regras estruturadas**: 2º runner da plataforma, uma config por linha, vocabulário compartilhado, dedup por pedido, funil e explicação por linha | **novo** |
| **contrato** | view de exportação `vw_mod_diamond_<linha>_export_v<n>` | **existe** — o motor só precisa gravar a saída que ela consome |
| **envio** | API da Central de Captação, OneDrive (Excel por unidade), randomização, controle de carga | **existe** — reaproveitado sem mudança |
| **destino** | Central de Captação → Tábia (card do paciente) | **existe**; regras específicas de biópsia **a confirmar** |

## 2. O envio que já existe × o que a história pede

O envio por API (`plataform/tools/data_exchange/ntb_ia_data_api.py`, `InferenceIngestor`) lê a view da linha pelo nome
(`diamond_fabrica_ia{_env}.<linha>.vw_mod_diamond_<linha>_export_v<n>`, ou a `_intervencao` quando há randomização),
monta o payload validado e posta em lotes, com nova tentativa, em `/api/ingestao/inferencias` da **Central de
Captação**; os exames aceitos ficam no log de envio. **Não depende do motor NLP** — depende só de uma view no contrato.

### Campos do payload → origem na linha de biópsias

| campo do payload | origem | situação |
|---|---|---|
| `idInferencia` | nº do pedido no Motion | ✅ estável entre dias |
| `idHospital`, `nomeHospital`, `ufRegional` | RC; para pedido fora do RC, hospital do Motion | ✅ |
| `nomePaciente`, `cpfPaciente`, `sexoPaciente`, `idadePaciente`, `dataNascimentoPaciente`, `telefonePaciente` | RC (`tb_gold_mov_paciente`) | ✅ — ⚠️ ~7% dos pedidos fora do RC exigem decisão sobre o cadastro do Motion |
| `convenio`, `plano` | RC | ✅ (mesma ressalva) |
| `exameAn` | exame do RC ou nº do pedido | ✅ |
| `laudoExameDataLiberacao` | emissão do AP | ✅ |
| `origemExame`, `modalidade`, `tipoExame` | RC / `PAT` / procedimento do Motion | ✅ |
| `laudoExameOriginal` | laudo do AP montado (material, macroscopia, microscopia, conclusão) | ✅ |
| `medicoSolicitante`, `crmSolicitante` | RC | ✅ |
| `medicoLaudo`, `crmLaudo` | profissional que liberou no Motion | ✅ nome · ⚠️ CRM a verificar |
| `saidaModelo.achados` | categoria do AP | ✅ |
| `saidaModelo.observacoes` | material e classificação da amostra | ✅ ajuda a enfermagem |
| `flgRelevante` | 1 | ✅ |
| `idLinhaNavegacao`, `nomeLinha` | config de navegação da linha | ✅ **mesma linha do Ca de Cólon** — a Tábia tem campo de **origem** (algoritmo NLP × Motion) |
| origem do caso (NLP × Motion) | ? — o payload atual não tem campo explícito de origem | ⚠️ **confirmar como a Central preenche**: pelo `projeto` (linha de biópsias com `projeto` próprio) ou por campo novo no payload |

### Requisitos da história

| requisito | o que existe | situação |
|---|---|---|
| envio diário no padrão das linhas | job + view + API/OneDrive | ✅ |
| laudo do AP junto, categoria no lugar do achado | payload já tem `laudoExameOriginal` e `achados` | ✅ |
| randomização | `_intervencao` + envio fail-closed | ✅ (se decidida) |
| não reenviar laudo reemitido | a view filtra só o dia de execução | ✅ se o **motor** deduplicar por pedido entre dias · ⚠️ confirmar se a Central é idempotente por `idInferencia` |
| **paciente já em navegação → apenas anotar** | a API só **insere** inferência; não há operação de anotação | ❌ **lacuna** — regra da Central/Tábia (deduplicar por paciente + linha e anexar) ou campo novo no payload |
| campos de destino na Tábia | lista pendente | ⚠️ |

## 3. Plano de implementação e industrialização

| fase | entrega | quem | depende de |
|---|---|---|---|
| **0 · alinhamento** | arquitetura (motor como 2º runner), contrato, linha de navegação, regra de "já navegado"; chamados (schema, grants, identidade de serviço) | DS + plataforma + Datahub + negócio | — |
| **1 · SPEC** | SPEC v0 do motor (cólon + tireoide) e da linha: config, saída no contrato da view, dedup, janela, mapeamento do payload (§2) | DS | fase 0 |
| **2 · motor (dev)** | runner mínimo com **paridade 100%** com a consulta v3; funil e explicação gravados | DS + plataforma | SPEC · fonte do Motion |
| **3 · exposição (dev → hml)** | view da linha, navegação nos 3 ambientes, job `PAUSED`, `disabled` em hml, randomização se decidida | DS (config/navegação) + plataforma | fase 2 |
| **4 · validação** | paridade com a v3 · histórico da navegação (Dash AP) em período longo · envio de teste em hml até a Central | DS + navegação | dados da navegação |
| **5 · produção** | promoção `hml → main`, job ativo, painel (volume, % sem RC, % indefinido, duplicados, funil) | MLOps + DS | aprovação da fase 4 |
| **6 · Tábia** | origem Motion no destino (campo da Tábia), campos, regra de "já navegado" | plataforma (Central) + negócio | fase 0 |
| **7 · próximas linhas** | estômago e próstata **só por config**; tireoide pela extração do Bethesda | DS | fase 5 |

**Caminho crítico:** fase 0 (decisões) → fonte do Motion e identidade de serviço (chamados) → motor (fase 2). A fase 6
corre em paralelo e define se o "já navegado" se resolve na Central ou na linha.

## 4. Decisões para a fase 0

1. Motor de regras estruturadas como **2º runner da plataforma**.
2. ~~Linha própria ou a mesma~~ → **mesma linha do Ca de Cólon**, distinguida pelo campo de origem da Tábia (NLP × Motion). Falta: **qual campo do payload alimenta a origem** (`projeto` ou campo novo).
3. **"Já navegado = anotar"**: regra na Central/Tábia ou campo novo no payload.
4. Central de Captação **idempotente por `idInferencia`**?
5. Paciente só do RC (perde ~7%) ou também do Motion (decisão de LGPD).
6. Randomização nesta linha.
7. Fonte do Motion pelo Datahub (ingestão) ou exceção registrada.
