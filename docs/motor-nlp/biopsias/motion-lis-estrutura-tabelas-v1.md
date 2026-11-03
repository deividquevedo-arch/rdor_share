---
tags: [biopsias, motion-lis, linha-de-cuidado]
---

# Motion LIS — estrutura original das tabelas

> v1 · 30/10/2026 · anexo técnico do [[estudo-motion-lis-biopsias-v2|estudo]] e do documento da agenda.
> Fonte: `DESCRIBE` das tabelas (nomes e tipos, sem dados). ⭐ = coluna usada no estudo.
> Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados do Motion LIS disponíveis no Repositório Clínico*.

## 1. `especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame` — resultado do Motion LIS

49 colunas · particionada por `origem` · **uma linha por campo do laudo** · 3,26 mi linhas, 474 mil pedidos (13/10/2025 a 27/10/2026).

| grupo | coluna | tipo | conteúdo |
|---|---|---|---|
| identificação | `unique_id` · `id` · `id_res_exame` · `id_mv` | string | identificadores do registro e do resultado |
| | `atendimento_id` | string | identificador do atendimento (um por pedido) |
| | `status` | string | status do exame — sempre `final` |
| | `issued` | string | data e hora de emissão |
| ⭐ campo do laudo | `component_code_text` | string | **nome do campo** — `codificação da doença`, `material biópsia`, `conclusão genérico biópsia`… |
| | `component_code_coding_display` · `_code` · `_system` · `_position` · `_userSelected` | string / int | codificação do nome do campo |
| | ⭐ `component_value` | string | **conteúdo do campo** |
| interpretação | `component_interpretation_text` · `_coding_code` · `_coding_display` · `_system` · `_userSelected` | string | interpretação do componente |
| referência | `component_referenceRange_low` · `_high` · `_text` · `_age_low` · `_age_high` · `_type_*` (5) | string | faixas de referência |
| nota | `note_time` · `note_text` | string | observações livres |
| ⭐ integração | `numero_pedido_integracao` | string | **nº do pedido** (12 caracteres) — chave com a gold de exames |
| | ⭐ `numero_sequencia_item_pedido_integracao` | bigint | **item do pedido** — chave com a tabela FHIR |
| | `numero_atendimento_integracao` · `codigo_paciente_integracao` | string | atendimento e paciente no sistema de origem |
| | ⭐ `descricao_procedimento_integracao` | string | procedimento (ex.: `exame anatomopatológico`) |
| | `codigo_procedimento_integracao` · `modalidade_procedimento_integracao` | string | código e modalidade — sempre `PAT` |
| | `codigo_profissional_liberou_integracao` · `nome_profissional_liberou_integracao` | string | profissional que liberou |
| | `integracao` · `tipo_integracao` | string | sempre `MOTION` · `ANATOMIA` |
| origem | `base_origem` · `sistema_origem` · `origem` · `sistema` · `data_filtro` | string | sempre `ANATOMIA` · `MOTION (ORACLE)` · `motionlis` |
| carga | `file_modification_time` · `file_data` | timestamp · date | chegada do dado |

**Não há coluna de unidade.**

### Campos do laudo (`component_code_text`) mais frequentes

| campo | pedidos (12 meses) | natureza |
|---|---|---|
| `informações clínicas/hipótese diagnóstica` | 379.471 | texto livre do pedido |
| `codificação da doença` | 292.740 | **categoria** (552 valores) |
| `conclusão genérico biópsia` · `peça` | 223.358 · 218.621 | texto livre — diagnóstico |
| `microscopia biópsia` · `peça` | 223.330 · 218.578 | texto livre |
| `macroscopia - biópsia` · `peça` | 223.166 · 218.388 | texto livre |
| `material biópsia` · `peça` | 223.152 · 218.366 | texto curto — local da amostra |
| `tipo de exame citopatológico` · `conclusão citologia` | ~32 mil | categoria · texto |
| `conclusão ihq` · `resultado ihq` · `técnica ihq` | ~32.900 | texto (imuno-histoquímica) |
| `hpv - genotipo 16/18/31/…` | 12.055 | categoria (negativo / positivo / inconclusivo) |

## 2. `gold_corporativo.observation.tb_gold_mov_observation_exames` — pedido do hospital (FHIR)

25 colunas · particionada por `sistema` (`tasy`, `wpd`, `mv_soul`, `mv`, `smart`) · anatomopatológico: `extension.tipo_integracao = 'ANATOMIA'`.

| coluna | tipo | conteúdo |
|---|---|---|
| `_id` · `resourceType` · `version` · `identifier` · `hash_linha` | string / struct | identificação e versão |
| `basedOn` · `partOf` · `status` · `issued` | string | vínculos, status, emissão |
| `category` · `code` · `method` | struct (coding + text) | categoria, exame, método |
| `subject` · `encounter` · `performer` | struct (reference + display) | paciente, atendimento, executor |
| `note` · `specimen` | struct / string | notas · amostra (93% preenchido, conteúdo não avaliado) |
| `bodySite` | struct (coding + text) | local anatômico — ⚠️ **vazio em 100%** no anatomopatológico |
| ⭐ `component` | array de struct | campos do resultado, mesmos nomes do Motion |
| ⭐ `extension` | struct, 76 campos | metadados do pedido |
| `origem` · `sistema` · `file_modification_time` · `file_data` | string / timestamp / date | origem e carga |

### Campos de `extension`

| grupo | campos |
|---|---|
| ⭐ unidade | `descricao_corporativa_unidade` · `codigo_corporativo_unidade` · `unidade_origem` · `codigo_local_unidade` · `prefixo_local_unidade` · `codigo_unidade_integracao` · `base_origem` · `sistema_origem` |
| paciente | `codigo_corporativo_paciente` · `nome_corporativo_paciente` · `codigo_local_prontuario` · `codigo_local_paciente` · `nome_local_paciente` · `codigo_paciente_integracao` |
| atendimento | `numero_local_atendimento` · `numero_atendimento_integracao` |
| datas | `data_local_entrada` · `data_local_prescricao` · `data_local_pedido` · ⭐ `data_local_coleta` · `data_local_triagem` · `data_local_digitacao_laudo` · `data_local_assinatura_laudo` · ⭐ `data_local_liberacao_laudo` · `data_local_previsao_laudo` · `data_local_entrega_laudo` · `data_local_alta` |
| profissionais | responsável · solicitante · digitou o laudo · assinante · liberou — código e nome, local e corporativo; usuário que digitou |
| prescrição e pedido local | `numero_local_prescricao` · `numero_local_item_prescricao` · `numero_local_sequencia_item_prescricao` · `numero_local_pedido` · `numero_local_item_pedido` · `numero_local_sequencia_item_pedido` · `numero_local_laudo` · `codigo_local_procedimento` · `tipo_local_procedimento` · `codigo_local_modalidade` · `codigo_local_re` |
| ⭐ integração | `integracao` · `tipo_integracao` · `codigo_servico_solicitante_integracao` · `numero_pedido_integracao` · `numero_sequencia_item_pedido_integracao` · `codigo_procedimento_integracao` · `codigo_modalidade_integracao` · `descricao_procedimento_integracao` · `codigo_profissional_liberou_integracao` · `nome_profissional_liberou_integracao` |
| controle | `data_filtro` · `data_carga_mongo` · `data_atualizacao_mongo` · `tipo_observation_fhir` |

## 3. `gold_corporativo_ia.corporativo.tb_gold_mov_exame` — base da captação (colunas usadas)

| coluna | conteúdo |
|---|---|
| ⭐ `num_pedido_integracao` | chave com o Motion (~90% casam) |
| ⭐ `id_encounter` · `id_patient` | atendimento e paciente — ligação com o procedimento de origem |
| ⭐ `id_unidade` · `emp_descricao_unidade` | unidade |
| `proced_descricao` · `dt_dia_exame` · `proced_lista_exames` | descrição, data e laudos do exame |

## 4. Chaves entre as tabelas

| de → para | chave | casam |
|---|---|---|
| Motion → FHIR | `numero_pedido_integracao` + `numero_sequencia_item_pedido_integracao` ↔ `extension.numero_pedido_integracao` + `extension.numero_sequencia_item_pedido_integracao` | ~95% (jun–set/2026) |
| Motion → gold de exames | `numero_pedido_integracao` ↔ `num_pedido_integracao` | ~90% |
| AP → procedimento de origem | `id_encounter` + `id_patient` (na gold de exames) | ~36% dos AP |
