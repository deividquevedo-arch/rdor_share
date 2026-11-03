---
tags: [biopsias, motion-lis, linha-de-cuidado]
---

# Estudo — dados do Motion LIS no Repositório Clínico (biópsias e anatomopatológicos)

> **v2 · 29/10/2026 · estudo fechado** (v1 de 28/10 mantida) · card `393825` — *[Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados do Motion LIS
> disponíveis no Repositório Clínico*.
> Método: só `DESCRIBE` e consultas agregadas; valores listados apenas para campos de domínio curto com ≥ 50
> ocorrências. Nenhum texto de laudo, nome ou identificador de paciente foi lido.

## Resumo

| pergunta do card | resposta |
|---|---|
| **CA01** campos mapeados | sim — seção 1 |
| **CA02** órgão da amostra | **sim, por texto curto**: `material biópsia` / `material peça`. Sem código estruturado; `bodySite` da tabela FHIR vem vazio |
| **CA03** resultado | **sim**: `conclusão genérico biópsia/peça` (texto livre) e `codificação da doença` (categoria estruturada) |
| **CA04** "neoplasia primária" | **sim, estruturado**: valor `neoplasia maligna primária` do campo `codificação da doença` — 14.848 pedidos; campo 100% preenchido desde abr/2026 |
| **CA05** relação entre as fontes | **sim**: `numero_pedido_integracao` + `numero_sequencia_item_pedido_integracao` — 92,4% dos pedidos casam; 89,6% estão na gold de exames da captação |
| **CA06** evidência | este documento: tabela, campo, conteúdo e origem de cada informação; exemplos só de campos de domínio curto |

## Conclusão

**Os dados do Motion LIS permitem identificar, para cada anatomopatológico, o órgão da amostra e se o resultado é
neoplasia maligna primária, e ligar o exame à gold que a captação já usa.** O diagnóstico vem **estruturado** (sem
NLP) desde abr/2026; o órgão vem de **texto curto** e depende de um vocabulário, hoje com 78% de cobertura.

| critério | status |
|---|---|
| CA01 · CA03 · CA04 · CA05 · CA06 | ✅ atendidos |
| CA02 | ✅ atendido, com ressalva: órgão por vocabulário, cobertura 78%, **sem validação por amostra** |

## 1. As duas tabelas

### `especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame` — o resultado

- **3.255.769 linhas · 473.780 pedidos · 13/10/2025 a 27/10/2026.** Uma linha por **componente** do resultado.
- 100% `modalidade_procedimento_integracao = 'PAT'`, `tipo_integracao = 'ANATOMIA'`, `status = 'final'`,
  origem `MOTION (ORACLE)`.
- Chaves: `numero_pedido_integracao`, `numero_sequencia_item_pedido_integracao`, `numero_atendimento_integracao`,
  `codigo_paciente_integracao`, `atendimento_id`, `id_res_exame`.
- O nome do campo do laudo está em `component_code_text` (ou `component_code_coding_display`); o conteúdo, em
  `component_value`.

**Procedimentos** (`descricao_procedimento_integracao`, pedidos em 12 meses): exame anatomopatológico **434.939** ·
citopatológico 23.051 · HPV genotipagem 6.701 · per-operatório (congelação) 4.326 · imuno-histoquímica 1.098 ·
imunofenotipagem 766 · painéis genéticos e moleculares (dezenas a centenas cada).

**Campos do resultado relevantes** (pedidos · tamanho médio do valor):

| campo (`component_code_text`) | pedidos | tam. médio | natureza |
|---|---|---|---|
| `codificação da doença` | 292.740 | 25 | **categoria estruturada**, 552 valores distintos |
| `material biópsia` / `material peça` | 223.152 / 218.366 | 58 / 53 | texto curto — **local da amostra** |
| `conclusão genérico biópsia` / `peça` | 223.358 / 218.621 | 417 / 380 | texto livre — **diagnóstico** |
| `macroscopia` / `microscopia` (biópsia, peça) | ~220 mil cada | 200–1.050 | texto livre descritivo |
| `informações clínicas/hipótese diagnóstica` | 379.471 | 47 | texto livre do pedido |
| `tipo de exame citopatológico` | 31.979 | 36 | categoria (cérvico-vaginal 20.489 · tireoide 5.577 · mama 1.892 · …) |
| `conclusão citologia` | 31.997 | 514 | texto livre |
| `hpv - genotipo 16/18/31/…` | 12.055 | 8 | categoria: negativo / positivo / inconclusivo |
| `conclusão ihq`, `resultado ihq` | ~32.900 | 350–370 | texto livre |

### `gold_corporativo.observation.tb_gold_mov_observation_exames` — o pedido (FHIR / MDM)

- Não há linhas com origem Motion: as linhas são os pedidos dos sistemas hospitalares (TASY, WPD, MV, MV Soul).
  Os de anatomia têm `extension.tipo_integracao = 'ANATOMIA'` — **462.312 linhas** desde 13/10/2025.
- Nessas linhas, `component` vem preenchido em 100% (média 6,5 componentes) e 59,8% trazem `codificação da doença`:
  **a tabela já incorpora o resultado do Motion LIS**.
- `extension` traz unidade, paciente corporativo, datas de coleta e liberação, profissionais e as mesmas chaves de
  integração da tabela do Motion.
- 🔴 **`bodySite` vem com string vazia em 100% das linhas de anatomia** — preenchido, não utilizável. `specimen`
  aparece em 92,9% (conteúdo não avaliado).

## 2. Órgão da amostra (CA02)

Não há órgão codificado. O local vem no texto curto de `material biópsia` / `material peça` — ex.: `biópsia gástrica.`,
`a - biópsia de antro gástrico; b - biópsia de corpo gástrico.`, `pólipo de reto.`, `biópsia de endométrio.`,
`vesícula biliar.`, `fragmentos de próstata.` (70–110 mil variações escritas).

Um vocabulário inicial de 20 grupos por expressão regular classifica **78%** dos 434 mil anatomopatológicos
(pedidos em 12 meses):

| órgão | pedidos | neoplasia maligna primária | pré-neoplásica ou in situ |
|---|---|---|---|
| estômago | 179.530 | 571 | 4.260 |
| útero / colo | 40.485 | 1.089 | 612 |
| cólon / reto / íleo | 38.050 | 901 | 2.578 |
| vesícula | 22.761 | 35 | 16 |
| mama | 14.247 | 1.207 | 181 |
| pele | 10.492 | 1.046 | 338 |
| próstata | 6.134 | 2.001 | 3 |
| tireoide · rim · bexiga · pulmão | 1.427 · 1.370 · 1.893 · 1.975 | 486 · 399 · 409 · 347 | — |
| **não classificado** | **97.109** | 3.496 | 1.400 |

⚠️ As contagens de neoplasia por órgão incluem o período anterior a abr/2026, quando o campo não era preenchido
(ver seção 3) — são **piso**. O vocabulário é primeira passada; o grupo não classificado é o trabalho seguinte.

## 3. Resultado e "neoplasia primária" (CA03, CA04)

`codificação da doença` é uma **categoria do laboratório**, não texto livre. Valores (pedidos em 12 meses):
processo inflamatório 142.322 · proliferativo não neoplásico 42.467 · normalidade 33.990 · neoplasia benigna 27.725 ·
**neoplasia maligna primária 14.848** · neoplasia benigna e hamartoma 11.595 · **lesão pré-neoplásica 10.511** ·
processo degenerativo 8.780 · **neoplasia maligna metastática 1.636** · suspeito e indefinido para neoplasia 1.000 ·
neoplasia de comportamento incerto 704 · **maligna indeterminada se primária ou metastática 582** ·
**carcinoma in situ / displasia de alto grau 537** · doença inflamatória intestinal 286 · endometriose 278 · …

🔴 **O campo nasceu em mar/2026:** preenchido em < 3% dos anatomopatológicos até fev/2026, 80,9% em março e
**100% de abr/2026 em diante** (~35–39 mil pedidos por mês). Para frente, todo anatomopatológico vem classificado.

No texto livre, "neoplasia primária" aparece em 226 pedidos e "neoplasia" em 226.561 — a categoria estruturada é a
fonte, não o texto.

## 4. Relação entre as fontes (CA05)

| ligação | chave | casamento |
|---|---|---|
| Motion → FHIR | `numero_pedido_integracao` + `numero_sequencia_item_pedido_integracao` (`extension.*` no FHIR) | **437.762 de 473.780 itens (92,4%)** |
| Motion → gold de exames da captação (`gold_corporativo_ia.corporativo.tb_gold_mov_exame`) | `numero_pedido_integracao` = `num_pedido_integracao` | **424.472 de 473.780 pedidos (89,6%)** |

Os 7,6% sem par no FHIR não foram caracterizados (unidade, período).

## 4.1 🔴 Pedido com várias amostras — a codificação é do PEDIDO, não da amostra

Medido de abr a out/2026 (258.519 anatomopatológicos): **122.102 (47%)** têm material com várias partes
(`a - …; b - …`), e **24.462** trazem mais de uma linha de codificação (15.451 com valores diferentes).
Dos **11.397** com neoplasia maligna primária, **5.707 são multi-parte** e **5.247** destes têm **uma única
codificação** para todas as partes.

Quando as partes são do mesmo órgão (antro e corpo gástricos), o órgão segue determinado. Quando são de órgãos
diferentes, **a categoria não diz qual parte é maligna** — a atribuição exige ler a `conclusão`, que costuma vir
separada por parte.

**Medido (29/10), partes classificadas pelo mesmo vocabulário da seção 2:**

| malignos multi-parte com uma codificação | pedidos |
|---|---|
| todas as partes no **mesmo órgão** | **4.083 (77,8%)** |
| partes em **órgãos diferentes** | **33 (0,6%)** |
| alguma parte **não classificada** pelo vocabulário | 1.131 (21,6%) |

A mistura de órgãos é **marginal**: o órgão é determinado para quase todo maligno multi-parte. O limite superior
(33 + 1.131) depende do vocabulário, não da estrutura do dado — refinar o vocabulário resolve as duas lacunas.

## 5. O que isto viabiliza e o que fica em aberto

- **Para as linhas de câncer:** diagnóstico maligno primário **estruturado**, com órgão por vocabulário, ligado ao
  exame da gold — **~1,6 mil por mês** (11.397 de abr a out/2026, com o campo 100% preenchido). Os grupos pré-neoplásicos (displasia, in situ) são candidatos
  a rastreio.
- **Para a linha nova de colo do útero:** citologia cérvico-vaginal (20.489 pedidos/ano, com conclusão) e HPV com
  genótipo estruturado.
- **Em aberto:**
  - vocabulário de órgão para os 22% não classificados e validação por amostra;
  - o que é `specimen` no FHIR;
  - os 7,6% sem par no FHIR;
  - contrato de saída para a Tábia — fora do escopo deste estudo;
  - conferência dos valores de `codificação da doença` com a PO do Motion LIS (dicionário oficial das 552 categorias).
