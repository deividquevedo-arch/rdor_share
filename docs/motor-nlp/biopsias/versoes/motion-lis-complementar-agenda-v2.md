---
tags: [biopsias, motion-lis, linha-de-cuidado]
status: versao-anterior
---

# Motion LIS — documento complementar para a agenda de entendimento

> v2 · 30/10/2026 (v1 mantida; acrescenta a seção 2.1) · complementa o [[estudo-motion-lis-biopsias-v2|estudo]] e o relatório preliminar
> (`relatorio-preliminar-motion-lis-biopsias-v1.docx`). Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e
> mapeamento dos dados do Motion LIS disponíveis no Repositório Clínico*.
> Medições de 29–30/10/2026, só agregadas; nenhum laudo ou dado de paciente lido.

## 1. O que a agenda precisa decidir

O estudo respondeu **se** dá para usar o dado. A agenda decide **como** usar: qual regra, onde roda, para onde vai e
em que ordem. As perguntas estão na seção 7.

## 2. Como o dado chega

```
laboratório de anatomia patológica (Motion LIS)
        │  integração (status final)
        ▼
especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame     ← resultado, um registro por campo do laudo
        │  cruzamento com o pedido do hospital
        ▼
gold_corporativo.observation.tb_gold_mov_observation_exames             ← pedido (TASY, WPD, MV Soul) com o resultado
        │  num_pedido_integracao
        ▼
gold_corporativo_ia.corporativo.tb_gold_mov_exame                       ← base que a captação já lê
```

## 2.1 Unidade do exame

**A tabela do Motion não identifica a unidade.** As colunas de origem são fixas (`integracao` = "MOTION" em 100% dos
477 mil pedidos; `base_origem` = "ANATOMIA"), o número do pedido tem 12 caracteres sem código de unidade e
`atendimento_id` é um identificador por pedido.

A unidade vem do **cruzamento**:

| cruzar com | chave | campos de unidade | casam |
|---|---|---|---|
| tabela de pedidos (`gold_corporativo.observation.tb_gold_mov_observation_exames`) | número do pedido + item de integração | `extension.descricao_corporativa_unidade`, `extension.codigo_corporativo_unidade` | ~95% (jun–set/2026) |
| gold de exames da captação (`gold_corporativo_ia.corporativo.tb_gold_mov_exame`) | `num_pedido_integracao` | `id_unidade`, `emp_descricao_unidade` | ~90% |

O pedido sem par fica **sem unidade** — o que importa para o escopo de envio por unidade da navegação.

## 3. Cobertura e ritmo

| | |
|---|---|
| unidades com anatomopatológico (abr–out/2026) | **69**, em SP, RJ, DF, BA e Nordeste; sistemas TASY, WPD e MV Soul |
| maiores volumes | São Luiz Itaim, Anália Franco, Brasil Santo André, São Luiz Morumbi, Campinas — 9 a 15 mil pedidos cada no período |
| itens de resultado por mês | 38–43 mil (abr–out/2026) |
| **coleta → laudo liberado** | mediana **5 dias**, 90% em até **7 dias** |
| registro → dado disponível na tabela | mediana ~4 dias, 90% em até ~6 dias (ago–out/2026) |

Para a captação, o caso de biópsia fica disponível **cerca de uma semana depois da coleta**.

## 4. Ligação com a base da captação

| mês | itens | sem par na tabela de pedidos |
|---|---|---|
| out–dez/2025 | 19–35 mil | 10–11% |
| fev–mar/2026 | 32–40 mil | 8–9% |
| **jun–set/2026** | 40–43 mil | **4–5%** |
| out/2026 (mês aberto) | 42 mil | 8,5% |

A fração sem par **caiu ao longo do ano**; o valor de outubro reflete o mês ainda em processamento, não perda.

## 5. Volume mensal por órgão (média abr–set/2026, anatomopatológico)

| órgão | neoplasia maligna primária | pré-neoplásica ou in situ | metastática | exames |
|---|---|---|---|---|
| próstata | **255** | 0 | 0 | 508 |
| mama | **146** | 23 | 4 | 1.217 |
| útero / colo | **136** | 77 | 15 | 3.455 |
| pele | 133 | 44 | 4 | 918 |
| cólon / reto | **115** | **319** | 8 | 3.161 |
| estômago | 72 | **539** | 4 | 15.443 |
| tireoide | 62 | 0 | 0 | 115 |
| bexiga · rim · pulmão | 51 · 51 · 38 | — | 3 · 2 · 9 | 160 · 118 · 164 |
| outros / ainda não classificado | 538 | 191 | 95 | 11.814 |
| **total** | **~1.600** | **~1.190** | **~145** | **~37 mil** |

Leitura rápida: em **próstata, mama, útero e cólon** o volume de câncer primário é alto e o órgão é bem definido;
em **estômago e cólon** o grupo pré-neoplásico é maior que o de câncer — relevante para rastreio.

## 6. Opções de implementação

| opção | o que faz | precisa de | entrega |
|---|---|---|---|
| **A. regra estruturada** | `codificação da doença` + órgão pelo texto do material | lista de termos completa e validada; dicionário das categorias | malignos e pré-neoplásicos por órgão, **sem NLP**, desde abr/2026 |
| **B. A + leitura da conclusão** | NLP leve na conclusão para pedido multi-parte e para o histórico antes de mar/2026 | régua por órgão na conclusão | resolve multi-parte (≤ 1.164 casos em 7 meses) e o retroativo |
| **C. integração com as linhas de captação** | cruzar o resultado da biópsia com o achado de imagem/endoscopia das linhas existentes (estômago, cólon, colo do útero, próstata) | ligação por paciente; regra por linha | confirmação de diagnóstico e priorização na navegação |

**Recomendação técnica:** começar por **A** — é estruturado, auditável e cobre o fluxo corrente; **B** só onde A
deixar lacuna medida; **C** como evolução por linha.

## 7. Perguntas para a agenda

**Negócio / linhas de cuidado**
1. Quais desfechos entram: só neoplasia maligna primária, ou também pré-neoplásica, in situ e metastática?
2. Quais órgãos primeiro? (por volume: próstata, mama, útero/colo, cólon)
3. Precisa de retroativo antes de mar/2026 (exige leitura do texto — opção B)?
4. O caso de biópsia vira lista própria ou complementa as linhas que já existem (opção C)?

**Plataforma**
5. Onde roda: nova fonte no runner da plataforma NLP ou job próprio sobre as tabelas do Motion?
6. Entrega pela mesma saída das linhas (view + envio) ou formato próprio para a Tábia?
7. Identidade de serviço com leitura em `especialistas.gold` e na tabela FHIR em hml e prd.

**Dados e governança**
8. Quem é o dono das tabelas do Motion no catálogo `especialistas` — SLA e atraso garantidos?
9. Dicionário oficial das 552 categorias de `codificação da doença` (área do Motion LIS).
10. Contrato de entrada da Tábia: campos, frequência, identificação do paciente.
11. Laudo anatomopatológico é dado sensível: base legal e escopo de uso para captação.

## 8. Riscos

- **Órgão por texto:** sem validação por amostra, a precisão da lista de termos é desconhecida.
- **Campo de local anatômico vazio:** `bodySite` da tabela de pedidos vem em branco — não há alternativa codificada.
- **Categoria nova:** a `codificação da doença` só existe desde mar/2026; mudança de dicionário altera a regra.
- **Dependência de integração:** o caso só existe para a captação se o pedido casar com a gold (95% hoje).
