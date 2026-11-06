---
tags: [biopsias, motion-lis, apresentacao]
---

# O que o Motion LIS tem — campos disponíveis

> Apresentação para a navegação · 05/11/2026 · tabela `especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame`
> no Repositório Clínico. Detalhe técnico em [[motion-lis-estrutura-tabelas-v1]].

## 1. Como a tabela é organizada

Cada **pedido** de exame vira **várias linhas** — **uma por campo do laudo**.

| nº do pedido | campo do laudo | conteúdo |
|---|---|---|
| 1234… | material biópsia | "pólipo de cólon sigmoide" |
| 1234… | macroscopia - biópsia | "fragmento pardacento medindo…" |
| 1234… | conclusão genérico biópsia | "adenocarcinoma moderadamente diferenciado…" |
| 1234… | codificação da doença | "neoplasia maligna primária" |

*(exemplo ilustrativo, não é laudo real)*

## 2. Tipos de exame

| procedimento | pedidos (abr–out/26) | o que é |
|---|---|---|
| **exame anatomopatológico** | 267 mil | biópsia e peça cirúrgica — **base da lista de cólon** |
| **exame citopatológico** | 15 mil | punção (ex.: PAAF de tireoide) e citologia |
| HPV — genotipagem | 5 mil | teste de HPV |
| congelação (per-operatório) | 2,6 mil | exame durante a cirurgia |
| imuno-histoquímica, revisão e outros | < 1 mil cada | complementares |

## 3. Campos do laudo — anatomopatológico

O laudo vem em **dois modelos**, com os mesmos campos: **biópsia** e **peça**.
⚠️ O modelo "peça" **não quer dizer cirurgia** — pólipo retirado na colonoscopia também usa esse modelo.

| campo | o que traz | tipo |
|---|---|---|
| **codificação da doença** | categoria do resultado (neoplasia maligna primária, benigna, metastática…) | **categoria fixa** (100% preenchida desde abr/26) |
| **material** biópsia / peça | de onde veio a amostra | texto curto |
| **macroscopia** biópsia / peça | descrição a olho nu | texto livre |
| **microscopia** biópsia / peça | descrição ao microscópio | texto livre |
| **conclusão** biópsia / peça | o diagnóstico | texto livre |
| informações clínicas / hipótese diagnóstica | o que o médico pediu | texto livre |
| imuno-histoquímica (material, técnica, resultado, conclusão) | quando houve IHQ | texto |

## 4. Campos do laudo — citopatológico (punção)

| campo | o que traz |
|---|---|
| tipo de exame citopatológico | ex.: punção de tireoide |
| macroscopia citologia | descrição da amostra |
| **conclusão citologia** | o diagnóstico — na tireoide, a **categoria Bethesda** escrita no texto |

⚠️ Aqui **não existe** a categoria "neoplasia maligna primária"; o resultado precisa ser lido no texto.

## 5. Campos de identificação

| campo | o que traz |
|---|---|
| **nº do pedido** | chave para abrir o caso no Motion e para cruzar com o RC |
| nº do atendimento | atendimento no sistema de origem |
| código do paciente | código no sistema de origem (não é CPF) |
| procedimento | anatomopatológico, citopatológico… |
| profissional que liberou | patologista |
| data de emissão | quando o laudo foi liberado |

## 6. O que o Motion **não** tem

| falta | de onde vem |
|---|---|
| nome, CPF, idade, telefone | Repositório Clínico, pelo nº do pedido |
| unidade (hospital) | Repositório Clínico, pelo nº do pedido |
| **órgão** como campo | lido no texto do **material** |
| se é **peça cirúrgica** | lido no texto do **material** |

## 7. Atualização

- **Uma carga por dia**, por volta das **05:30**, com os laudos liberados até o dia anterior.
- O laudo do AP sai, em média, **~5 dias** depois da coleta.
