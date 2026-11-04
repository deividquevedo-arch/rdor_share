---
tags: [biopsias, motion-lis, linha-de-cuidado, cancer-colon]
---

# Biópsias (Motion LIS) — definição do envio

> v2 · 03/11/2026 · incorpora o refinamento de 03/11 (planilha *[IA] Refinamento das frentes 4 Trimestre*, aba
> *Integração Tabia*) e a medição do recorte de cólon. Substitui o escopo do [[biopsias-definicao-envio-v1|v1]], que
> fica como registro da definição geral de 30/10. Base técnica: [[estudo-motion-lis-biopsias-v2|estudo]],
> [[motion-lis-complementar-agenda-v4|documento da agenda]] e [[motion-lis-estrutura-tabelas-v1|estrutura das tabelas]].
> Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados do Motion LIS disponíveis no Repositório
> Clínico*.

## 1. O que mudou do v1

| | v1 (30/10) | v2 (03/11) |
|---|---|---|
| escopo | todos os órgãos | **só Ca de Cólon** — primeiro entregável |
| órgão | coluna informativa (pergunta aberta) | **filtro** pelo material do AP |
| exame | anatomopatológico, qualquer procedimento | **biópsia** (colonoscopia); peça cirúrgica a decidir |
| volume | ~1.290 pacientes/mês | **~42 a ~55 pacientes/mês** (seção 3) |

## 2. A definição

| | |
|---|---|
| fonte | tabelas do Motion LIS no Repositório Clínico |
| órgão relevante | cólon e subdivisões (sigmoide, ceco, ascendente, transverso, descendente, flexuras) |
| exame | biópsia em colonoscopia — anatomopatológico |
| relevante | categoria `neoplasia maligna primária` (`codificação da doença`) |
| coluna de achado | a categoria do AP |
| laudo | o laudo do AP |
| saída | os mesmos campos de navegação dos envios de laudo |

A decisão continua determinística sobre campo estruturado: sem régua de NLP e sem juiz.

## 3. Tamanho do recorte de cólon

Média mensal abr–out/2026, pedidos com `neoplasia maligna primária`, unidades navegadas = lista `e.id_unidade` das
configs de navegação de prd (a mesma em todas as linhas).

| recorte | pedidos/mês | em unidades navegadas | **pacientes/mês** |
|---|---|---|---|
| **cólon — biópsia** | 44,1 | 42,3 | **42,3** |
| reto — biópsia | 13,6 | 13,0 | 13,0 |
| cólon — peça cirúrgica (sem apêndice) | 53,9 | 49,9 | 47,9 |
| reto — peça cirúrgica | 38,3 | 34,6 | 33,4 |
| apêndice (fora de cólon) | 13,0 | 12,0 | 12,0 |
| *referência: todos os órgãos* | *1.866,7* | *1.705,9* | *1.565,6* |

**Leitura:** só cólon por biópsia dá **~42 pacientes/mês (~2/dia útil)**. Com reto, **~55/mês**. A peça cirúrgica
dobraria o volume, mas é, em regra, de paciente com diagnóstico já feito — entrada que a navegação provavelmente já
recebeu pela biópsia.

**Método e limites**
- Órgão classificado pelo texto dos campos de material (`material biópsia`, `material peça`, `material ihq`,
  `material ihq - complementar`, `material revisão`), preenchidos em 93% dos pedidos. Fronteira explícita antes de
  `reto` e `ceco`, para não casar "direto" ou "seco".
- Biópsia × peça vem do nome do campo de material. Biópsia de cólon é quase sempre colonoscópica, mas a origem não é
  verificável pedido a pedido: só 36% dos AP se ligam ao procedimento de origem pelo atendimento.
- A categoria é do **pedido**, não da amostra. Pedido com várias amostras (47%) e alguma de cólon conta como cólon,
  mesmo que a neoplasia seja de outra amostra. Isso é limite superior; a conferência de amostra na homologação mede o
  efeito.
- Controle: dos pedidos com material citando intestino e classificados como "outro órgão" (~20/mês), os mais
  frequentes são pólipo endometrial — não há perda visível de cólon.

## 4. Atualização das tabelas (pré-requisito do desenvolvimento)

| | medido (20/10 a 03/11) |
|---|---|
| frequência | **uma carga por dia**, entre 05:00 e 06:30 (`file_modification_time`) |
| volume típico | 1.400 a 2.000 pedidos/carga |
| exceções | 26/10 e 02/11 com 17 e 24 pedidos; 03/11 com 6.516 (recuperação) |
| atraso do laudo | AP liberado ~5 dias após a coleta |

🔴 **A tabela do Motion está em dia, mas paciente e unidade vêm da gold de exames**, parada desde 30/10. Sem a gold,
a lista sai sem unidade (não filtra por unidade navegada) e sem os campos cadastrais. Alternativa a avaliar: a tabela
FHIR de observações (pedido + item, ~95% de casamento), cuja atualização não foi verificada.

## 5. Os seis pontos do v1

| ponto | situação |
|---|---|
| 1. só `neoplasia maligna primária`? | ✅ confirmado |
| 2. laudo do AP ou do procedimento? | ✅ laudo do AP |
| 3. um envio por paciente ou por pedido? | ❌ aberto — ligado ao critério "novo exame de paciente já em navegação" |
| 4. órgão | ✅ vira filtro (cólon) — falta decidir **reto** e **peça cirúrgica** |
| 5. unidades | ❌ aberto — quais unidades a navegação acompanha pelo painel de AP |
| 6. destino e campos | ❌ aberto — lista de campos da Tabia e se aproveita o fluxo dos laudos |

## 6. Cronograma do refinamento

Responsáveis na planilha de refinamento.

| entrega | período |
|---|---|
| desenvolvimento — lista de pacientes relevantes | 09–13/11 |
| homologação da lista | 16–20/11 |
| industrialização (fluxo da Fábrica, prd) | 16–20/11 |
| integração com a Tabia, execução diária | 23–27/11 |

Pré-requisitos registrados: acesso de desenvolvedor reconfirmado para o time; **usuário de serviço sem acesso — exige
chamado** (bloqueia a industrialização); campos de destino na Tabia; regra para paciente já em navegação.

## 7. A decidir antes de 09/11

1. **Reto entra?** +13 pacientes/mês (cólon 42 → 55).
2. **Peça cirúrgica entra?** +48 (cólon) ou +81 (com reto) por mês, em regra de paciente já diagnosticado.
3. **Paciente já em navegação:** reenviar, ignorar ou anotar?
4. **Fonte de paciente e unidade** enquanto a gold estiver parada: esperar, ou usar a tabela FHIR.

## 8. Implementação — mantida do v1

Visão de entrada do Motion com categoria, material e laudo montado; filtro de cólon pelo material; `fl_relevante = 1`
para `neoplasia maligna primária`; saída no contrato da view de exportação das linhas, reaproveitando o envio
existente. Conferência da montagem do laudo e dos volumes da seção 3 antes do primeiro envio.
