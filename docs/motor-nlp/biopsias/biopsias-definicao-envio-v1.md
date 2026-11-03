---
tags: [biopsias, motion-lis, linha-de-cuidado]
---

# Biópsias (Motion LIS) — definição do envio

> v1 · 30/10/2026 · registro da definição de negócio após a agenda de entendimento de 30/10.
> Base técnica: [[estudo-motion-lis-biopsias-v2|estudo]], [[motion-lis-complementar-agenda-v4|documento da agenda]] e
> [[motion-lis-estrutura-tabelas-v1|estrutura das tabelas]]. Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e
> mapeamento dos dados do Motion LIS disponíveis no Repositório Clínico*.

## 1. A definição

| | |
|---|---|
| padrão | o mesmo das outras linhas de captação: envio do caso **com o laudo** |
| relevante | **sempre e somente** os casos com categoria **neoplasia (maligna) primária** |
| coluna de achado | no lugar do "Achado" da IA, vai a **categoria do AP** (`codificação da doença`) |
| laudo | vai o **laudo da biópsia**, como nas outras linhas |

**Consequência técnica:** a relevância vem de um **campo estruturado do laboratório**, não de leitura de texto — não há
régua de NLP nem juiz nesta linha. É regra determinística sobre a categoria.

## 2. Tamanho (média abr–set/2026)

| | por mês |
|---|---|
| pedidos com neoplasia maligna primária | **~1.830** (todos os procedimentos de anatomia patológica) |
| desses, localizados na gold de exames (paciente e unidade) | ~1.690 (92%) |
| **pacientes distintos** | **~1.565** |
| em **unidades navegadas** (mesma lista de 103 ids das demais linhas) | **~1.390 pedidos · ~1.290 pacientes** |

Composição do laudo nesses casos: conclusão 100% · material 100% · microscopia 100% · macroscopia 98,9%.

## 3. O que muda em relação às outras linhas

| | linhas atuais | biópsias |
|---|---|---|
| fonte | gold de exames (texto do laudo de imagem/endoscopia) | tabela do Motion + gold de exames (paciente, unidade) |
| decisão | NLP (régua → semântica → juiz) | **regra sobre a categoria** — sem modelo |
| coluna de achado | achado identificado pela IA | **categoria do AP** |
| laudo | laudo do exame | laudo do AP, **montado a partir dos campos** (material, macroscopia, microscopia, conclusão) |
| unidade | da gold | da gold (cruzamento pelo nº do pedido) |
| tempo até o caso | ~D+1 do exame | ~1 semana após a coleta (laudo do AP sai em ~5 dias) |

## 4. A confirmar

1. **"Neoplasia primária" = só a categoria `neoplasia maligna primária`?** Ficam de fora, pela definição (pedidos/mês,
   abr–set/2026): `metastática` (~190), `suspeito e indefinido para neoplasia` (~130),
   `carcinoma in situ / displasia de alto grau` (~70) e `maligna indeterminada se primária ou metastática` (~70).
2. **"Laudo da biópsia" é o laudo do AP** (o resultado do laboratório) **ou o laudo do procedimento** que colheu a amostra
   (endoscopia, colonoscopia)? O do AP existe para todos; o do procedimento, para ~36%.
3. **Um envio por paciente ou por pedido?** ~1.690 pedidos para ~1.565 pacientes por mês — há paciente com mais de um AP
   maligno no mês. As outras linhas deduplicam por paciente + data + procedimento.
4. **Órgão:** vale enviar também o `material` (de onde veio a amostra), para a navegação saber a linha de cuidado?
5. **Escopo de unidades:** a mesma lista "Nacional Targa" das outras linhas (~1.290 pacientes/mês)?
6. **Destinatários e destino:** uma linha nova "Biópsias" na navegação, e a Tábia recebe pelo mesmo envio?

## 5. Implementação — proposta para a plataforma

O motor NLP não se aplica: a decisão é um filtro sobre a categoria. Proposta:

1. **visão de entrada:** pedidos de anatomia patológica do Motion, com categoria, material e laudo montado dos campos,
   ligados à gold pelo nº do pedido (paciente, unidade, dados cadastrais);
2. **regra:** `fl_relevante = 1` quando a categoria é neoplasia maligna primária;
3. **saída no mesmo contrato da view de exportação das linhas** (coluna de achado = categoria; laudo = laudo do AP),
   para reaproveitar o envio existente (OneDrive / API / Tábia) sem código novo de envio;
4. **validação antes do envio:** conferência da montagem do laudo numa amostra e dos volumes contra a seção 2.

Decisões de plataforma: onde a visão roda (job próprio ou fonte nova no runner) e como a linha entra no `data_exchange`.
