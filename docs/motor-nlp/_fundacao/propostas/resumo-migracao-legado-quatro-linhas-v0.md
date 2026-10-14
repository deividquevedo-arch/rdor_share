---
tags: [migracao, legado, resumo]
---

# Resumo — o que cada linha precisa além da config

> 14/10/2026. Versão curta do [[mapa-migracao-legado-quatro-linhas-v0|mapa de migração]]; evidência
> em `_processo/medicoes/auditoria-legado-quatro-linhas-2026-10-14.md`.
> Card do plano: `303791` — *Plano de Migração algoritmos final*.

## Mama (BI-RADS)

1. **Adaptador de envio para a Mule** (Portal de Acolhimento) — GET de disparo e espera do log de
   confirmação por até 4 h. A plataforma só faz POST na Central.
2. **View com a categoria de todo exame** (−1 a 6), não só os relevantes.
3. **Fonte PACS** como segunda entrada — ou decisão de deixá-la fora, depois de medir quanto acrescenta.
4. **Janela móvel de 30 dias** com reprocessamento, se o contrato da Mule exigir.
5. **Schema `birads`** nos três catálogos.

## Nódulo pulmonar

1. **Lib nova:** atributos por nódulo (tamanho, localização, caracterização, risco), evolução
   contra o exame anterior e conduta.
2. **Saída em lista de lesões**, no lugar de `fl_relevante` + `findings`.
3. **Idade do cadastro** como entrada do motor.
4. **Adaptador de envio para o Salesforce**, com de-para de unidades.
5. **Os 14 dicionários CSV** de `/mnt` para Volume.
6. **Schema `pulmao`**.

## Nefro

1. **Lugar para rodar fora do runner de NLP** — é cálculo de clearance sobre creatinina, sem texto;
   nenhum dos seis arquivos de linha serve.
2. **Entrada de laboratório** (TUSS 40301630) com sexo e idade do cadastro.
3. **Descobrir quem consome a saída** e para onde vai — não há código de envio.
4. **Validação clínica da fórmula** — falta o termo `α` do CKD-EPI; menores de 18 anos recebem a
   fórmula adulta.
5. **Schema `nefrologia`**.

## Hepato

1. **Captura do retorno** — as planilhas preenchidas pelos médicos voltam para uma tabela no schema
   `hepatologia`.
2. **Navegação por fluxo** a partir do retorno, em duas listas: transplante; oncologia + baixa
   complexidade.
3. **Migração do histórico** de 5.836 retornos.
4. **URL do Logic App** com assinatura SAS: sai do código, vai para secret scope, e a assinatura é
   rotacionada.

## Comum às quatro

- **Acesso ao orquestrador** do `workspace-analytics-bi-hml-02` — sem ele o legado não é desligado.
- **Decisão de destino** por linha: manter o sistema atual ou convergir para a Central.
