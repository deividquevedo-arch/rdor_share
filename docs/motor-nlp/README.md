# Documentação — Motor NLP / Plataforma clínica

Índice central. Reorganizado em 2026-07-13 por **assunto** (antes tudo solto em `notas/`).

## Estrutura

| Pasta | Conteúdo |
|---|---|
| **`_fundacao/`** | Base do motor: discovery, análise de engines, visão, roadmap, anexos (modelo/arquitetura/histórias/decisões), diretrizes, contratos, specs (rule-engine, scoring), modelo de decisão, LLM router, governança/telemetria. |
| **`rads/`** | **Extração de categoria BI/PI/TI-RADS.** Arquitetura de config RADS, plano de implementação, regras clínicas. `rads/notas/`: checkpoints (birads/rads), decisões de expansão PI/TI, homologações BI/PI/TI-RADS, linha de evolução xxRADS. |
| **`tireoide/`** | **Linha de cuidado de tireoide (relevância V1).** Gold spot v13, mapa de gaps, relatório final V1, spec da camada quantitativa. Subpastas: `checkpoints/` (tirads/quantitativa), `homologacao/` (reconciliação, homologação de negócio, homologação manual), `dados/` (base ouro, revisões médicas, resíduos, CSVs/xlsx). |
| **`hepatologia/`** | Bancada, calibração, serving, validações e relatórios de homologação de hepatologia. |
| **`pulmao/`** | Piloto e deep-dive de pulmão. |
| **`sprints/`** | Evidências genéricas de sprint/paridade (s01, s02, s09, s10-validação, matriz de gaps, quality-guard, inventário de governança). |
| **`_processo/`** | Handoff, notas de call, znotes, prep de conversa, decisão de embedding/model-serving, exploração global. |
| **`checklists/`** | Checklist de implementação (Fase 1). |

## Ordem de leitura (fundação)

1. **Contexto:** `_fundacao/01-discovery-estado-atual-nlp-v0.md` → `_fundacao/02-analise-profunda-engines-nlp-v0.md`
2. **Ideação:** `_fundacao/03-documento-auxiliar-brainstorm-motor-ds-nlp-llm-ml-v0.md`
3. **Visão e entregas:** `_fundacao/04-visao-refinada-motor-nlp-unificado-v0.md` → `_fundacao/05-roadmap-entregas-sprint-v0.md`
4. **Alinhamento EngML:** `_fundacao/06-resumo-alinhamento-engml-v0.md`
5. **Síntese executiva:** `_fundacao/07-relatorio-final-v0.4-plataforma-nlp-clinica.md`
6. **Anexos:** `_fundacao/anexo01`…`anexo04` (modelo, arquitetura, histórias/tasks, decisões config/deploy)
7. **Diretrizes:** `_fundacao/diretriz-arquitetura-*` → `diretriz-desenvolvimento-*` → `diretriz-config-*` → `diretriz-tech-lead-*`
8. **Specs de engine:** `_fundacao/spec-rule-engine-t022-v0.md`, `_fundacao/spec-scoring-t023-v0.md`

## Por onde começar por especialidade

- **RADS (categoria):** `rads/notas/00 - linha-evolucao-xxrads-nlp-engine-v0.md` → `rads/doc-regras-clinicas-rads-v0.md`
- **Tireoide (relevância V1):** `tireoide/relatorio-final-tirads-v1-2026-07-12.md` (fecho) + `tireoide/mapa-gaps-tirads-v0.md` (gaps vivos) + `tireoide/gold-spot-tirads-v13-2026-07-07.md`
- **Hepatologia:** `hepatologia/Relatorio-final-homologacao-hepatologia-v1.md`

## Desenvolvimento (código)
- Validação local × Databricks: `_fundacao/doc-validacao-paridade-databricks-v0.md`
- Busca conversacional (escopo por componente): `_fundacao/doc-busca-conversacional-componentes-v0.md`
- Checklist Fase 1: `checklists/checklist-implementacao-motor-nlp-fase1-v0.md`
