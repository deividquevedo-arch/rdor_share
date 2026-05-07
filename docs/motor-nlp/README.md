# Documentacao -- Motor NLP / Plataforma clinica

Indice central dos documentos de discovery, arquitetura, roadmap, diretrizes e alinhamento operacional.

---

## Ordem de leitura sugerida

1. **Contexto e estado atual:** `01-discovery-estado-atual-nlp-v0.md` -> `02-analise-profunda-engines-nlp-v0.md`
2. **Ideacao / brainstorm:** `03-documento-auxiliar-brainstorm-motor-ds-nlp-llm-ml-v0.md`
3. **Visao e entregas:** `04-visao-refinada-motor-nlp-unificado-v0.md` -> `05-roadmap-entregas-sprint-v0.md`
4. **Alinhamento EngML:** `06-resumo-alinhamento-engml-v0.md`
5. **Sintese executiva:** `07-relatorio-final-v0-plataforma-nlp-clinica.md`
6. **Anexos:** `anexo01` a `anexo04` (modelo, arquitetura, historias/tasks, decisoes config/deploy)
7. **Diretrizes de execucao:** `diretriz-arquitetura-*` -> `diretriz-desenvolvimento-*` -> `diretriz-config-*` -> `diretriz-tech-lead-*`
8. **Operacional / alinhamento:** `doc-pontos-alinhamento-*`, `doc-gestao-dependencias-*`, resumos de decisoes, `prep-conversa-mleng.md`

---

## Desenvolvimento (codigo)

- **Validação local x Databricks (paridade, matriz, smoke):** [doc-validacao-paridade-databricks-v0.md](doc-validacao-paridade-databricks-v0.md)
- **Pacote `nlp_engine` (TextPipeline, CI, Pandoc/RTF):** [plataform/nlp_engine/README.md](../../plataform/nlp_engine/README.md)
- **Checklist Fase 1 (modulo a modulo):** [checklists/checklist-implementacao-motor-nlp-fase1-v0.md](checklists/checklist-implementacao-motor-nlp-fase1-v0.md)
- **Historias e tasks:** `anexo03-historias-e-tasks-v0.md`
- **Arquitetura tecnica e contrato I/O:** `anexo02-arquitetura-motor-nlp-v0.md`
- **Regras do Agent (Cursor):** `.cursor/rules/motor-nlp.mdc` (raiz do repositorio)

---

## Tabela de ficheiros

| Ficheiro | Descricao |
|----------|-----------|
| `01-discovery-estado-atual-nlp-v0.md` | Discovery do estado atual dos pipelines NLP |
| `02-analise-profunda-engines-nlp-v0.md` | Analise dos 5 tipos de engine por especialidade |
| `03-documento-auxiliar-brainstorm-motor-ds-nlp-llm-ml-v0.md` | Brainstorm DS/NLP/ML/LLM |
| `04-visao-refinada-motor-nlp-unificado-v0.md` | Visao tecnico-cientifica e fases 3-4 |
| `05-roadmap-entregas-sprint-v0.md` | Roadmap por sprint, lacunas, plano de acao |
| `06-resumo-alinhamento-engml-v0.md` | Briefing para Engenharia de ML |
| `07-relatorio-final-v0-plataforma-nlp-clinica.md` | Relatorio consolidado v0 |
| `anexo01-modelo-motor-nlp-v0.md` | Modelo acessivel do motor (o que/como/porque) |
| `anexo02-arquitetura-motor-nlp-v0.md` | Arquitetura hoje vs alvo, contrato de dados |
| `doc-contrato-engine-rule-based-v0.md` | Contrato minimo entrada / `nlp` / saida (motor rule-based S02) |
| `doc-quality-guard-t024b-composition-v0.md` | T02.4b ate lib `monitoring`: gate no composition root |
| `anexo03-historias-e-tasks-v0.md` | Historias S00+ e tasks por fase |
| `notas/s01-s02-paridade-evidencia-v0.md` | Comandos e registo de paridade S01 / auditoria S02 |
| `notas/paridade-legado-matriz-gaps-v0.md` | Matriz legado vs motor e Gap_ID (anexo03) |
| `anexo04-decisoes-config-e-deploy-v0.md` | Decisoes HEAD + MLOps (config/deploy) |
| `diretriz-arquitetura-pre-codigo-v0.md` | Protocolo de 4 passos antes de codar |
| `diretriz-desenvolvimento-libs-v0.md` | Padroes das 3 libs e testes |
| `diretriz-config-e-governanca-v0.md` | YAML em duas camadas, governanca clinica |
| `diretriz-tech-lead-refatoracao-v0.md` | RPI, SDD, progressive disclosure, code review |
| `doc-pontos-alinhamento-pre-codigo-v0.md` | Checklist B1-B4 e paralelos pre-codigo |
| `doc-gestao-dependencias-libs-v0.md` | Opcoes de pip/requirements/cluster no Databricks |
| `doc-validacao-paridade-databricks-v0.md` | Validação pytest x cluster; matriz de paridade; smoke Databricks |
| `prep-conversa-mleng.md` | Preparacao pessoal para conversa com EngML |
| `resumo-decisoes-config-deploy-v0.md` | Resumo acessivel das decisoes config/deploy |
| `onepager-decisoes-config-deploy-v0.md` | One-pager executivo config/deploy |
| `checklists/checklist-implementacao-motor-nlp-fase1-v0.md` | SPEC/impl/teste/validacao por historia Fase 1 |
| `notas/` | Notas de trabalho (`znotes`, etc.) |

---

## Codigo dos algoritmos

Os repositorios de produto continuam em `algoritmos/` e `modelo/` na raiz do projeto (fora de `docs/`).
