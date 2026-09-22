# Catálogo de agents

Versão inicial da camada **Agent Factory**. Cada entrada deve ter **responsabilidade única**. Agents grandes ou sobrepostos devem ser divididos ou fundidos após revisão.

Status: `ativo` | `experimental` | `deprecado`

| ID | Nome | Responsabilidade única | Regras / docs principais | Template base | Status |
|----|------|-------------------------|---------------------------|---------------|--------|
| AF-001 | **Agent Factory Implementer** | Criar/atualizar definições de agents, templates e governança; auditar catálogo | `02-agent-factory.mdc`, `agents/factory/agent_factory.md` | `agent.template.md` | ativo |
| AF-002 | **Motor NLP Implementer** | Implementar/refinar código das libs NLP conforme SPEC e paridade legado | `motor-nlp.mdc`, `05-clinical-nlp-rules.mdc`, `docs/motor-nlp/` | `nlp_agent.template.md` | ativo |
| AF-003 | **Databricks Notebook Engineer** | Estrutura enxuta de notebooks, injecção de dict, separação DEV/HML/PRD | `03-databricks-engineering.mdc`, `04-python-lib-architecture.mdc` | `databricks_agent.template.md` | ativo |
| AF-004 | **Structural Code Reviewer** | Revisão de PR: acoplamento, contratos, simplicidade, riscos — sem implementar feature | `00-global-project-rules.mdc`, `06-testing-quality.mdc` | `reviewer_agent.template.md` | ativo |
| AF-005 | **Git Steward (usage)** | Operações Git seguras; distinguir backup A3Data vs Azure DevOps | `git-steward.mdc`, `01-git-safety.mdc` | — | ativo |
| AF-006 | **NLP Lib Port Sync** | **Guardião** de `fabrica-ia-lib` (foco `src/fabrica_ia/nlp_engine/`): A3Data vs Azure; transposição, adaptações e paridade funcional | `git-steward.mdc`, `01-git-safety.mdc`, `doc-transmissao-engml-nlp-engine-v0.md` | `agents/definitions/AF-006-nlp-lib-port-sync.md` | ativo |
| AF-007 | **Levantamento Medido** | Devolver **número + a consulta que o produziu**; não conclui, não recomenda, não escreve em disco | `00-global-project-rules.mdc`, `05-clinical-nlp-rules.mdc`, `07-documentation.mdc` | `agents/definitions/AF-007-levantamento-medido.md` | experimental |

## Obsolescência

- Ao deprecar: mudar status, data e substituto na coluna **Responsabilidade** ou nota abaixo.

## Notas

- **AF-005** não duplica o ficheiro `git-steward.mdc`; é o *papel* humano/agent quando se pede disciplina Git explícita.
- Novos agents: preencher linha e seguir `docs/agents/agent_creation_workflow.md`.
- **AF-006:** definição completa em `agents/definitions/AF-006-nlp-lib-port-sync.md` (usar como system prompt ou anexar na conversa).
