# Agent Factory — operação

## Propósito

Padronizar **criação**, **revisão** e **auditoria** de agents especializados, em harmonia com `cursor.md`, as regras `.cursor/rules/00-07`, e o backlog do motor NLP.

## Fluxos

### 1. Criar agent

1. Verificar `agents/registry/agents_catalog.md` — evitar duplicar responsabilidade.
2. Escolher template em `agents/templates/` (`agent.template.md` + especialização se necessário).
3. Preencher **responsabilidade única**, faz/não faz, remissões (sem colar `motor-nlp.mdc` inteiro).
4. Registar no catálogo com ID `AF-xxx` e status `experimental` até primeira revisão.
5. Opcional: entrada mínima em `cursor.md` apenas se for índice global (evitar poluição).

### 2. Rever agent (qualidade)

- **Composição:** um papel, um nome, prompts curtos com ponteiros a ficheiros.
- **Fronteira:** não cobrir Git + NLP + Databricks no mesmo agent salvo justificação documentada.
- **Governança:** `docs/agents/agent_governance.md` para critérios de aprovação.

### 3. Auditar (manutenção)

- Periodicidade definida pelo time (ex.: início de sprint).
- Verificar links quebrados, agents obsoletos, sobreposição com regras Cursor.
- Atualizar catálogo e datas.

## Relação com código e infra

- A Factory **não** altera pipelines Databricks, notebooks de produção, ou libs por si só — isso continua ligado a **Sxx/Txx** e implementação humana/agent implementador.
- Alterações no **repositório oficial** Rede D'Or: sempre através do gate em `01-git-safety.mdc` e `git-steward.mdc`.

## Artefactos obrigatórios por novo agent

| Artefacto | Descrição |
|-----------|-----------|
| Linha no catálogo | ID, nome, responsabilidade, template base |
| Ficheiro derivado do template | Guardado em local acordado (ex.: `agents/definitions/` futuro) ou referência no catálogo |
| Workflow seguido | `docs/agents/agent_creation_workflow.md` |

## Princípio final

**Menos agents bem definidos** é preferível a **muitos agents genéricos**.
