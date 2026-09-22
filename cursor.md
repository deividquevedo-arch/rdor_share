# Cursor — fonte única de entrada (índice)

Este ficheiro é o **índice canónico** para agents e humanos: aponta para regras, governança e docs. **Não substitui** os documentos referenciados — evita duplicação e deriva.

## Hierarquia de verdade (ordem)

1. **Backlog acordado** — histórias/tasks (**Sxx** / **Txx.y**) e sprint corrente (`docs/motor-nlp/anexo03-historias-e-tasks-v0.md`, roadmap).
2. **Diretrizes do domínio** — `docs/motor-nlp/diretriz-*.md`, anexos e checklists.
3. **Regras Cursor** — `.cursor/rules/` (ver números `00-` a `07-` abaixo).
4. **Governança de agents** — `docs/agents/agent_governance.md`, `agents/registry/agents_catalog.md`, `agents/factory/agent_factory.md`.

## Frentes de trabalho (`docs/`)

| frente | índice | o que é |
|---|---|---|
| **Motor NLP** | `docs/motor-nlp/ESTADO.md` | a lib `nlp_engine` e as réguas por especialidade — a frente madura |
| **CDI** | `docs/cdi/README.md` | repositório clínico, legibilidade do laudo e filtros de entrada — o que chega ao motor |
| **Randomização** | `docs/randomizacao/README.md` | quem entra na lista da captação e por qual braço — desenho de estudo |
| **Multiagentes** | `docs/multiagentes/README.md` | como `nlp_engine`, prontuário, histórico do paciente e NER convivem |
| **Agents** | `docs/agents/`, `agents/registry/agents_catalog.md` | ferramenta de trabalho, **não** produto |

⚠️ **Cada frente guarda o próprio estado no seu índice.** Misturá-las foi o que tornou o
`ESTADO.md` do motor difícil de ler.

## Regras Cursor (`.cursor/rules/`)

| Ficheiro | Âmbito |
|----------|--------|
| `00-global-project-rules.mdc` | Princípios globais, SDD/RPI, compactação de contexto |
| `01-git-safety.mdc` | Segurança Git + validação antes do repo oficial |
| `02-agent-factory.mdc` | Criação, revisão e auditoria de agents especializados |
| `03-databricks-engineering.mdc` | Notebooks, ambientes DEV/HML/PRD |
| `04-python-lib-architecture.mdc` | Três libs, injecção de dict, fronteiras |
| `05-clinical-nlp-rules.mdc` | NLP clínico — remete à regra canónica `motor-nlp.mdc` |
| `06-testing-quality.mdc` | Testes, qualidade, PHI |
| `07-documentation.mdc` | Onde e como documentar mudanças |

**Regras de domínio já existentes (não renomear):**

- `motor-nlp.mdc` — Motor NLP clínico (sempre aplicável ao trabalho do motor).
- `git-steward.mdc` — Git local A3Data vs Azure DevOps Rede D'Or.

## Agent Factory

- Processo: `docs/agents/agent_creation_workflow.md`
- Fábrica (operacional): `agents/factory/agent_factory.md`
- Catálogo: `agents/registry/agents_catalog.md`
- Templates: `agents/templates/`
- **Portagem lib (A3Data → Azure) — guardião `fabrica-ia-lib` / `src/fabrica_ia/nlp_engine`:** `AF-006` — `agents/definitions/AF-006-nlp-lib-port-sync.md`

## Ambientes

- **DEV / HML / PRD:** código e notebooks devem respeitar promoção controlada; mudanças em repo **oficial** só após validação explícita (ver `01-git-safety.mdc` e `git-steward.mdc`).

---
*Última organização: camada Agent Factory (estrutura mínima de governança).*
