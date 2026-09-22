---
description: Engenharia Databricks — notebooks como composition root, ambientes DEV/HML/PRD, sem lógica espessa no notebook
paths:
  - "**/ntb_ia_*.py"
  - "fabrica-ia-nlp-platform/**"
  - "fabrica-ia-plataforma/**"
  - "**/*.ipynb"
---

# Databricks e notebooks

## Princípios (alinhados a docs/motor-nlp)

- **Notebook enxuto (~50 linhas como referência):** leitura YAML / preparação de dict, chamada às libs, orquestração mínima — **sem** lógica NLP pesada inline.
- **Libs Python** concentram testes e reutilização; notebooks não substituem testes unitários.
- **Separação de ambientes:** DEV / HML / PRD — dados, clusters e permissões diferem; não assumir que um widget ou caminho funciona noutro ambiente sem validação.

## Segurança e dados

- **LGPD:** sem PHI em repositório, logs ou prompts versionados; conforme diretrizes de governança clínica.
- Segredos e credenciais: mecanismos da plataforma (secrets scopes, etc.), não valores em código versionado.

## Relação com agents

- Agents que operem sobre Databricks devem declarar **âmbito** (ex.: apenas estrutura de notebook, apenas validação de job) e remeter a esta regra + `04-python-lib-architecture.mdc`.

## O que esta regra não cobre

- Alteração de pipelines, jobs ou políticas de cluster sem decisão/backlog explícitos (MLOps/infra conforme `motor-nlp.mdc`).
