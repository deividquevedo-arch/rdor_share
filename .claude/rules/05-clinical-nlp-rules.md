---
description: NLP clínico — remissão à regra canónica motor-nlp.mdc e docs; evita duplicação
paths:
  - "nlp-engine-lib/src/**/*.py"
  - "**/config/speciality/*.py"
  - "docs/motor-nlp/**/*.md"
---

# NLP clínico

## Regra canónica

O texto **completo** das obrigações do Motor NLP clínico está em **`motor-nlp.mdc`** (`alwaysApply: true`). Esta entrada existe para **orientar agents** sem duplicar esse conteúdo.

## Checklist rápido para agents

- Backlog: só trabalho com **Sxx / Txx.y** mapeado; caso contrário parar e alinhar.
- **Lista para o negócio só sai do fluxo COMPLETO calibrado** (regra → híbrido → juiz). `rule_only`
  é estágio de desenvolvimento — ver *Entrega ao negócio* em `motor-nlp.mdc`.
- Zero hardcode clínico no código; **YAML** para keywords, thresholds, órgãos.
- **Sem PHI** em testes, logs ou repo.
- Contrato de dados e campos de saída conforme `motor-nlp.mdc` e `docs/motor-nlp/diretriz-config-e-governanca-v0.md`.

## Documentação

- `docs/motor-nlp/` — diretrizes, anexos, checklist de implementação.

## Anti-padrão

- Copiar parágrafos longos de `motor-nlp.mdc` para prompts de agent — preferir **“seguir motor-nlp.mdc”** + tarefa concreta.
