---
description: Documentação — onde escrever, ligação ao backlog, evitar duplicação com regras Cursor
paths:
  - "docs/**/*.md"
  - "**/*.md"
---

# Documentação

## Onde vive a doc de produto

- **Motor NLP / domínio:** `docs/motor-nlp/` (diretrizes, anexos, roadmap, histórias).
- **Agents e governança IA:** `docs/agents/` e `agents/registry/`.
- **Índice Cursor:** `cursor.md` na raiz — aponta para regras e docs; **não** duplicar capítulos longos das diretrizes.

## Mudanças de código

- Referenciar **Sxx/Txx** em PR ou notas de commit quando aplicável.
- Documentar decisões não óbvias no sítio já usado pelo time (ADR, nota, ou diretriz — conforme convenção existente).

## Registro técnico, nunca pessoal

Documentação é artefato de engenharia. Escrever em registro impessoal, sempre.

**Não entra em documento:**

- primeira pessoa — "eu havia afirmado", "cheguei a registrar", "nossa validação", "conseguimos ler"
- nome de pessoa como sujeito de um fato técnico — "foi ali que o <fulano> mediu"
- terceira pessoa que aponta time — "eles", "deles", "do lado deles", "é config deles"
- citação de fala de reunião ou de chat

**Entra no lugar:** o fato, a medição e o parâmetro que a produziu. Quem precisa de dono nomeado
é o **card**, não o documento.

| em vez de | escrever |
|---|---|
| "foi ali que o <fulano> mediu 126 de 127 como erro" | "na configuração com `threshold: 0.80`, 126 de 127 casos adicionais foram classificados como incorretos" |
| "a decisão é do card `283644`, com o <fulano>" | "a decisão pertence ao card `283644`" |
| "é config deles" | "é parâmetro da configuração da especialidade" |
| "duas frases nomearam o problema: '<citação>'" | o requisito que a fala expressa |
| "Correções ao que eu havia afirmado antes" | "Divergências entre a descrição do card e o estado do código" |

⚠️ **A medição é preservada integralmente.** O número perde o dono, não o valor — nem a data, nem
o parâmetro que o gerou, nem a rastreabilidade ao card.

**Exceção:** guia de uso dirigido ao leitor (`COMO-USAR.md`, tutorial) mantém a segunda pessoa —
"seu primeiro exemplo", "copie e cole". É voz de instrução, não apontamento pessoal.

## Anti-padrão

- Ter duas fontes que divergem para o mesmo requisito (ex.: regra no `.mdc` **e** parágrafo contraditório noutro doc). Preferir **uma fonte canónica** e links.

## Relação com Agent Factory

- Novos agents: actualizar `agents/registry/agents_catalog.md` e, se necessário, uma linha em `cursor.md` apenas como índice.
