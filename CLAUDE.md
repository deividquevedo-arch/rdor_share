# RedeDor AI Engineering Context

## Contexto

Projeto do time de Ciência de Dados e IA da Rede D'Or.

O ambiente principal envolve:

- Azure Databricks
- notebooks e bibliotecas Python
- pipelines de dados
- NLP clínico aplicado a laudos
- Delta Lake
- ambientes DEV / HML / PRD
- Git e versionamento
- governança, LGPD e rastreabilidade

---

## Objetivos

- criar código limpo e modular
- reduzir acoplamento
- melhorar reusabilidade
- facilitar manutenção
- garantir escalabilidade
- melhorar rastreabilidade técnica

---

## Regras Globais

- nunca hardcodar secrets, tokens ou paths pessoais
- nunca expor dados sensíveis
- sempre separar configuração da lógica
- preservar compatibilidade com Databricks
- evitar notebooks monolíticos
- priorizar simplicidade antes de complexidade
- explicar impacto técnico antes de grandes mudanças
- considerar validações, testes mínimos e logging básico
- documentar decisões relevantes

---

## Princípios de Engenharia

- priorizar simplicidade antes de complexidade
- evitar overengineering
- priorizar modularização e baixo acoplamento
- utilizar abordagem incremental para mudanças estruturais
- manter documentação mínima, objetiva e recuperável

---

## Stack Principal

- Python
- Spark
- Databricks
- Azure
- NLP clínico
- Machine Learning
- Delta Lake

---

## Padrões Esperados

- modularização
- funções pequenas e reutilizáveis
- configuração desacoplada
- código orientado a manutenção
- SOLID quando fizer sentido para o contexto

---

## Estado das frentes

O estado atual de cada frente — o que está feito, o que falta, de quem depende — vive em
@docs/motor-nlp/ESTADO.md e é carregado aqui.

**Manter atualizado.** Estado vai nesse documento; fato durável e lição aprendida vão para a
memória. Memória com "estado atual" e data envelhece e passa a enganar.

---

## Onde vivem as regras

- **`.claude/rules/*.md` é a FONTE CANÓNICA.** Regra sem `paths:` carrega em toda sessão;
  com `paths:` carrega só quando um arquivo que casa o padrão é lido.
- **`.cursor/rules/*.mdc` é GERADO** — não editar lá. Depois de mexer numa regra:

  ```
  python .claude/scripts/sync-cursor-rules.py
  ```

- Procedimento de vários passos não é regra: vira **skill**. Coisa que precisa rodar num momento
  fixo (antes de commit, depois de editar) não é regra: vira **hook**.

## Ciclo de sessão

| skill | quando |
|---|---|
| **`checkin`** | ao começar — o que espera você, o que espera terceiros, e um foco proposto |
| **`checkout`** | ao encerrar — grava o diário do dia, atualiza o `ESTADO.md` e a memória |
| **`git-steward`** | antes de commit, push ou PR |
| **`validar-padrao-plataforma`** | antes de commit, push ou PR **na plataforma** (`fabrica-ia-nlp-platform`), ao criar linha ou módulo e ao revisar PR — regra `09-plataforma-padrao-codigo` |

O diário fica em `docs/motor-nlp/_processo/diario/AAAA-MM-DD.md` — é o **histórico** (o que
aconteceu e por quê). O `ESTADO.md` é o **retrato** (onde estamos agora) e é sobrescrito.

---

## Formato Esperado das Respostas

Sempre responder com:

1. diagnóstico
2. riscos ou impactos
3. recomendação
4. exemplo prático
5. próximos passos
