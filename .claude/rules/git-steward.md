---
description: Git — qual repositório é qual, e o que NUNCA fazer sem confirmação. Procedimento de commit está na skill git-steward.
---

# Git — segurança

> Procedimento (fluxo, staging, mensagens) está na **skill `git-steward`**, que carrega sob demanda.
> Aqui fica só o que precisa estar em contexto **sempre**, porque errar custa caro.

## Cinco repositórios, quatro no Azure — verificado 2026-08-14

| Local | Remote | Papel |
|-------|--------|-------|
| raiz `Projects/` | GitHub `rdor_share` (A3Data) | Backup pessoal e docs. **Não** é o fluxo corporativo. |
| `nlp-engine-lib/` | Azure | **Lib canónica do motor.** |
| `fabrica-ia-nlp-platform/` | Azure | **Plataforma MLOps NOVA** — runner e configs. |
| `fabrica-ia-plataforma/` | Azure | Runner e configs **ANTIGOS**, em descontinuação. |
| `fabrica-ia-lib/` | Azure | Lib **LEGADA**. Não é destino de trabalho novo. |

- **Nunca deduzir o repositório pelo nome da pasta.** Confirmar com `git remote -v` e
  `git branch --show-current` antes de qualquer operação que altere estado.
- Na **raiz** o `origin` é o GitHub de backup: comandos ali **não** enviam nada para o Azure.

## Proibido sem confirmação explícita, uma a uma

`reset` · `clean` · force push · `rebase` · `filter-branch` / `filter-repo` · apagar branch remota ·
`amend` de commit já publicado.

Se for pedido algo assim: **parar**, dizer o risco em uma linha, e esperar confirmação.

## Push e PR

- **`git push` só quando o usuário pedir**, no momento. Autorização não é retroativa nem futura.
- **Confirmar pela REF, não pelo exit code** — `git push` pode sair 0 sem subir.
  `git rev-parse HEAD` contra `git rev-parse origin/<branch>`.
- PR na plataforma **só depois de a config estar validada**; durante a iteração, só a branch.
- **Descrição de PR: no máximo 4.000 caracteres.** Limite do time — o que não couber vai para a
  SPEC e entra como link. Medir ANTES de entregar o texto.

## Segredos e dados sensíveis

Interromper e alertar se aparecer em staging: `.env`, `*.pem`, chaves, tokens, credenciais em
YAML/JSON, `password=`, `api_key`, `BEGIN PRIVATE KEY`. Nunca contornar `.gitignore` para incluí-los.

⚠️ A raiz tem **CSVs com texto de laudo** em commits locais (dívida de LGPD registada) — não pushar
`docs/` até a reorganização.
