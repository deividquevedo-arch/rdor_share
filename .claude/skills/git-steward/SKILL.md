---
name: git-steward
description: Procedimento de versionamento — inspecionar antes de mexer, separar por assunto, mensagem semântica, e verificar o push pela REF. Use antes de qualquer commit, ao preparar push ou PR, ou quando o working tree tiver mudanças misturadas.
---

# Git Steward — procedimento

> A parte de **segurança** (qual repo é qual, operações proibidas, segredos) está na regra
> `git-steward`, sempre em contexto. Aqui é o **como fazer**.

## 1. Situar antes de tocar

```bash
git -C <repo> remote -v            # qual repositório é este de verdade
git -C <repo> branch --show-current
git -C <repo> status --short
git -C <repo> diff --stat
```

Se a branch for `main`/`master`/`hml`, **parar** e propor `feature/...` ou `fix/...` antes de commitar.

## 2. Separar por assunto

Um commit = um assunto coerente. Se o working tree mistura temas, propor a divisão ao usuário —
não empacotar tudo por conveniência.

`git add` **seletivo**, por path. Evitar `git add .` quando houver ruído.

## 3. Antes de commitar, mostrar

- lista de arquivos que entram, com o tipo de mudança
- destacar binários, artefatos grandes ou arquivos de dados
- a mensagem sugerida

E **esperar confirmação** do conjunto + mensagem.

## 4. Mensagem

Semântica e imperativa: `fix(decision): juiz não revê evidência determinística`.

No corpo, o que a equipe precisará saber daqui a seis meses:

- **o defeito ou a necessidade**, com o número que o dimensiona
- **o mecanismo** — por que acontecia
- **a compatibilidade** — o que muda de comportamento, e onde não muda
- **como foi verificado** — gate rodado, teste que mata o mutante

Assinar com `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

⚠️ Aspas e crases em heredoc de shell podem comer partes da mensagem. Conferir com
`git log -1 --format=%B` depois de commitar.

## 5. Bump de versão, quando houver

`pyproject.toml` **e** `uv.lock` no **mesmo commit** — esquecer o lock já quebrou a hml.
Rodar o gate completo antes: `ruff check` **e** `ruff format --check` **e** `mypy` **e** `pytest`.
`ruff check` não cobre o format.

## 6. Push — só com autorização, e verificar pela REF

```bash
git -C <repo> push -u origin <branch>
git -C <repo> fetch origin -q
git -C <repo> rev-parse HEAD
git -C <repo> rev-parse origin/<branch>    # têm de bater
```

Exit code 0 **não** prova que subiu. Comparar as refs.

## 7. PR e tag

Fornecer sempre os três: **título**, **descrição** e **tag**. Tag anotada, apontando para o merge
commit na branch de destino — mover se a branch avançar antes do merge.

**Descrição: teto de 4.000 caracteres.** Medir antes de entregar:

```bash
python -c "import pathlib;s=pathlib.Path('desc.md').read_text(encoding='utf-8');print(len(s))"
```

O que **fica**, em qualquer corte: a **matriz de impacto** (exigida no code review acordado em
21/08), o aviso de breaking change e o alcance da publicação.

O que **sai primeiro**: detalhe de implementação que o revisor lê melhor no diff — nome de função,
protocolo interno, lista exaustiva de checks. Se ainda não couber, o excedente vai para a SPEC e
entra como link, não como anexo.

## O que esta skill NÃO faz

Não decide por conta própria fazer push, abrir PR ou criar tag. Prepara e espera.
