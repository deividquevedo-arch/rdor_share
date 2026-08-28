#!/usr/bin/env python
"""Gate de PHI e segredo — impede que dado de paciente ou credencial entre no histórico.

Roda como hook `PreToolUse` em `git commit` e `git push`, em **qualquer** repositório do
workspace. Verificações mecânicas por assinatura; nunca julgamento sobre o conteúdo.

Por que existe
--------------
Há 9 CSVs com texto de laudo em commits locais na raiz do workspace. Eles bloqueiam o backup de
`docs/` desde 2026-08-14 — mais de uma semana de trabalho sem cópia em lugar nenhum. A única
defesa contra o décimo é alguém lembrar, e lembrar não é controle.

Commit publicado com dado de paciente não se apaga: reescreve-se histórico, com todo mundo
tendo que reclonar. É o único erro deste projeto que não tem desfazer barato.

Onde intercepta
---------------
- **`git commit`** — inspeciona o índice (`git diff --cached`). É onde o dado entra no histórico.
- **`git push`** — inspeciona os commits que subiriam. É onde o dado sai da máquina.

`git add` **não** é interceptado de propósito: staging é local e reversível, e resolver
`git add .` exigiria adivinhar a lista de arquivos — adivinhação em gate produz bloqueio falso,
e gate que bloqueia à toa é gate que alguém desliga.

Regras
------
1. arquivo tabular versionado (`.csv`, `.xlsx`, `.parquet`, …) — dado não é artefato de repositório
2. CPF em formato pontuado, em qualquer arquivo
3. CRM, em qualquer arquivo — identificação de profissional é dado pessoal
4. chave privada ou `.env`

Saída
-----
Código 0 libera; código 2 bloqueia e o texto do stderr chega ao agente e ao usuário.

Escape hatch
------------
    [phi-waiver: <motivo>]

Na mensagem do commit (ou no próprio `git commit -m`). Fica no log e é auditável.
⚠️ Waiver serve para falso positivo — nunca para "é só um arquivinho".
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

LIMITE_LEITURA = 256 * 1024  # basta para a assinatura; evita segurar o commit em arquivo grande

EXT_TABULAR = {".csv", ".tsv", ".xlsx", ".xls", ".parquet", ".feather", ".dta", ".sav", ".pkl"}

# Pontuado de propósito: 11 dígitos soltos casam com id de exame, hash e timestamp.
RE_CPF = re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")
RE_CRM = re.compile(r"\bCRM\s*[-:/]?\s*[A-Z]{0,2}\s*\d{4,7}\b", re.I)
RE_CHAVE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")
RE_WAIVER = re.compile(r"\[phi-waiver:\s*[^\]]+\]")


def git(*args: str, cwd: Path) -> str:
    try:
        r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True,
                           check=False, errors="replace")
        return r.stdout if r.returncode == 0 else ""
    except OSError:
        return ""


def raiz_do_repo(cwd: Path) -> Path | None:
    top = git("rev-parse", "--show-toplevel", cwd=cwd).strip()
    return Path(top) if top else None


# `cd <dir> && git push` no INICIO do comando. O hook recebe o cwd da SESSAO, que neste workspace
# e a raiz — mas o push costuma alvejar um dos repos aninhados (nlp-engine-lib, plataforma...).
# Sem isto o gate inspecionava a raiz e bloqueava push de outro repo por arquivo que nem viaja
# nele: falso positivo que so tem duas saidas ruins, contornar o gate ou nunca pushar.
RE_CD = re.compile(r"""^\s*cd\s+(?:'([^']+)'|"([^"]+)"|(\S+))\s*(?:&&|;)""")


def cwd_efetivo(comando: str, cwd_sessao: Path) -> Path:
    """Diretorio onde o comando de fato roda: honra um `cd` inicial, senao o cwd da sessao."""
    m = RE_CD.match(comando)
    if not m:
        return cwd_sessao
    alvo_cd = Path(next(g for g in m.groups() if g))
    destino = alvo_cd if alvo_cd.is_absolute() else (cwd_sessao / alvo_cd)
    return destino if destino.is_dir() else cwd_sessao


def alvo(comando: str) -> str | None:
    """Qual dos dois momentos estamos interceptando."""
    if re.search(r"\bgit\s+(-\S+\s+)*commit\b", comando):
        return "commit"
    if re.search(r"\bgit\s+(-\S+\s+)*push\b", comando):
        return "push"
    return None


def arquivos_e_fonte(repo: Path, momento: str) -> tuple[list[str], str]:
    """Devolve (arquivos, prefixo para `git show`)."""
    if momento == "commit":
        saida = git("diff", "--cached", "--name-only", "--diff-filter=ACMR", cwd=repo)
        return [l for l in saida.splitlines() if l.strip()], ":"

    upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}", cwd=repo).strip()
    faixa = f"{upstream}..HEAD" if upstream else "HEAD~1..HEAD"
    saida = git("diff", "--name-only", "--diff-filter=ACMR", faixa, cwd=repo)
    return [l for l in saida.splitlines() if l.strip()], "HEAD:"


def conteudo(repo: Path, prefixo: str, caminho: str) -> str:
    return git("show", f"{prefixo}{caminho}", cwd=repo)[:LIMITE_LEITURA]


def achados(repo: Path, prefixo: str, caminho: str) -> list[str]:
    """Motivos pelos quais este arquivo não pode entrar. Vazio = liberado."""
    p = Path(caminho)
    motivos: list[str] = []

    if p.suffix.lower() in EXT_TABULAR:
        motivos.append(
            f"`{caminho}` é dado tabular. Dado não é artefato de repositório neste projeto — "
            f"vai para tabela no schema da especialidade."
        )
        return motivos  # não adianta ler o conteúdo: a extensão já decide

    if p.name in {".env", ".env.local"} or p.name.startswith(".env."):
        motivos.append(f"`{caminho}` é arquivo de ambiente — credencial não entra no git.")
        return motivos

    texto = conteudo(repo, prefixo, caminho)
    if not texto:
        return motivos

    n_cpf = len(RE_CPF.findall(texto))
    if n_cpf:
        motivos.append(f"`{caminho}` tem {n_cpf} ocorrência(s) de CPF.")

    n_crm = len(RE_CRM.findall(texto))
    if n_crm:
        motivos.append(
            f"`{caminho}` tem {n_crm} ocorrência(s) de CRM — identificação de profissional "
            f"também é dado pessoal."
        )

    if RE_CHAVE.search(texto):
        motivos.append(f"`{caminho}` contém chave privada.")

    return motivos


def mensagens(repo: Path, momento: str, comando: str) -> str:
    if momento == "commit":
        return comando  # o -m está no próprio comando
    upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}", cwd=repo).strip()
    faixa = f"{upstream}..HEAD" if upstream else "HEAD~1..HEAD"
    return git("log", "--format=%B", faixa, cwd=repo)


def main() -> int:
    try:
        evento = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0

    comando = str((evento.get("tool_input") or {}).get("command") or "")
    momento = alvo(comando)
    if momento is None:
        return 0

    repo = raiz_do_repo(cwd_efetivo(comando, Path(evento.get("cwd") or ".")))
    if repo is None:
        return 0

    if RE_WAIVER.search(mensagens(repo, momento, comando)):
        print("gate-phi: waiver declarado — liberado e auditável.", file=sys.stderr)
        return 0

    arquivos, prefixo = arquivos_e_fonte(repo, momento)
    bloqueios = [m for a in arquivos for m in achados(repo, prefixo, a)]
    if not bloqueios:
        return 0

    verbo = "commit" if momento == "commit" else "push"
    print(f"BLOQUEADO pelo gate de PHI — este {verbo} não pode seguir:\n", file=sys.stderr)
    for i, b in enumerate(bloqueios, 1):
        print(f"  {i}. {b}\n", file=sys.stderr)
    print(
        "Dado de paciente em commit publicado não se apaga — reescreve-se histórico.\n"
        "Para resolver: tire o arquivo do índice (`git restore --staged <arquivo>`) e mova o dado\n"
        "para fora do repositório, ou para tabela no schema da especialidade.\n"
        "Se for falso positivo, declare no commit: [phi-waiver: <motivo>]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
