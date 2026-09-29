#!/usr/bin/env python
"""Gate de documentação — bloqueia push sem a documentação que a mudança exige.

Roda como hook `PreToolUse` em comandos `git push`. Faz verificações **mecânicas**, nunca
julgamento de qualidade: cada regra pergunta "o arquivo que deveria acompanhar essa mudança foi
tocado?", e não "está bem escrito?".

Por que existe
--------------
Em 2026-08-20 o pipeline do TI-RADS quebrou em produção. A mudança de contrato responsável estava
documentada no `RELEASE.md` da lib (0.6.0) e **não** na documentação que a plataforma consome — que
é a que o time de engenharia lê. Documentamos para nós e não para quem consome.

Saída
-----
Código 0 libera; código 2 bloqueia e o texto do stderr chega ao agente e ao usuário.

Escape hatch
------------
Marcador no corpo da mensagem do último commit, com motivo obrigatório:

    [doc-waiver: <motivo>]

Waiver aparece no log e é auditável. Use quando a regra não se aplica, não para pular trabalho.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

# Módulos cuja assinatura é contrato com quem consome a lib. Mudança aqui exige atualizar a
# documentação do CONSUMIDOR, não só a nossa.
SUPERFICIE_DE_CONTRATO = {
    "config_loader.py",
    "contracts.py",
    "engine.py",
    "output_invariants.py",
}

# Onde vive a documentação que a plataforma lê. Caminhos relativos à raiz do workspace.
DOC_DO_CONSUMIDOR = (
    "fabrica-ia-nlp-platform/docs/usage",
    "fabrica-ia-nlp-platform/.docs",
)


def git(*args: str, cwd: Path) -> str:
    """Roda git e devolve stdout limpo; string vazia em qualquer falha."""
    try:
        r = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
        )
        return r.stdout.strip() if r.returncode == 0 else ""
    except OSError:
        return ""


def raiz_do_repo(cwd: Path) -> Path | None:
    top = git("rev-parse", "--show-toplevel", cwd=cwd)
    return Path(top) if top else None


def arquivos_do_push(repo: Path) -> list[str]:
    """Arquivos alterados nos commits que o push levaria — ou no último commit, se não houver
    upstream configurado."""
    upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}", cwd=repo)
    faixa = f"{upstream}..HEAD" if upstream else "HEAD~1..HEAD"
    saida = git("diff", "--name-only", faixa, cwd=repo)
    return [l for l in saida.splitlines() if l.strip()]


def mensagens_do_push(repo: Path) -> str:
    upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}", cwd=repo)
    faixa = f"{upstream}..HEAD" if upstream else "HEAD~1..HEAD"
    return git("log", "--format=%B", faixa, cwd=repo)


def versao_no_pyproject(repo: Path, ref: str) -> str | None:
    conteudo = git("show", f"{ref}:pyproject.toml", cwd=repo)
    m = re.search(r'^version\s*=\s*"([^"]+)"', conteudo, re.M)
    return m.group(1) if m else None


def verificar(repo: Path) -> list[str]:
    """Devolve a lista de bloqueios. Vazia = pode pushar."""
    alterados = arquivos_do_push(repo)
    if not alterados:
        return []

    bloqueios: list[str] = []
    tocou = lambda prefixo: any(a.startswith(prefixo) for a in alterados)  # noqa: E731

    # ── 1. bump de versão exige entrada no RELEASE.md ────────────────────────────────────
    if "pyproject.toml" in alterados:
        upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}", cwd=repo)
        antes = versao_no_pyproject(repo, upstream or "HEAD~1")
        agora = versao_no_pyproject(repo, "HEAD")
        if agora and agora != antes:
            release = (repo / "RELEASE.md").read_text(encoding="utf-8", errors="replace") \
                if (repo / "RELEASE.md").exists() else ""
            if agora not in release:
                bloqueios.append(
                    f"versão subiu para {agora} e o RELEASE.md não tem entrada para ela. "
                    f"Quem consome a lib descobre o que mudou por ali."
                )

    # ── 2. módulo da lib alterado exige a SPEC dele ──────────────────────────────────────
    mods = [
        Path(a).stem
        for a in alterados
        if a.startswith("src/nlp_engine/nlp_engine/") and a.endswith(".py")
        and not Path(a).name.startswith("_")
    ]
    if mods:
        specs = {a for a in alterados if a.startswith("docs/specs/")}
        sem_spec = [m for m in mods if not any(m in s for s in specs)]
        if sem_spec and not specs:
            bloqueios.append(
                f"módulo(s) alterado(s) sem SPEC atualizada: {', '.join(sorted(set(sem_spec)))}. "
                f"A SPEC é o que permite o code review de quem não escreveu o módulo."
            )

    # ── 3. mudança na superfície de contrato exige a doc do CONSUMIDOR ───────────────────
    contrato = [
        Path(a).name
        for a in alterados
        if a.startswith("src/nlp_engine/nlp_engine/") and Path(a).name in SUPERFICIE_DE_CONTRATO
    ]
    if contrato:
        # a doc do consumidor vive em OUTRO repositório: procura alteração pendente lá
        workspace = repo.parent
        tocada = False
        for rel in DOC_DO_CONSUMIDOR:
            alvo = workspace / rel
            if not alvo.exists():
                continue
            repo_doc = raiz_do_repo(alvo)
            if repo_doc and git("status", "--porcelain", "--", str(alvo), cwd=repo_doc):
                tocada = True
                break
        if not tocada:
            bloqueios.append(
                f"mudança em superfície de contrato ({', '.join(sorted(set(contrato)))}) sem "
                f"atualizar a documentação do CONSUMIDOR em {DOC_DO_CONSUMIDOR[0]}. "
                f"Foi exatamente essa lacuna que quebrou o TI-RADS em produção: a mudança estava "
                f"no nosso RELEASE.md e não na doc que a plataforma lê."
            )

    return bloqueios


def main() -> int:
    try:
        evento = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0

    comando = str((evento.get("tool_input") or {}).get("command") or "")
    if "git push" not in comando:
        return 0

    cwd = Path(evento.get("cwd") or ".")
    repo = raiz_do_repo(cwd)
    if repo is None:
        return 0

    # o gate só vale para a lib; a plataforma tem a esteira do MLOps
    if repo.name != "nlp-engine-lib":
        return 0

    if re.search(r"\[doc-waiver:\s*[^\]]+\]", mensagens_do_push(repo)):
        print("gate-documentacao: waiver declarado no commit — liberado e auditável.",
              file=sys.stderr)
        return 0

    bloqueios = verificar(repo)
    if not bloqueios:
        return 0

    print("BLOQUEADO pelo gate de documentação:\n", file=sys.stderr)
    for i, b in enumerate(bloqueios, 1):
        print(f"  {i}. {b}\n", file=sys.stderr)
    print(
        "Para resolver: rode a skill `documentar-mudanca`, que produz os artefatos que faltam.\n"
        "Se a regra não se aplica, declare no commit: [doc-waiver: <motivo>]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
