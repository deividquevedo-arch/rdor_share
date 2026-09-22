"""Gera .cursor/rules/*.mdc a partir de .claude/rules/*.md — fonte canonica e o Cursor derivado.

Por que existe: usamos Claude Code e Cursor. Manter as regras nos dois lugares a mao faz elas
divergirem, o que a propria regra 07-documentation proibe ("uma fonte canonica e links").

Direcao: .claude/rules/  ->  .cursor/rules/
  - regra SEM `paths`  ->  alwaysApply: true
  - regra COM `paths`  ->  alwaysApply: false + globs

Rodar depois de editar qualquer regra:
    python .claude/scripts/sync-cursor-rules.py
"""
import io
import os
import re
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FONTE = os.path.join(RAIZ, ".claude", "rules")
DEST = os.path.join(RAIZ, ".cursor", "rules")

FM = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
AVISO = ("<!-- GERADO por .claude/scripts/sync-cursor-rules.py a partir de .claude/rules/. "
         "NAO editar aqui: edite a fonte e rode o script. -->")


def ler_frontmatter(texto):
    """Devolve (description, [paths], corpo). Parser minimo — evita dependencia de pyyaml."""
    m = FM.match(texto)
    if not m:
        return "", [], texto
    bloco, corpo = m.group(1), texto[m.end():]
    desc = ""
    paths = []
    dentro_paths = False
    for linha in bloco.splitlines():
        if linha.startswith("description:"):
            desc = linha.split(":", 1)[1].strip()
            dentro_paths = False
        elif linha.strip() == "paths:":
            dentro_paths = True
        elif dentro_paths and linha.lstrip().startswith("- "):
            paths.append(linha.lstrip()[2:].strip().strip('"').strip("'"))
        elif linha and not linha[0].isspace():
            dentro_paths = False
    return desc, paths, corpo


def main():
    if not os.path.isdir(FONTE):
        print(f"fonte nao encontrada: {FONTE}")
        return 1
    os.makedirs(DEST, exist_ok=True)

    gerados, mantidos = [], []
    esperados = set()
    for arq in sorted(os.listdir(FONTE)):
        if not arq.endswith(".md"):
            continue
        nome = arq[:-3]
        esperados.add(nome + ".mdc")
        desc, paths, corpo = ler_frontmatter(io.open(os.path.join(FONTE, arq), encoding="utf-8").read())

        fm = ["---", f"description: {desc}"]
        if paths:
            fm.append("globs: " + ", ".join(paths))
            fm.append("alwaysApply: false")
        else:
            fm.append("alwaysApply: true")
        fm.append("---")

        saida = "\n".join(fm) + "\n\n" + AVISO + "\n\n" + corpo.lstrip("\n")
        destino = os.path.join(DEST, nome + ".mdc")
        antes = io.open(destino, encoding="utf-8").read() if os.path.exists(destino) else None
        if antes == saida:
            mantidos.append(nome)
        else:
            io.open(destino, "w", encoding="utf-8", newline="\n").write(saida)
            gerados.append((nome, "sempre" if not paths else f"{len(paths)} glob(s)"))

    orfaos = [a for a in os.listdir(DEST) if a.endswith(".mdc") and a not in esperados]

    for nome, escopo in gerados:
        print(f"  gerado    {nome:32} {escopo}")
    if mantidos:
        print(f"  inalterados: {len(mantidos)}")
    if orfaos:
        print("\n  ORFAOS em .cursor/rules (sem par em .claude/rules) — remover a mao se obsoletos:")
        for o in orfaos:
            print(f"    {o}")
    print(f"\n{len(gerados)} gerado(s), {len(mantidos)} inalterado(s), {len(orfaos)} orfao(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
