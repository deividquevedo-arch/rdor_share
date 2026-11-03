"""Gera um mapa (MOC) por pasta do cofre docs/, ligando toda nota ao grafo do Obsidian.

Uso: python .claude/scripts/gerar-mapas-obsidian.py <pasta> [seco] [prefixo=<nome>]
- reescreve os arquivos mapa-<pasta>.md (marcados com tags: [mapa]); nao edita nenhuma outra nota;
- _arquivo/ fica de fora (congelado). Rodar de novo sempre que entrarem documentos novos.
"""
import re, sys
from pathlib import Path
ROOT = Path(sys.argv[1]); ARGS = sys.argv[2:]; DRY = "seco" in ARGS
PREFIXO = next((a.split("=",1)[1] for a in ARGS if a.startswith("prefixo=")), ROOT.resolve().name)
def visivel(p): return not any(x.startswith(".") for x in p.relative_to(ROOT).parts) and "_arquivo" not in p.parts
def slug(d): r = d.relative_to(ROOT).parts; return "mapa-" + (PREFIXO + "-" + "-".join(r) if r else PREFIXO) if PREFIXO != "docs" else "mapa-" + ("-".join(r) if r else "docs")
def eh_mapa(p):
    return p.read_text(encoding="utf-8", errors="ignore").startswith("---" + chr(10) + "tags: [mapa]")

def titulo(p):
    for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()[:40]:
        m = re.match(r"#\s+(.+)", ln)
        if m: return re.sub(r"[`*\[\]]", "", m.group(1)).strip()[:110]
    return ""
dirs = sorted({p.parent for p in ROOT.rglob("*.md") if visivel(p)} | {ROOT})
# garante que pastas intermediárias também tenham mapa
for d in list(dirs):
    q = d
    while q != ROOT: q = q.parent; dirs.append(q)
dirs = sorted(set(d for d in dirs if visivel(d) or d == ROOT))
criados = []
for d in dirs:
    nome = slug(d); alvo = d / f"{nome}.md"
    notas = sorted([p for p in d.glob("*.md") if visivel(p) and not eh_mapa(p)], key=lambda p: p.stem,
                   reverse=(d.name == "diario"))
    subs = sorted(s for s in dirs if s.parent == d and s != d)
    pai = "README" if d == ROOT / "motor-nlp" else (slug(d.parent) if d != ROOT else None)
    rel = "/".join(d.relative_to(ROOT).parts) or ROOT.resolve().name
    linhas = ["---", "tags: [mapa]", "---", "", f"# Mapa — `{rel}`", ""]
    if d == ROOT / "motor-nlp": linhas += ["> Índice navegável da frente: [[README|índice do motor-nlp]] · estado atual: [[ESTADO]].", ""]
    elif pai: linhas += [f"↑ [[{pai}]]", ""]
    linhas += ["> Gerado para ligar cada nota ao grafo. Uma linha por nota, com o título do documento. "
               "`_arquivo/` fica de fora (congelado).", ""]
    if subs:
        linhas += ["## Subpastas", ""] + [f"- [[{slug(s)}|{s.name}/]]" for s in subs] + [""]
    if notas:
        linhas += ["## Notas", ""] + [f"- [[{p.stem}]]" + (f" — {titulo(p)}" if titulo(p) and titulo(p).lower() != p.stem.lower() else "") for p in notas] + [""]
    if not DRY: alvo.write_text("\n".join(linhas), encoding="utf-8")
    criados.append((str(alvo.relative_to(ROOT)), len(notas), len(subs)))
for c in criados: print(*c, sep=" | ")
print("mapas:", len(criados))
