"""Mede o grafo do cofre Obsidian como o Obsidian resolve os links: órfãs, ilhas e links fantasmas.

Uso: python .claude/scripts/verificar-grafo-obsidian.py [raiz-do-cofre]
- link com caminho resolve pelo caminho; nome repetido resolve para a mesma pasta, senão para o caminho mais curto;
- ignora links dentro de crase e de bloco de código; _arquivo/ é reportado à parte (congelado).
"""
import re, sys
from pathlib import Path
from collections import defaultdict, Counter
ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
notas = [p for p in ROOT.rglob("*.md") if not any(x.startswith(".") for x in p.relative_to(ROOT).parts) and "node_modules" not in p.parts]
rel = {p: p.relative_to(ROOT).with_suffix("").as_posix().lower() for p in notas}
por_rel = {v: k for k, v in rel.items()}
por_nome = defaultdict(list)
for p in notas: por_nome[p.stem.lower()].append(p)
def limpar(t):
    t = re.sub(r"```.*?```", "", t, flags=re.S)
    return re.sub(r"`[^`\n]*`", "", t)
def resolver(alvo, origem):
    a = alvo.strip().rstrip("\\").removesuffix(".md").lower()
    if "/" in a:
        if a in por_rel: return por_rel[a]
        a = a.split("/")[-1]
    c = por_nome.get(a, [])
    if not c: return None
    mesma = [q for q in c if q.parent == origem.parent]
    return (mesma or sorted(c, key=lambda q: len(q.parts)))[0]
adj, ent = defaultdict(set), defaultdict(set); fant = []
for p in notas:
    t = limpar(p.read_text(encoding="utf-8", errors="ignore"))
    for w in re.findall(r"\[\[([^\]|#^]+)", t):
        q = resolver(w, p)
        if q is None: fant.append((p.relative_to(ROOT).as_posix(), w.strip()))
        elif q != p: adj[p].add(q); adj[q].add(p); ent[q].add(p)
    for m in re.findall(r"\]\(([^)#\s]+\.md)\)", t):
        q = resolver(Path(m).with_suffix("").as_posix().lstrip("./"), p) or resolver(Path(m).stem, p)
        if q and q != p: adj[p].add(q); adj[q].add(p); ent[q].add(p)
arq = lambda p: "_arquivo" in p.parts
seen, comps = set(), []
for p in notas:
    if p in seen: continue
    st, comp = [p], []
    while st:
        x = st.pop()
        if x in seen: continue
        seen.add(x); comp.append(x); st += list(adj[x] - seen)
    comps.append(comp)
comps.sort(key=len, reverse=True)
orf = [p for p in notas if not adj[p]]
print(f"notas: {len(notas)} | ilhas: {len(comps)} (maior = {len(comps[0])}) | órfãs: {len(orf)} ({sum(map(arq, orf))} no _arquivo) | fantasmas: {len(fant)}")
fora = [c for c in comps[1:] if not all(arq(x) for x in c)]
print("ilhas fora da principal (exceto _arquivo):", [[x.relative_to(ROOT).as_posix() for x in c][:3] for c in fora][:15])
print("fantasmas:", fant[:15])
