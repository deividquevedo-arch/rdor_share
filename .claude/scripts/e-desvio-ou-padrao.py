#!/usr/bin/env python
"""É desvio deste autor, ou é o padrão da plataforma?

**Rodar ANTES de apontar qualquer chave numa revisão de config ou de job.**

Cobrar de um autor o que 5 das 7 configs fazem igual é ruído — e pior, desloca a atenção do item
que importa. Aconteceu duas vezes neste projeto (17/09 e 22/09); na segunda, **quatro** itens de
uma revisão caíram de uma vez.

    python .claude/scripts/e-desvio-ou-padrao.py ambiguity_band
    python .claude/scripts/e-desvio-ou-padrao.py pause_status --jobs
    python .claude/scripts/e-desvio-ou-padrao.py similarity_threshold --excluir tumor_osseo

Saída: quantas declaram, com que valor, e o veredito — PADRÃO, DESVIO ou DIVIDIDO.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

# ⚠️ O console deste projeto é `cp1252` e quebra com acento. Reconfigurar é melhor do que
# escrever sem acento: script de uso diário que falha na máquina real não é usado.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAIZ = Path(__file__).resolve().parents[2] / "fabrica-ia-nlp-platform"
CONFIGS = RAIZ / "plataform" / "config" / "speciality"
JOBS = RAIZ / "jobs" / "definicoes"


def _valor_declarado(texto: str, chave: str) -> str | None:
    """Devolve o valor literal da chave, ou ``None`` quando ela não é declarada.

    Aceita as formas que aparecem nos arquivos do projeto: ``'chave': valor`` em Python e
    ``"chave": valor`` em JSON. Pega o primeiro literal — lista, número, booleano ou string.

    🔴 **22/09: a primeira versão não reconhecia BLOCO.** O padrão só casava escalar e lista,
    então ``'runtime': {`` devolvia ``None`` e o script respondia *"ninguém declara"* para uma
    chave que as SETE configs declaram. É a resposta exatamente invertida, na classe de chave
    que mais aparece em revisão — ``runtime``, ``llm_router``, ``embeddings``,
    ``findings_policy``, ``segmentation`` são todos blocos.

    Bloco devolve o literal ``{bloco}``: o script diz que a chave EXISTE e não finge comparar
    conteúdo de dicionário, que não é o que ele mede.
    """
    padrao = re.compile(
        rf"""['"]{re.escape(chave)}['"]\s*:\s*([{{]|\[[^\]]*\]|['"][^'"]*['"]|[A-Za-z0-9_.+-]+)"""
    )
    m = padrao.search(texto)
    if not m:
        return None
    bruto = m.group(1).strip()
    return "{bloco}" if bruto == "{" else bruto


def _arquivos(usar_jobs: bool) -> list[Path]:
    if usar_jobs:
        return sorted(p for p in JOBS.glob("*.json") if "demo" not in p.name)
    return sorted(CONFIGS.glob("ntb_ia_*_config.py"))


def _nome(caminho: Path, usar_jobs: bool) -> str:
    if usar_jobs:
        return caminho.stem.replace("-batch", "")
    return caminho.stem.replace("ntb_ia_", "").replace("_config", "")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("chave", help="a chave a investigar, sem aspas (ex.: ambiguity_band)")
    ap.add_argument("--jobs", action="store_true", help="olhar jobs/definicoes em vez das configs")
    ap.add_argument("--excluir", default="", help="linha a deixar de fora (a que está em revisão)")
    args = ap.parse_args()

    arquivos = _arquivos(args.jobs)
    if not arquivos:
        print(f"nenhum arquivo encontrado em {JOBS if args.jobs else CONFIGS}", file=sys.stderr)
        return 2

    achados: dict[str, str | None] = {}
    for caminho in arquivos:
        nome = _nome(caminho, args.jobs)
        if args.excluir and nome == args.excluir:
            continue
        achados[nome] = _valor_declarado(caminho.read_text(encoding="utf-8"), args.chave)

    declaram = {k: v for k, v in achados.items() if v is not None}
    print(f"chave: {args.chave}   ({len(achados)} linhas comparadas)\n")
    for nome, valor in achados.items():
        print(f"  {nome:<24} {valor if valor is not None else '(nao declara)'}")

    if not declaram:
        print(f"\n>> NINGUEM declara `{args.chave}`.")
        print("   Se a linha em revisao declara, e DESVIO -- e pode ser inovacao legitima.")
        return 0

    contagem = Counter(declaram.values())
    valor, n = contagem.most_common(1)[0]
    print(f"\n  declaram: {len(declaram)} de {len(achados)}")
    print(f"  valor mais comum: {valor}  ({n}x)")

    if n >= 2 and n >= len(declaram) / 2:
        print(f"\n>> PADRAO DA PLATAFORMA. Não cobrar de um autor isoladamente.")
        print("   Se o padrao estiver errado, o lugar e o card acumulador da plataforma")
        print("   (`299238` — SPEC 27 contradiz o código) ou o alinhamento único.")
    elif len(contagem) == len(declaram):
        print("\n>> DIVIDIDO: cada linha declara um valor. Não há padrão a invocar —")
        print("   a cobranca precisa de razao propria, medida, e nao de comparacao.")
    else:
        print("\n>> DESVIO: a maioria não faz assim. Apontar é legítimo — com o porquê.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
