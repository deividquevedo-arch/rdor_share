"""Remocao de rodapes e avisos de sistema no final do laudo (notebook biliar)."""

from __future__ import annotations

import re

from nlp_engine.text_pipeline.norm import norm


def remove_final_laudo(texto: str) -> str:
    if not texto:
        return ""

    texto = re.sub(
        r"(?i)\blaudo\s+(?:gerado|liberado)\s+por\s+um?\s*sistema\s+especialista\b\.?",
        " ",
        texto,
    )
    texto = re.sub(
        r"(?i)\bpara\s+visualiza\w*\s+(?:o\s+conte[uú]do\s+do\s+)?laudo\b.?"
        r"\b(?:favor\s+)?(?:acesse|acessar)\b.?\b(?:viewer|op[cç][aã]o\s+imagem|"
        r"imagem\s+dispon[ií]vel)\b\.?",
        " ",
        texto,
    )
    texto = re.sub(
        r"(?i)\blaudo\s+pode\s+na[oã]\s+estar\s+completo\s+na\s+visualiza\w+\s+em\s+"
        r"(?:rtf|imagem)\b\.?",
        " ",
        texto,
    )
    texto = re.sub(
        r"(?i)\brefer[eê]ncia[s]?\s*[:\-.]?.*$",
        " ",
        texto,
        flags=re.DOTALL,
    )

    pats_norm = [
        r"laudo\s+pode\s+nao\s+estar\s+completo.*?\b(rtf|imagem)\b",
        r"para\s+visualiza\w+.*?\b(rtf|imagem|sistema|webris|viewer)\b",
        r"acessar\s+a\s+op[cç][aã]o\s+imagem",
        r"sistema\s+especialista",
        r"sistema\s+da\s+radiologia\s+webris",
        r"este\s+laudo\s+foi\s+liberado\s+por\s+um?\s+sistema\s+especialista",
        r"favor\s+acessar\s+a\s+op[cç][aã]o\s+imagem\s+dispon[ií]vel",
        r"visualiza\w+\s+do\s+conteudo\s+do\s+laudo",
        r"laudo\s+pode\s+nao\s+estar\s+completo\s+na\s+visualiza\w+\s+em\s+rtf",
    ]

    rx = re.compile(r"^(?:\s*(?:{})(?:[.!?])?\s*)$".format("|".join(pats_norm)), re.IGNORECASE)

    lines_in = texto.splitlines()
    lines_out = []

    for line in lines_in:
        if not line.strip():
            lines_out.append(line)
            continue

        sents = re.split(r"(?<=[.!?])\s+", line.strip())
        kept = []
        for s in sents:
            s_norm = norm(s)
            if not rx.match(s_norm):
                kept.append(s)

        lines_out.append(" ".join(kept).strip())

    out = "\n".join(lines_out)
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\s+([.!?,;:])", r"\1", out)
    return out.strip()
