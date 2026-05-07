"""S01 T01.5: laudos sinteticos (plain, RTF, HTML) com e sem negacao; sem PHI.

Cada caso aplica `to_plain` ao bruto e valida `is_negated_in_sentence_*` sobre o texto
resultante, alinhado ao fluxo notebook (limpeza antes do match/negacao).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from nlp_engine.text_pipeline import to_plain
from nlp_engine.text_pipeline.negation import (
    is_negated_in_sentence_config,
    is_negated_in_sentence_plain,
)

_NEG_TOKENS = ["nao", "sem", "ausencia de", "sem evidencias de"]
_WINDOW = 7

_FIXTURE_YAML = Path(__file__).resolve().parent.parent / "fixtures" / (
    "text_pipeline_header_aliases_minimal.yaml"
)


def _wrap(kind: str, body: str) -> str:
    if kind == "plain":
        return body
    if kind == "html":
        return f"<!DOCTYPE html><html><body><p>{body}</p></body></html>"
    if kind == "rtf":
        return (
            r"{\rtf1\ansi\deff0{\fonttbl{\f0 Times New Roman;}}\f0\fs24 "
            + body
            + r"\par}"
        )
    raise ValueError(kind)


def _span(s: str, needle: str) -> tuple[int, int]:
    """Offsets em ``s`` para a primeira ocorrencia de ``needle`` (case-insensitive)."""
    low = s.lower()
    n = needle.lower()
    start = low.index(n)
    return start, start + len(needle)


@pytest.mark.parametrize("kind", ["plain", "html", "rtf"])
def test_laudo_sem_negacao_apos_to_plain(kind: str) -> None:
    body = "Bexiga com paredes de espessura habitual no exame sintetico."
    out = to_plain(_wrap(kind, body))
    start, end = _span(out, "paredes")
    assert is_negated_in_sentence_plain(
        out, start, end, _NEG_TOKENS, window_tokens=_WINDOW
    ) is False


@pytest.mark.parametrize("kind", ["plain", "html", "rtf"])
def test_laudo_com_negacao_apos_to_plain(kind: str) -> None:
    body = "Bexiga sem calculos radiopacos no estudo sintetico."
    out = to_plain(_wrap(kind, body))
    start, end = _span(out, "calculos")
    assert is_negated_in_sentence_plain(
        out, start, end, _NEG_TOKENS, window_tokens=_WINDOW
    ) is True


@pytest.mark.parametrize("kind", ["plain", "html", "rtf"])
def test_laudo_com_frase_negacao_apos_to_plain(kind: str) -> None:
    body = "Figado com contornos regulares. Ausencia de lesoes focais sinteticas."
    out = to_plain(_wrap(kind, body))
    start, end = _span(out, "lesoes")
    assert is_negated_in_sentence_plain(
        out, start, end, _NEG_TOKENS, window_tokens=_WINDOW
    ) is True


def test_t015_negacao_via_yaml_apos_to_plain_html() -> None:
    payload = yaml.safe_load(_FIXTURE_YAML.read_text(encoding="utf-8"))
    neg_cfg = payload["nlp"]["negation"]
    body = "Rins sem hidronefrose no exame sintetico."
    out = to_plain(_wrap("html", body))
    start, end = _span(out, "hidronefrose")
    assert is_negated_in_sentence_config(out, start, end, neg_cfg) is True
