"""Testes sintéticos para to_plain (sem PHI)."""

from __future__ import annotations

import pytest

from nlp_engine.text_pipeline import to_plain


def test_empty_string() -> None:
    assert to_plain("") == ""


def test_plain_ascii_passthrough_stripped() -> None:
    assert to_plain("  exame normal.  ") == "exame normal."


def test_literal_escape_newlines() -> None:
    raw = "linha um\\r\\nlinha dois"
    out = to_plain(raw)
    assert "linha um" in out
    assert "linha dois" in out
    assert out.count("\n") >= 1


def test_html_paragraph_to_text() -> None:
    raw = "<html><body><p>Colono scopia normal.</p></body></html>"
    out = to_plain(raw)
    assert "normal" in out.lower()
    assert "<p>" not in out


def test_minimal_rtf_extracts_content() -> None:
    rtf = (
        r"{\rtf1\ansi\deff0{\fonttbl{\f0 Times;}}\f0\fs24 "
        r"Paciente sem alteracoes relevantes.\par}"
    )
    out = to_plain(rtf)
    assert "alter" in out.lower() or "relevant" in out.lower()


def test_referencias_section_trimmed() -> None:
    raw = "Achados descritivos.\nReferências: artigo sintético 123."
    out = to_plain(raw)
    assert "Achados" in out
    assert "artigo sintético" not in out


@pytest.mark.parametrize(
    "snippet,forbidden",
    [
        ("Laudo gerado por um sistema especialista.", "sistema especialista"),
        ("Laudo pode nao estar completo na visualizacao em rtf.", "visualizacao em rtf"),
    ],
)
def test_footer_phrases_reduced_or_removed(snippet: str, forbidden: str) -> None:
    out = to_plain(snippet)
    assert forbidden not in out.lower()
    assert len(out.strip()) < len(snippet)
