"""Testes de ancoragem por orgao (S01 T01.2). Doc/spaCy so nos testes com ``@requires_spacy``."""

from __future__ import annotations

import pytest

try:
    import spacy
except ImportError:
    spacy = None  # type: ignore[assignment, misc]

from nlp_engine.text_pipeline.anchors import (  # noqa: E402
    organ_anchors,
    organ_term_set,
    segment_by_organs,
    sentence_mentions_organ,
)

requires_spacy = pytest.mark.skipif(
    spacy is None,
    reason="spacy ausente: pip install -e '.[dev]' ou '.[spacy]'",
)


@pytest.fixture
def nlp_pt():
    assert spacy is not None
    nlp = spacy.blank("pt")
    nlp.add_pipe("sentencizer")
    return nlp


def test_organ_term_set_includes_name_seeds_lexicon() -> None:
    cfg = {"seeds": ["fígado", "hepatico"]}
    terms = organ_term_set("Figado", cfg, ["figado nodular"])
    assert "figado" in terms
    assert "hepatico" in terms
    assert "figado nodular" in terms


def test_sentence_mentions_organ_word_boundary() -> None:
    cfg = {"seeds": []}
    assert sentence_mentions_organ("O figado esta preservado.", "figado", cfg) is True
    assert sentence_mentions_organ("Aspecto semelhante ao normal.", "sem", cfg) is False


def test_sentence_mentions_organ_regex() -> None:
    cfg = {"seeds": [], "regex": [r"\btc\s+abdome\b"]}
    assert sentence_mentions_organ("Paciente com TC abdome recente.", "x", cfg) is True


@requires_spacy
def test_organ_anchors_one_sentence(nlp_pt: spacy.language.Language) -> None:
    doc = nlp_pt("O figado esta normal. A bexiga esta distendida.")
    spans = organ_anchors(doc, "figado", {"seeds": []}, [])
    assert len(spans) == 1
    a, b = spans[0]
    assert doc[a:b].text.strip().startswith("O")


@requires_spacy
def test_segment_by_organs_two_organs(nlp_pt: spacy.language.Language) -> None:
    doc = nlp_pt("Figado homogeneo. Bexiga com urina.")
    organs = {
        "figado": {"seeds": []},
        "bexiga": {"seeds": []},
    }
    blocks = segment_by_organs(doc, organs, {})
    organs_found = {b["organ"] for b in blocks}
    assert organs_found == {"figado", "bexiga"}
    for b in blocks:
        assert "text" in b and b["text"]
        assert b["start_char"] >= 0
        assert b["end_char"] > b["start_char"]


def test_organ_anchors_requires_doc() -> None:
    with pytest.raises(TypeError):
        organ_anchors(None, "x", {}, [])
