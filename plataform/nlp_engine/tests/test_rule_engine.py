"""S02 T02.2 — process_rule_based (config dict, sem PHI)."""

from __future__ import annotations

import json

import pytest

from nlp_engine.rule_engine import process_rule_based
from nlp_engine.scoring import (
    SCORE_POLICY_V2_DENSITY,
    confidence_rule_based,
    fl_relevante_from_counts,
)


def test_rule_engine_finds_phrase_without_organ_filter() -> None:
    cfg = {
        "findings": {"lesao": ["nodulo"]},
        "negation_phrases": ["sem"],
        "negation_window": 7,
    }
    r = process_rule_based("TC torax. Nodulo no pulmao.", cfg)
    assert r["n_positive_spans"] >= 1
    assert any("nodulo" in x.lower() for x in r["summary_compact"])


def test_rule_engine_respects_target_organ() -> None:
    cfg = {
        "findings": {"lesao": ["nodulo"]},
        "target_organs": ["figado"],
        "organs": {"figado": {"seeds": ["figado", "hepatica"]}},
        "negation_phrases": [],
        "negation_window": 7,
    }
    no_org = process_rule_based("Nodulo descrito sem orgao alvo.", cfg)
    assert no_org["n_positive_spans"] == 0
    ok = process_rule_based("Figado com nodulo focal.", cfg)
    assert ok["n_positive_spans"] >= 1


def test_rule_engine_skips_negated_span() -> None:
    cfg = {
        "findings": {"lesao": ["nodulo"]},
        "negation_phrases": ["sem"],
        "negation_window": 7,
    }
    r = process_rule_based("Pulmao sem nodulo patologico.", cfg)
    assert r["n_positive_spans"] == 0
    assert r["n_negated_spans"] >= 1


def test_scoring_matches_legacy_bins() -> None:
    assert confidence_rule_based(n_positive_spans=1, n_negated_spans=0) == 0.9
    assert confidence_rule_based(n_positive_spans=0, n_negated_spans=1) == 0.35
    assert confidence_rule_based(n_positive_spans=0, n_negated_spans=0) == 0.0
    assert fl_relevante_from_counts(1) == 1
    assert fl_relevante_from_counts(0) == 0


def test_scoring_v2_density_increases_with_multiple_spans() -> None:
    s1 = confidence_rule_based(
        n_positive_spans=1, n_negated_spans=0, policy=SCORE_POLICY_V2_DENSITY
    )
    s3 = confidence_rule_based(
        n_positive_spans=3, n_negated_spans=0, policy=SCORE_POLICY_V2_DENSITY
    )
    assert s3 > s1


def test_matcher_finds_accented_token() -> None:
    cfg = {
        "findings": {"lesao": ["nodulo"]},
        "negation_phrases": [],
        "negation_window": 7,
        "use_spacy_matcher": True,
    }
    r = process_rule_based("Achado: Nódulo periferico.", cfg)
    assert r["n_positive_spans"] >= 1
    assert "rule_engine_version" in r


def test_findings_regex_finds_span() -> None:
    cfg = {
        "findings": {},
        "findings_regex": {"x": [r"\bALFA\b"]},
        "negation_phrases": [],
    }
    r = process_rule_based("Texto com ALFA no meio.", cfg)
    assert r["n_positive_spans"] >= 1


def test_proximity_rejects_distant_organ() -> None:
    cfg = {
        "findings": {"lesao": ["nodulo"]},
        "target_organs": ["figado"],
        "organs": {"figado": {"seeds": ["figado"]}},
        "finding_organ_max_chars": 5,
        "negation_phrases": [],
        "negation_window": 7,
    }
    sent = "Figado homogeneo " + ("x " * 50) + "nodulo periferico"
    r = process_rule_based(sent, cfg)
    assert r["n_positive_spans"] == 0


def test_proximity_accepts_close_organ() -> None:
    cfg = {
        "findings": {"lesao": ["nodulo"]},
        "target_organs": ["figado"],
        "organs": {"figado": {"seeds": ["figado"]}},
        "finding_organ_max_chars": 40,
        "negation_phrases": [],
        "negation_window": 7,
    }
    r = process_rule_based("Figado com nodulo de 5 mm.", cfg)
    assert r["n_positive_spans"] >= 1


@pytest.mark.parametrize(
    ("flag", "expect_entries"),
    [
        ("false", 0),
        ("0", 0),
        ("true", 1),
    ],
)
def test_engine_feature_flag_rule_engine(flag: str, expect_entries: int) -> None:
    from nlp_engine.engine import ClinicalNlpEngine

    eng = ClinicalNlpEngine(engine_version="1.0.0")
    cfg = {
        "feature_flags": {"rule_engine": flag},
        "findings": {"x": ["alfa"]},
        "negation_phrases": [],
    }
    out = eng.process([{"id_exame": "1", "exm_laudo_texto": "texto com alfa"}], cfg)
    payload = json.loads(out[0]["exm_laudo_resultado"])
    n = len(payload["summary_compact"])
    assert n == expect_entries
