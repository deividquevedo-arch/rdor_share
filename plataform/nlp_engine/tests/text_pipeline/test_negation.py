"""Testes sinteticos de negacao (S01 T01.3); sem PHI."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from nlp_engine.text_pipeline.negation import (
    is_negated_in_sentence_config,
    is_negated_in_sentence_plain,
    starts_with_negator,
)

_SYNTH_NEG = [
    "nao",
    "sem",
    "ausencia de",
    "sem evidencias de",
]


def test_starts_with_negator() -> None:
    assert starts_with_negator("sem achado", _SYNTH_NEG) is True
    assert starts_with_negator("achado presente", _SYNTH_NEG) is False


def test_negation_left_unigram() -> None:
    s = "Paciente sem evidencia de processo sintetico."
    start = s.index("processo")
    end = start + len("processo")
    assert is_negated_in_sentence_plain(s, start, end, _SYNTH_NEG, window_tokens=7) is True


def test_negation_no_substring_sem_inside_word() -> None:
    s = "Aspecto semelhante ao caso sintetico."
    start = s.index("semelhante")
    end = start + len("semelhante")
    assert is_negated_in_sentence_plain(s, start, end, _SYNTH_NEG, window_tokens=7) is False


def test_negation_phrase_left() -> None:
    s = "Paciente sem evidencias de problema sintetico."
    start = s.index("problema")
    end = start + len("problema")
    assert (
        is_negated_in_sentence_plain(
            s,
            start,
            end,
            list(_SYNTH_NEG),
            window_tokens=10,
        )
        is True
    )


def test_negation_hit_starts_with_negator() -> None:
    s = "Nao ha achado sintetico no exame."
    start = s.index("Nao")
    end = start + len("Nao")
    assert is_negated_in_sentence_plain(s, start, end, _SYNTH_NEG, window_tokens=5) is True


def test_negation_invalid_span_false() -> None:
    s = "Texto curto."
    assert is_negated_in_sentence_plain(s, 100, 101, _SYNTH_NEG, 7) is False


def test_negation_config_dict() -> None:
    cfg = {"tokens": list(_SYNTH_NEG), "window_tokens": 7}
    s = "Obs sem achado patologico sintetico."
    start = s.index("achado")
    end = start + len("achado")
    assert is_negated_in_sentence_config(s, start, end, cfg) is True


_FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / (
    "text_pipeline_header_aliases_minimal.yaml"
)


def test_negation_from_yaml_fixture() -> None:
    payload = yaml.safe_load(_FIXTURE.read_text(encoding="utf-8"))
    neg_cfg = payload["nlp"]["negation"]
    s = "Texto sintetico sem achado patologico."
    start = s.index("achado")
    end = start + len("achado")
    assert is_negated_in_sentence_config(s, start, end, neg_cfg) is True


@pytest.mark.parametrize("window", [0, 3])
def test_window_limits_matches(window: int) -> None:
    # Muitos tokens entre negador e alvo: com janela 3 pode nao alcancar "sem"
    s = "Um dois tres quatro cinco seis sete oito alvo sintetico."
    start = s.index("alvo")
    end = start + len("alvo")
    neg_far = is_negated_in_sentence_plain(
        s,
        start,
        end,
        ["sem"],
        window_tokens=window,
    )
    # "sem" nao aparece nesta frase
    assert neg_far is False
