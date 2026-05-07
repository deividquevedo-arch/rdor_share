from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from nlp_engine.config_loader import merge_with_shared_organs

_ROOT = Path(__file__).resolve().parents[1]
_HEP_CFG = _ROOT / "configs" / "hepatologia" / "config.yaml"
_SHARED_ORGANS = _ROOT / "configs" / "shared" / "organs.yaml"


def _load_yaml(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    assert isinstance(raw, dict)
    return raw


def test_hepatologia_yaml_merge_with_shared_organs_has_expected_keys() -> None:
    specialty = _load_yaml(_HEP_CFG)
    shared = _load_yaml(_SHARED_ORGANS)
    merged = merge_with_shared_organs(shared, specialty)

    assert merged["specialty_id"] == "hepatologia"
    assert merged["config_version"].startswith("0.1.")
    assert merged["nlp"]["target_organs"] == ["figado", "vesicula_biliar", "vias_biliares"]
    assert "figado" in merged["nlp"]["organs"]
    assert "figado" in merged["nlp"]["header_aliases"]


def test_hepatologia_yaml_invalid_missing_specialty_id() -> None:
    specialty = _load_yaml(_HEP_CFG)
    specialty.pop("specialty_id", None)
    shared = _load_yaml(_SHARED_ORGANS)

    with pytest.raises(ValueError):
        merge_with_shared_organs(shared, specialty)


def test_hepatologia_yaml_invalid_embeddings_decision_mode() -> None:
    specialty = _load_yaml(_HEP_CFG)
    specialty["nlp"]["embeddings"]["decision_mode"] = "override"
    shared = _load_yaml(_SHARED_ORGANS)
    with pytest.raises(TypeError):
        merge_with_shared_organs(shared, specialty)
