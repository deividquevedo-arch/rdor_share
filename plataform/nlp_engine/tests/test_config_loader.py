import pytest

from nlp_engine.config_loader import load, merge_with_shared_organs


def test_load_accepts_dict() -> None:
    cfg = {
        "specialty_id": "hepatologia",
        "config_version": "1.0.0",
        "nlp": {"findings": {"lesao": ["nodulo"]}, "target_organs": ["figado"]},
    }
    got = load(cfg)
    assert got["specialty_id"] == "hepatologia"
    assert got["config_version"] == "1.0.0"
    assert got["nlp"]["findings"]["lesao"] == ["nodulo"]
    assert got["data"] == {}
    assert got["monitoring"] == {}


def test_load_rejects_non_dict() -> None:
    with pytest.raises(TypeError):
        load("not a dict")  # type: ignore[arg-type]


def test_load_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError):
        load({"specialty_id": "x", "nlp": {}})


def test_load_rejects_bad_findings_shape() -> None:
    cfg = {"specialty_id": "x", "config_version": "1", "nlp": {"findings": {"a": "b"}}}
    with pytest.raises(TypeError):
        load(cfg)


def test_merge_with_shared_organs_specialty_overrides() -> None:
    shared = {
        "organs": {"figado": {"seeds": ["figado"]}, "baco": {"seeds": ["baco"]}},
        "header_aliases": {"figado": ["figado", "fígado"]},
    }
    specialty = {
        "specialty_id": "hepato",
        "config_version": "1",
        "nlp": {
            "organs": {"figado": {"regex": [r"hepatic"]}},
            "header_aliases": {"figado": ["figado"]},
        },
    }
    out = merge_with_shared_organs(shared, specialty)
    organs = out["nlp"]["organs"]
    assert "figado" in organs and "baco" in organs
    assert organs["figado"]["seeds"] == ["figado"]
    assert organs["figado"]["regex"] == [r"hepatic"]
    assert out["nlp"]["all_organs"]["baco"]["seeds"] == ["baco"]
    assert out["nlp"]["header_aliases"]["figado"] == ["figado"]
