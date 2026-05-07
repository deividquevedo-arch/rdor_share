"""Integracao do piloto Pulmao com runtime YAML (sem PHI)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from nlp_engine.config_loader import merge_with_shared_organs
from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.output_invariants import validate_engine_output_row

_ROOT = Path(__file__).resolve().parents[2]
_PULMAO_CFG = _ROOT / "configs" / "pulmao" / "config.yaml"
_SHARED_ORGANS = _ROOT / "configs" / "shared" / "organs.yaml"


def _merged_pulmao_runtime() -> tuple[dict, str, str]:
    specialty = yaml.safe_load(_PULMAO_CFG.read_text(encoding="utf-8-sig"))
    shared = yaml.safe_load(_SHARED_ORGANS.read_text(encoding="utf-8-sig"))
    assert isinstance(specialty, dict)
    assert isinstance(shared, dict)
    merged = merge_with_shared_organs(shared, specialty)
    nlp = dict(merged["nlp"])
    return nlp, str(merged["specialty_id"]), str(merged["config_version"])


def test_pulmao_runtime_detects_positive_and_sets_relevante() -> None:
    nlp, sid, cv = _merged_pulmao_runtime()
    eng = ClinicalNlpEngine(engine_version="integ-pulmao")
    row = {
        "id_exame": "p1",
        "exm_laudo_texto": "Pulmoes com nodulo nao calcificado no lobo superior direito.",
    }
    out = eng.process([row], nlp, specialty_id=sid, config_version=cv)
    assert validate_engine_output_row(out[0]) == []
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert out[0]["fl_relevante"] == 1
    assert payload["n_positive_spans"] >= 1


def test_pulmao_runtime_detects_without_word_pulmao() -> None:
    nlp, sid, cv = _merged_pulmao_runtime()
    eng = ClinicalNlpEngine(engine_version="integ-pulmao")
    row = {
        "id_exame": "p3",
        "exm_laudo_texto": "Lobo inferior direito com opacidade irregular e micronodulos.",
    }
    out = eng.process([row], nlp, specialty_id=sid, config_version=cv)
    assert validate_engine_output_row(out[0]) == []
    assert out[0]["fl_relevante"] == 1


def test_pulmao_runtime_respects_negation() -> None:
    nlp, sid, cv = _merged_pulmao_runtime()
    eng = ClinicalNlpEngine(engine_version="integ-pulmao")
    row = {"id_exame": "p2", "exm_laudo_texto": "Pulmao sem nodulo suspeito."}
    out = eng.process([row], nlp, specialty_id=sid, config_version=cv)
    assert validate_engine_output_row(out[0]) == []
    payload = json.loads(out[0]["exm_laudo_resultado"])
    assert out[0]["fl_relevante"] == 0
    assert payload["n_positive_spans"] == 0
    assert payload["n_negated_spans"] >= 1
