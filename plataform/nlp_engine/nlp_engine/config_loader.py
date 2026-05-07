"""S03 baseline: validacao/normalizacao e merge de config runtime (dict-in, sem I/O)."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

_REQ_TOP = ("specialty_id", "config_version", "nlp")


def _as_dict(name: str, raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return dict(raw)


def _validate_nlp_shape(nlp: Mapping[str, Any]) -> None:
    findings = nlp.get("findings")
    if findings is not None:
        if not isinstance(findings, Mapping):
            raise TypeError("nlp.findings must be a mapping[str, list[str]]")
        for _, terms in findings.items():
            if not isinstance(terms, (list, tuple)) or any(not isinstance(t, str) for t in terms):
                raise TypeError("nlp.findings values must be list[str]")
    target_organs = nlp.get("target_organs")
    if target_organs is not None:
        if not isinstance(target_organs, (list, tuple)) or any(
            not isinstance(x, str) for x in target_organs
        ):
            raise TypeError("nlp.target_organs must be list[str]")
    embeddings = nlp.get("embeddings")
    if embeddings is not None:
        if not isinstance(embeddings, Mapping):
            raise TypeError("nlp.embeddings must be a mapping")
        mode = embeddings.get("decision_mode")
        if mode is not None and str(mode).strip().lower() not in ("fallback", "hybrid"):
            raise TypeError("nlp.embeddings.decision_mode must be fallback|hybrid")
        band = embeddings.get("ambiguity_band")
        if band is not None:
            if not isinstance(band, (list, tuple)) or len(band) != 2:
                raise TypeError("nlp.embeddings.ambiguity_band must be [lo, hi]")
        by_m = embeddings.get("similarity_threshold_by_model")
        if by_m is not None:
            if not isinstance(by_m, Mapping):
                raise TypeError("nlp.embeddings.similarity_threshold_by_model must be a mapping")
            for _k, v in by_m.items():
                if v is not None and not isinstance(v, Mapping):
                    raise TypeError(
                        "nlp.embeddings.similarity_threshold_by_model values must be mappings"
                    )
                if isinstance(v, Mapping):
                    if "ambiguity_band" in v and (
                        not isinstance(v["ambiguity_band"], (list, tuple))
                        or len(v["ambiguity_band"]) != 2
                    ):
                        raise TypeError("per-model ambiguity_band must be [lo, hi]")


def _merge_organs(shared_organs: Any, specialty_organs: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(shared_organs, Mapping):
        out.update({str(k): deepcopy(v) for k, v in shared_organs.items()})
    if isinstance(specialty_organs, Mapping):
        for k, v in specialty_organs.items():
            key = str(k)
            if isinstance(v, Mapping) and isinstance(out.get(key), Mapping):
                merged = dict(out[key])
                merged.update(dict(v))
                out[key] = merged
            else:
                out[key] = deepcopy(v)
    return out


def _shared_organs_map(shared: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(shared, Mapping):
        return {}
    shared_organs = shared.get("organs")
    if isinstance(shared_organs, Mapping):
        return shared_organs
    return shared


def _shared_header_aliases(shared: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(shared, Mapping):
        return {}
    aliases = shared.get("header_aliases")
    return aliases if isinstance(aliases, Mapping) else {}


def load(config: Mapping[str, Any]) -> dict[str, Any]:
    """Valida e normaliza uma config runtime completa (specialty)."""
    conf = _as_dict("config", config)
    missing = [k for k in _REQ_TOP if k not in conf]
    if missing:
        raise ValueError(f"missing required fields: {missing}")
    if not isinstance(conf["specialty_id"], str) or not conf["specialty_id"].strip():
        raise TypeError("specialty_id must be a non-empty string")
    if not isinstance(conf["config_version"], str) or not conf["config_version"].strip():
        raise TypeError("config_version must be a non-empty string")

    conf["nlp"] = _as_dict("nlp", conf["nlp"])
    conf["data"] = _as_dict("data", conf.get("data", {}))
    conf["monitoring"] = _as_dict("monitoring", conf.get("monitoring", {}))
    _validate_nlp_shape(conf["nlp"])
    return conf


def merge_with_shared_organs(
    shared_organs: Mapping[str, Any] | None,
    specialty_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Merge de 2 camadas: universo shared + specialty (specialty prevalece por chave)."""
    conf = load(specialty_config)
    nlp = dict(conf["nlp"])
    shared_map = _shared_organs_map(shared_organs)
    merged_organs = _merge_organs(shared_map, nlp.get("organs", {}))
    nlp["all_organs"] = deepcopy(merged_organs)
    nlp["organs"] = merged_organs

    merged_aliases = dict(_shared_header_aliases(shared_organs))
    specialty_aliases = nlp.get("header_aliases")
    if isinstance(specialty_aliases, Mapping):
        merged_aliases.update({str(k): deepcopy(v) for k, v in specialty_aliases.items()})
    if merged_aliases:
        nlp["header_aliases"] = merged_aliases
    conf["nlp"] = nlp
    return conf
