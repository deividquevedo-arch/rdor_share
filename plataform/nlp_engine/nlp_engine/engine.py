"""ClinicalNlpEngine — interface do motor clinico (S02 T02.1+, Fase 1 rule-based)."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from importlib import metadata
from typing import Any
from uuid import uuid4

from nlp_engine.rule_engine import process_rule_based
from nlp_engine.semantic_expand import (
    effective_embeddings_ambiguity_band,
    effective_similarity_threshold,
    embeddings_decision_mode,
    embeddings_enabled,
    embeddings_hybrid_weights,
    semantic_evidence,
)
from nlp_engine.scoring import (
    confidence_calibrated_meta,
    confidence_hybrid,
    confidence_rule_based,
    fl_relevante_from_counts,
    normalize_score_policy,
)
from nlp_engine.text_pipeline.anchors import segment_by_organs
from nlp_engine.text_pipeline.by_headers import segment_by_headers_plain
from nlp_engine.llm_router_backend import llm_router_step
from nlp_engine.text_pipeline.to_plain import to_plain

_INPUT_PASS_THROUGH = (
    "id_exame",
    "id_paciente",
    "id_unidade",
    "exm_laudo_texto",
    "exm_mod",
    "exm_tipo",
    "dt_exame",
)
_SEG_DEFAULT = "auto"
_SEG_ALLOWED = {"auto", "headers_only", "anchors_only", "full_doc"}
_SEG_NLP = None


def _package_version() -> str:
    try:
        return metadata.version("nlp_engine")
    except metadata.PackageNotFoundError:
        return "0.1.0"


def _raw_laudo_text(row: Mapping[str, Any]) -> str:
    primary = row.get("exm_laudo_texto")
    if primary is not None and str(primary).strip():
        return str(primary)
    fallback = row.get("Laudo")
    if fallback is not None:
        return str(fallback)
    return ""


def _trailing_line_patterns(nlp_config: Mapping[str, Any]) -> list[str] | None:
    tp = nlp_config.get("text_pipeline")
    if not isinstance(tp, Mapping):
        return None
    raw = tp.get("trailing_line_patterns")
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        return [str(p) for p in raw]
    return None


def _rule_engine_enabled(nlp_config: Mapping[str, Any]) -> bool:
    flags = nlp_config.get("feature_flags")
    if not isinstance(flags, Mapping):
        return True
    v = flags.get("rule_engine")
    if v is None:
        return True
    return str(v).lower() in ("1", "true", "yes")


def _as_mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _as_str_list(raw: Any) -> list[str]:
    if not isinstance(raw, (list, tuple)):
        return []
    return [str(x) for x in raw if str(x).strip()]


def _in_band(v: float, lo: float, hi: float) -> bool:
    return lo <= v <= hi


def _clip01(value: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if v != v:
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def _llm_router_enabled(nlp_config: Mapping[str, Any]) -> bool:
    cfg = _as_mapping(nlp_config.get("llm_router"))
    if not cfg:
        return False
    v = cfg.get("enabled", False)
    return str(v).lower() in ("1", "true", "yes")


def _calibrated_hybrid_enabled(nlp_config: Mapping[str, Any]) -> bool:
    flags = _as_mapping(nlp_config.get("feature_flags"))
    v = flags.get("calibrated_hybrid", False)
    return str(v).lower() in ("1", "true", "yes")


def _segmentation_mode(nlp_config: Mapping[str, Any]) -> str:
    seg = _as_mapping(nlp_config.get("segmentation"))
    mode = str(seg.get("mode", _SEG_DEFAULT)).strip().lower() or _SEG_DEFAULT
    return mode if mode in _SEG_ALLOWED else _SEG_DEFAULT


def _force_full_doc_targets(nlp_config: Mapping[str, Any]) -> set[str]:
    seg = _as_mapping(nlp_config.get("segmentation"))
    return {x.lower() for x in _as_str_list(seg.get("force_full_doc_for"))}


def _target_organs(nlp_config: Mapping[str, Any]) -> list[str]:
    return _as_str_list(nlp_config.get("target_organs"))


def _all_organs(nlp_config: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = nlp_config.get("all_organs")
    if isinstance(raw, Mapping):
        return raw
    raw = nlp_config.get("organs")
    return raw if isinstance(raw, Mapping) else {}


def _header_aliases(nlp_config: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = nlp_config.get("header_aliases")
    return raw if isinstance(raw, Mapping) else {}


def _nlp_doc_for_segmentation(text: str) -> Any:
    global _SEG_NLP
    if _SEG_NLP is None:
        try:
            import spacy
        except ImportError:
            return None
        _SEG_NLP = spacy.blank("pt")
        _SEG_NLP.add_pipe("sentencizer")
    return _SEG_NLP(text or "")


def _segment_blocks(
    treated: str,
    nlp_config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    targets = _target_organs(nlp_config)
    if not targets:
        return [{"organ": "", "text": treated}], "full_doc"

    mode = _segmentation_mode(nlp_config)
    force_set = _force_full_doc_targets(nlp_config)
    if force_set and any(t.lower() in force_set for t in targets):
        return [{"organ": targets[0], "text": treated}], "full_doc"
    if mode == "full_doc":
        return [{"organ": targets[0], "text": treated}], "full_doc"

    aliases = _header_aliases(nlp_config)
    if mode in ("auto", "headers_only"):
        header_blocks = segment_by_headers_plain(treated, targets, aliases)
        if header_blocks:
            blocks = [{"organ": str(b.get("organ", targets[0])), "text": str(b.get("text", ""))} for b in header_blocks]
            return blocks, "headers"
        if mode == "headers_only":
            return [{"organ": targets[0], "text": treated}], "full_doc"

    if mode in ("auto", "anchors_only"):
        all_organs = _all_organs(nlp_config)
        subset = {o: all_organs.get(o, {}) for o in targets}
        doc = _nlp_doc_for_segmentation(treated)
        if doc is not None:
            anchor_blocks = segment_by_organs(doc, subset, {})
            if anchor_blocks:
                blocks = [{"organ": str(b.get("organ", targets[0])), "text": str(b.get("text", ""))} for b in anchor_blocks]
                return blocks, "anchors"

    return [{"organ": targets[0], "text": treated}], "full_doc"


class ClinicalNlpEngine:
    """Motor NLP clinico: `to_plain` + rule-based (T02.2) + scoring (T02.3)."""

    def __init__(self, *, engine_version: str | None = None) -> None:
        self._engine_version = engine_version or _package_version()

    def process(
        self,
        rows: Sequence[Mapping[str, Any]],
        nlp_config: Mapping[str, Any],
        *,
        specialty_id: str = "",
        config_version: str = "",
    ) -> list[dict[str, Any]]:
        """Texto limpo, achados via config, score e flag de relevancia."""
        trailing = _trailing_line_patterns(nlp_config)
        use_rules = _rule_engine_enabled(nlp_config)
        out: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()
        for row in rows:
            raw = _raw_laudo_text(row)
            treated = to_plain(raw, trailing_line_patterns=trailing)
            if use_rules:
                blocks, seg_strategy = _segment_blocks(treated, nlp_config)
                agg_summary: list[str] = []
                n_pos = 0
                n_neg = 0
                for block in blocks:
                    block_cfg = dict(nlp_config)
                    organ = str(block.get("organ", "")).strip()
                    if organ:
                        block_cfg["target_organs"] = [organ]
                    rb = process_rule_based(str(block.get("text", "")), block_cfg)
                    agg_summary.extend(rb.get("summary_compact", []))
                    n_pos += int(rb.get("n_positive_spans", 0))
                    n_neg += int(rb.get("n_negated_spans", 0))
                policy = normalize_score_policy(nlp_config.get("score_policy_version"))
                score = confidence_rule_based(
                    n_positive_spans=n_pos,
                    n_negated_spans=n_neg,
                    policy=policy,
                )
                fl = fl_relevante_from_counts(n_pos)
                decision_source = "rule"
                semantic_score = 0.0
                semantic_term = ""
                semantic_backend = ""
                semantic_model = ""
                if embeddings_enabled(dict(nlp_config)):
                    ev = semantic_evidence(treated, dict(nlp_config))
                    semantic_score = _clip01(ev.max_similarity)
                    semantic_term = ev.matched_term
                    semantic_backend = ev.backend_used
                    semantic_model = ev.model_name
                    sim_threshold = effective_similarity_threshold(dict(nlp_config))
                    mode = embeddings_decision_mode(dict(nlp_config))
                    if mode == "fallback":
                        lo, hi = effective_embeddings_ambiguity_band(dict(nlp_config))
                        if (
                            fl == 0
                            and _in_band(score, lo, hi)
                            and semantic_score >= sim_threshold
                        ):
                            fl = 1
                            score = max(score, semantic_score)
                            decision_source = "embedding_fallback"
                    else:
                        wr, ws = embeddings_hybrid_weights(dict(nlp_config))
                        score = confidence_hybrid(
                            rule_score=score,
                            semantic_score=semantic_score,
                            weight_rule=wr,
                            weight_semantic=ws,
                        )
                        if fl == 0 and semantic_score >= sim_threshold:
                            fl = 1
                        decision_source = "hybrid"
                calibrated_score = score
                if _calibrated_hybrid_enabled(nlp_config):
                    calibrated_score = confidence_calibrated_meta(
                        rule_score=score,
                        semantic_score=semantic_score,
                        n_positive_spans=n_pos,
                        n_negated_spans=n_neg,
                        modality=str(row.get("exm_mod", "") or ""),
                    )
                llm_meta: dict[str, Any] = {
                    "llm_router_mode": "deterministic",
                    "llm_called": False,
                    "llm_model": "",
                    "llm_error": "",
                }
                if _llm_router_enabled(nlp_config):
                    fl, decision_source, band_hit, llm_meta = llm_router_step(
                        treated=treated,
                        current_fl=fl,
                        calibrated_score=calibrated_score,
                        nlp_config=nlp_config,
                        specialty_id=specialty_id,
                    )
                else:
                    if _calibrated_hybrid_enabled(nlp_config) and decision_source in ("rule", "hybrid"):
                        decision_source = "hybrid_calibrated"
                    band_hit = False
                score = calibrated_score
                resultado = {
                    "summary_compact": agg_summary,
                    "n_positive_spans": n_pos,
                    "n_negated_spans": n_neg,
                    "rule_engine_version": "t022_v1",
                    "score_policy_version": policy,
                    "segmentation_strategy": seg_strategy,
                    "decision_source": decision_source,
                    "uncertainty_band_hit": bool(band_hit),
                    "semantic_score": round(float(semantic_score), 6),
                    "semantic_matched_term": semantic_term,
                    "semantic_backend": semantic_backend,
                    "embedding_model": semantic_model,
                }
                resultado.update(llm_meta)
            else:
                score = 0.0
                fl = 0
                resultado = {
                    "summary_compact": [],
                    "n_positive_spans": 0,
                    "n_negated_spans": 0,
                    "rule_engine_version": "",
                    "score_policy_version": normalize_score_policy(
                        nlp_config.get("score_policy_version")
                    ),
                    "decision_source": "disabled",
                    "uncertainty_band_hit": False,
                    "semantic_score": 0.0,
                    "semantic_matched_term": "",
                    "semantic_backend": "",
                    "embedding_model": "",
                    "llm_router_mode": "deterministic",
                    "llm_called": False,
                    "llm_model": "",
                    "llm_error": "",
                }
            record: dict[str, Any] = {}
            for key in _INPUT_PASS_THROUGH:
                if key in row and row[key] is not None:
                    record[key] = row[key]
            record["id_predicao"] = str(uuid4())
            record["dt_execucao"] = now
            record["specialty_id"] = specialty_id
            record["config_version"] = config_version
            record["engine_version"] = self._engine_version
            record["fl_relevante"] = fl
            record["confidence_score"] = score
            record["exm_laudo_resultado"] = json.dumps(resultado, ensure_ascii=False)
            record["exm_laudo_texto_tratado"] = treated
            out.append(record)
        return out
