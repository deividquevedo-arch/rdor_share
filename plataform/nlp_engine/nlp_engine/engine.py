"""ClinicalNlpEngine — interface do motor clinico (S02 T02.1+, Fase 1 rule-based)."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from importlib import metadata
from typing import Any
from uuid import uuid4

from nlp_engine.rule_engine import process_rule_based
from nlp_engine.scoring import confidence_rule_based, fl_relevante_from_counts
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
                rb = process_rule_based(treated, nlp_config)
                n_pos = int(rb["n_positive_spans"])
                n_neg = int(rb["n_negated_spans"])
                score = confidence_rule_based(n_positive_spans=n_pos, n_negated_spans=n_neg)
                fl = fl_relevante_from_counts(n_pos)
                resultado = {
                    "summary_compact": rb["summary_compact"],
                    "n_positive_spans": n_pos,
                    "n_negated_spans": n_neg,
                }
            else:
                score = 0.0
                fl = 0
                resultado = {"summary_compact": [], "n_positive_spans": 0, "n_negated_spans": 0}
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
