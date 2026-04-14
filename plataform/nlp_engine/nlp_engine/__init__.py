"""Pacote nlp_engine — motor NLP (Fase 1: rule-based)."""

from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.text_pipeline import (
    drop_boilerplate_lines,
    extract_section_lines,
    is_negated_in_sentence_config,
    is_negated_in_sentence_plain,
    organ_anchors,
    organ_term_set,
    segment_by_headers_plain,
    segment_by_organs,
    sentence_mentions_organ,
    starts_with_negator,
    strip_trailing_line_patterns,
    to_plain,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "ClinicalNlpEngine",
    "drop_boilerplate_lines",
    "extract_section_lines",
    "is_negated_in_sentence_config",
    "is_negated_in_sentence_plain",
    "organ_anchors",
    "organ_term_set",
    "segment_by_headers_plain",
    "segment_by_organs",
    "sentence_mentions_organ",
    "starts_with_negator",
    "strip_trailing_line_patterns",
    "to_plain",
]
