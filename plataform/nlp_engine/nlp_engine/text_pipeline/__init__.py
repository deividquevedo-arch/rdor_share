"""TextPipeline: conversao RTF/HTML/plain -> texto limpo (paridade Grupo 1b / biliar)."""

from nlp_engine.text_pipeline.anchors import (
    organ_anchors,
    organ_term_set,
    segment_by_organs,
    sentence_mentions_organ,
)
from nlp_engine.text_pipeline.boilerplate import (
    drop_boilerplate_lines,
    strip_trailing_line_patterns,
)
from nlp_engine.text_pipeline.by_headers import (
    extract_section_lines,
    segment_by_headers_plain,
)
from nlp_engine.text_pipeline.negation import (
    is_negated_in_sentence_config,
    is_negated_in_sentence_plain,
    starts_with_negator,
)
from nlp_engine.text_pipeline.to_plain import to_plain

__all__ = [
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
