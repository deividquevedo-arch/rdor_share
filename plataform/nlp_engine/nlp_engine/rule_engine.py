"""Motor rule-based S02 T02.2: Matcher spaCy, regex configuravel, proximidade orgao-achado."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from spacy.matcher import Matcher

from nlp_engine.text_pipeline.accent_pattern import token_accent_regex
from nlp_engine.text_pipeline.anchors import sentence_mentions_organ
from nlp_engine.text_pipeline.negation import is_negated_in_sentence_plain
from nlp_engine.text_pipeline.norm import norm

RULE_ENGINE_VERSION = "t022_v1"

_WS_WORD = re.compile(r"\S+")
_NLP = None


def _strip_edges(surface: str) -> str:
    return surface.strip().rstrip(".,;:!?)]}\"'")


def _nlp_singleton() -> Any:
    global _NLP
    if _NLP is None:
        import spacy

        _NLP = spacy.blank("pt")
        _NLP.add_pipe("sentencizer")
    return _NLP


def _nlp_doc(text: str) -> Any:
    return _nlp_singleton()(text)


def _word_spans(sentence: str) -> list[tuple[int, int, str]]:
    out: list[tuple[int, int, str]] = []
    for m in _WS_WORD.finditer(sentence):
        raw = m.group()
        out.append((m.start(), m.end(), norm(_strip_edges(raw))))
    return out


def _iter_phrase_spans(sentence: str, phrase: str) -> list[tuple[int, int]]:
    parts = [p for p in norm(phrase).split() if p]
    if not parts:
        return []
    words = _word_spans(sentence)
    n = len(parts)
    spans: list[tuple[int, int]] = []
    for i in range(len(words) - n + 1):
        if [words[i + j][2] for j in range(n)] == parts:
            spans.append((words[i][0], words[i + n - 1][1]))
    return spans


def _span_gap(a0: int, a1: int, b0: int, b1: int) -> int:
    if a1 <= b0:
        return b0 - a1
    if b1 <= a0:
        return a0 - b1
    return 0


def _organ_spans(sentence: str, targets: tuple[str, ...], organs_map: Mapping[str, Any]) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    for org in targets:
        raw_cfg = organs_map.get(org)
        cfg: Mapping[str, Any] = raw_cfg if isinstance(raw_cfg, Mapping) else {}
        seeds = (org,) + tuple(str(s) for s in (cfg.get("seeds") or ()) if s)
        for s in seeds:
            spans.extend(_iter_phrase_spans(sentence, str(s)))
    return spans


def _min_gap_finding_to_organs(
    sentence: str,
    f0: int,
    f1: int,
    targets: tuple[str, ...],
    organs_map: Mapping[str, Any],
) -> int | None:
    if not targets:
        return 0
    o_spans = _organ_spans(sentence, targets, organs_map)
    if not o_spans:
        return None
    return min(_span_gap(f0, f1, os0, os1) for os0, os1 in o_spans)


def _negation_list(cfg: Mapping[str, Any]) -> list[str]:
    raw = cfg.get("negation_phrases")
    if raw is None:
        raw = cfg.get("negation_expressions")
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x) for x in raw if x]
    return []


def _mentions_target_organ(
    sentence: str,
    targets: tuple[str, ...],
    organs_map: Mapping[str, Any],
) -> bool:
    if not targets:
        return True
    for org in targets:
        raw_cfg = organs_map.get(org)
        cfg: Mapping[str, Any] = raw_cfg if isinstance(raw_cfg, Mapping) else {}
        if sentence_mentions_organ(sentence, org, cfg, None):
            return True
    return False


def _use_matcher(cfg: Mapping[str, Any]) -> bool:
    v = cfg.get("use_spacy_matcher")
    if v is None:
        return True
    return str(v).lower() in ("1", "true", "yes")


def _spans_from_norm_findings(sentence: str, findings: Mapping[str, Any]) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    for cat, terms in findings.items():
        if not isinstance(terms, (list, tuple)):
            continue
        for phrase in terms:
            p = str(phrase).strip()
            if not p:
                continue
            for start, end in _iter_phrase_spans(sentence, p):
                out.append((str(cat), start, end))
    return out


def _spans_from_regex_findings(
    sentence: str, findings_regex: Mapping[str, Any] | None
) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    if not findings_regex or not isinstance(findings_regex, Mapping):
        return out
    for cat, pats in findings_regex.items():
        if not isinstance(pats, (list, tuple)):
            continue
        for pat in pats:
            ps = str(pat).strip()
            if not ps:
                continue
            try:
                for m in re.finditer(ps, sentence, re.IGNORECASE):
                    out.append((str(cat), m.start(), m.end()))
            except re.error:
                continue
    return out


def _spans_from_matcher(sentence: str, nlp: Any, findings: Mapping[str, Any]) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    doc = nlp(sentence)
    matcher = Matcher(nlp.vocab)

    rid = 0
    for cat, terms in findings.items():
        if not isinstance(terms, (list, tuple)):
            continue
        for phrase in terms:
            p = str(phrase).strip()
            if not p:
                continue
            words = [w for w in norm(p).split() if w]
            if not words:
                continue
            pat = [{"TEXT": {"REGEX": token_accent_regex(w)}} for w in words]
            rule_name = f"{cat}\t{rid}"
            matcher.add(rule_name, [pat])
            rid += 1

    matches = matcher(doc)
    for match_id, start, end in matches:
        rule_name = nlp.vocab.strings[match_id]
        cat = str(rule_name).split("\t", 1)[0]
        span = doc[start:end]
        out.append((cat, span.start_char, span.end_char))

    return out


def _dedupe_spans(raw: list[tuple[str, int, int]]) -> list[tuple[str, int, int]]:
    seen: set[tuple[str, int, int]] = set()
    out: list[tuple[str, int, int]] = []
    for t in raw:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _max_chars(cfg: Mapping[str, Any]) -> int | None:
    raw = cfg.get("finding_organ_max_chars")
    if raw is None:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def _passes_proximity(
    sentence: str,
    start: int,
    end: int,
    targets: tuple[str, ...],
    organs_map: Mapping[str, Any],
    max_chars: int | None,
) -> bool:
    if max_chars is None:
        return True
    if not targets:
        return True
    gap = _min_gap_finding_to_organs(sentence, start, end, targets, organs_map)
    if gap is None:
        return False
    return gap <= max_chars


def _evaluate_spans(
    sentence: str,
    spans: list[tuple[str, int, int]],
    targets: tuple[str, ...],
    organs_map: Mapping[str, Any],
    neg_list: list[str],
    window: int,
    max_chars: int | None,
) -> tuple[list[str], int, int]:
    summary: list[str] = []
    n_clear = 0
    n_neg = 0
    for cat, start, end in spans:
        if not _mentions_target_organ(sentence, targets, organs_map):
            continue
        if not _passes_proximity(sentence, start, end, targets, organs_map, max_chars):
            continue
        if is_negated_in_sentence_plain(sentence, start, end, neg_list, window):
            n_neg += 1
            continue
        n_clear += 1
        summary.append(f"{cat}: {sentence[start:end].strip()}")
    return summary, n_clear, n_neg


def process_rule_based(text: str, nlp_config: Mapping[str, Any]) -> dict[str, Any]:
    """Agrega achados por sentenca; ver SPEC em docs/motor-nlp/spec-rule-engine-t022-v0.md."""
    raw_findings = nlp_config.get("findings") or {}
    findings: Mapping[str, Any] = raw_findings if isinstance(raw_findings, Mapping) else {}
    findings_regex = nlp_config.get("findings_regex")
    targets = tuple(str(x) for x in (nlp_config.get("target_organs") or ()) if x)
    organs_raw = nlp_config.get("organs") or {}
    organs_map: Mapping[str, Any] = organs_raw if isinstance(organs_raw, Mapping) else {}
    neg_list = _negation_list(nlp_config)
    window = int(nlp_config.get("negation_window", 7))
    max_chars = _max_chars(nlp_config)
    nlp = _nlp_singleton()

    doc = _nlp_doc(text or "")
    summary_compact: list[str] = []
    n_clear = 0
    n_neg = 0
    for sent in doc.sents:
        st = sent.text
        if not st.strip():
            continue
        merged: list[tuple[str, int, int]] = []
        merged.extend(_spans_from_norm_findings(st, findings))
        merged.extend(_spans_from_regex_findings(st, findings_regex if isinstance(findings_regex, Mapping) else None))
        if _use_matcher(nlp_config) and findings:
            merged.extend(_spans_from_matcher(st, nlp, findings))
        merged = _dedupe_spans(merged)
        part_s, c_clear, c_neg = _evaluate_spans(
            st, merged, targets, organs_map, neg_list, window, max_chars
        )
        summary_compact.extend(part_s)
        n_clear += c_clear
        n_neg += c_neg

    return {
        "summary_compact": summary_compact,
        "n_positive_spans": n_clear,
        "n_negated_spans": n_neg,
        "rule_engine_version": RULE_ENGINE_VERSION,
    }
