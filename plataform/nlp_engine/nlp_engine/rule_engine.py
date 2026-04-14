"""Motor rule-based minimal (S02 T02.2): achados por frase, orgao-alvo e negacao via config.

Paridade de ideia com Grupo 1b: sentencas spaCy, termos em ``findings``, janela de negacao,
filtro por ``target_organs`` + ``organs`` (seeds/regex). Sem embeddings; Matcher spaCy
pode ser acrescentado como optimizacao.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from nlp_engine.text_pipeline.anchors import sentence_mentions_organ
from nlp_engine.text_pipeline.negation import is_negated_in_sentence_plain
from nlp_engine.text_pipeline.norm import norm

_WS_WORD = re.compile(r"\S+")
_NLP = None


def _strip_edges(surface: str) -> str:
    return surface.strip().rstrip(".,;:!?)]}\"'")


def _nlp_doc(text: str) -> Any:
    global _NLP
    if _NLP is None:
        import spacy

        _NLP = spacy.blank("pt")
        _NLP.add_pipe("sentencizer")
    return _NLP(text)


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
        oc = organs_map.get(org)
        cfg: Mapping[str, Any] = oc if isinstance(oc, Mapping) else {}
        if sentence_mentions_organ(sentence, org, cfg, None):
            return True
    return False


def _collect_finding_hits(
    sentence: str,
    findings: Mapping[str, Any],
    targets: tuple[str, ...],
    organs_map: Mapping[str, Any],
    neg_list: list[str],
    window: int,
) -> tuple[list[str], int, int]:
    summary: list[str] = []
    n_clear = 0
    n_neg = 0
    seen: set[tuple[str, int, int]] = set()
    for cat, terms in findings.items():
        if not isinstance(terms, (list, tuple)):
            continue
        for phrase in terms:
            p = str(phrase).strip()
            if not p:
                continue
            for start, end in _iter_phrase_spans(sentence, p):
                key = (str(cat), start, end)
                if key in seen:
                    continue
                seen.add(key)
                if not _mentions_target_organ(sentence, targets, organs_map):
                    continue
                if is_negated_in_sentence_plain(sentence, start, end, neg_list, window):
                    n_neg += 1
                    continue
                n_clear += 1
                summary.append(f"{cat}: {sentence[start:end].strip()}")
    return summary, n_clear, n_neg


def process_rule_based(text: str, nlp_config: Mapping[str, Any]) -> dict[str, Any]:
    """Extrai achados nao-negados; respeita ``target_organs`` quando definidos.

    Args:
        text: Saida de ``to_plain`` (ou equivalente).
        nlp_config: sub-arvore ``nlp`` — ``findings``, ``target_organs``, ``organs``,
            ``negation_phrases`` (ou ``negation_expressions``), ``negation_window``.
    """
    raw_findings = nlp_config.get("findings") or {}
    findings: Mapping[str, Any] = raw_findings if isinstance(raw_findings, Mapping) else {}
    targets = tuple(str(x) for x in (nlp_config.get("target_organs") or ()) if x)
    organs_raw = nlp_config.get("organs") or {}
    organs_map: Mapping[str, Any] = organs_raw if isinstance(organs_raw, Mapping) else {}
    neg_list = _negation_list(nlp_config)
    window = int(nlp_config.get("negation_window", 7))

    doc = _nlp_doc(text or "")
    summary_compact: list[str] = []
    n_clear = 0
    n_neg = 0
    for sent in doc.sents:
        st = sent.text
        if not st.strip():
            continue
        part_s, c_clear, c_neg = _collect_finding_hits(
            st, findings, targets, organs_map, neg_list, window
        )
        summary_compact.extend(part_s)
        n_clear += c_clear
        n_neg += c_neg

    return {
        "summary_compact": summary_compact,
        "n_positive_spans": n_clear,
        "n_negated_spans": n_neg,
    }
