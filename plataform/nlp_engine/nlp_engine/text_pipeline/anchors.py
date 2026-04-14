"""Ancoragem por orgao em Doc spaCy (S01 T01.2, paridade DII/colon legado).

SPEC:
- Entrada: ``doc`` (spaCy ``Doc`` com ``doc.sents``), ``organ_name``, ``organ_cfg`` dict com
  ``seeds`` (lista de str) e opcional ``regex`` (lista de padroes ``re`` sobre texto
  normalizado), ``organ_lexicon`` (termos extra do texto, lexical-only).
- Saida: ``organ_anchors`` -> lista de ``(start_token, end_token)`` por sentenca que casa;
  ``segment_by_organs`` -> blocos ``dict`` com orgao, indices de token, texto e offsets de char.
- Normalizacao de match: mesma ``norm`` que o resto do text_pipeline (acentos, lower, espacos).
- Nao faz: embeddings semanticos; negacao (usar ``negation.py``); cabecalhos (``by_headers``).
- Requer: pacote opcional ``spacy`` (``pip install nlp-engine[spacy]``).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from nlp_engine.text_pipeline.norm import norm

_ORGAN_CFG_SEEDS = "seeds"
_ORGAN_CFG_REGEX = "regex"


def _require_spacy_doc(doc: Any) -> None:
    if doc is None or not hasattr(doc, "sents"):
        raise TypeError("esperado spacy.tokens.Doc com atributo sents")


def organ_term_set(
    organ_name: str,
    organ_cfg: Mapping[str, Any],
    organ_lexicon: Sequence[str] | None,
) -> set[str]:
    """Universo de termos normalizados: nome do orgao, seeds e lexicon."""
    seeds = organ_cfg.get(_ORGAN_CFG_SEEDS) or []
    lex = organ_lexicon or []
    terms: set[str] = {norm(organ_name)}
    terms.update(norm(x) for x in seeds if x)
    terms.update(norm(x) for x in lex if x)
    terms.discard("")
    return terms


def sentence_mentions_organ(
    sentence_text: str,
    organ_name: str,
    organ_cfg: Mapping[str, Any],
    organ_lexicon: Sequence[str] | None = None,
) -> bool:
    """True se o texto da sentenca (plain) menciona o orgao por termo com word-boundary ou regex."""
    s_norm = norm(sentence_text)
    terms = organ_term_set(organ_name, organ_cfg, organ_lexicon)
    if _match_terms_norm(s_norm, terms):
        return True
    return _match_regex_norm(s_norm, organ_cfg.get(_ORGAN_CFG_REGEX) or [])


def _match_terms_norm(s_norm: str, terms: set[str]) -> bool:
    for t in terms:
        pattern = r"\b" + re.escape(t) + r"\b"
        if re.search(pattern, s_norm):
            return True
    return False


def _match_regex_norm(s_norm: str, patterns: Sequence[str]) -> bool:
    for rx in patterns:
        if rx and re.search(rx, s_norm):
            return True
    return False


def organ_anchors(
    doc: Any,
    organ_name: str,
    organ_cfg: Mapping[str, Any],
    organ_lexicon: Sequence[str] | None = None,
) -> list[tuple[int, int]]:
    """Lista de (start_token, end_token) por sentenca com evidencia do orgao (legado DII/colon)."""
    _require_spacy_doc(doc)
    terms = organ_term_set(organ_name, organ_cfg, organ_lexicon)
    regex_list = list(organ_cfg.get(_ORGAN_CFG_REGEX) or [])
    spans: list[tuple[int, int]] = []
    for sent in doc.sents:
        s_norm = norm(sent.text)
        hit = _match_terms_norm(s_norm, terms) or _match_regex_norm(s_norm, regex_list)
        if hit:
            spans.append((sent.start, sent.end))
    return spans


def segment_by_organs(
    doc: Any,
    organs_cfg: Mapping[str, Mapping[str, Any]],
    organ_lexicons: Mapping[str, Sequence[str]] | None = None,
) -> list[dict[str, Any]]:
    """Blocos por orgao; cada item tem organ, start_token, end_token, text, start_char, end_char."""
    _require_spacy_doc(doc)
    lex = organ_lexicons or {}
    blocks: list[dict[str, Any]] = []
    for organ, cfg in organs_cfg.items():
        for a, b in organ_anchors(doc, organ, cfg, lex.get(organ, ())):
            span = doc[a:b]
            blocks.append(
                {
                    "organ": organ,
                    "start_token": a,
                    "end_token": b,
                    "text": span.text,
                    "start_char": span.start_char,
                    "end_char": span.end_char,
                }
            )
    return blocks
