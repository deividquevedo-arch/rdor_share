"""Deteccao de negacao em janela na mesma frase (paridade notebook biliar, S01 T01.3).

Tokens e janela vêm de config injetado (ex.: CONFIG[\"negation\"]); sem lista clinica fixa no codigo.

``start_char`` / ``end_char`` sao offsets sobre a **mesma string** ``sentence`` (0-based, end exclusivo).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

from nlp_engine.text_pipeline.norm import norm

_WS = re.compile(r"\S+")


def _strip_edges(surface: str) -> str:
    return surface.strip().rstrip(".,;:!?)]}\"'")


def tokenize_sentence_norm(sentence: str) -> list[tuple[int, str]]:
    """Tokens nao-espaceiros com (start_char, texto normalizado para match)."""
    out: list[tuple[int, str]] = []
    for m in _WS.finditer(sentence):
        out.append((m.start(), norm(_strip_edges(m.group(0)))))
    return out


def split_negation_patterns(neg_list: Sequence[str]) -> tuple[set[str], list[tuple[str, ...]]]:
    unigrams: set[str] = set()
    phrases: list[tuple[str, ...]] = []
    for n in neg_list or ():
        n_norm = norm(n.strip())
        if not n_norm:
            continue
        parts = tuple(t for t in n_norm.split() if t)
        if not parts:
            continue
        if len(parts) == 1:
            unigrams.add(parts[0])
        else:
            phrases.append(parts)
    return unigrams, phrases


def starts_with_negator(term: str, neg_tokens: Iterable[str]) -> bool:
    """True se a primeira palavra normalizada de ``term`` estiver em ``neg_tokens`` normalizados."""
    neg_set = {norm(x) for x in neg_tokens if x}
    toks = norm(term.strip()).split()
    return bool(toks) and toks[0] in neg_set


def is_negated_in_sentence_plain(
    sentence: str,
    start_char: int,
    end_char: int,
    neg_list: Sequence[str],
    window_tokens: int = 7,
) -> bool:
    """
    Analogo a ``is_negated_in_sentence`` do legado (spaCy), em texto plano.

    Args:
        sentence: Frase onde o trecho ocorre (deve conter [start_char:end_char)).
        neg_list: Lista de negadores (unigrama ou frase); mesma semantica do CONFIG.
        window_tokens: Ultimos N tokens a esquerda e primeiros N a direita (legado usa CONFIG).
    """
    if not sentence or start_char < 0 or end_char <= start_char or end_char > len(sentence):
        return False

    sent_tokens = tokenize_sentence_norm(sentence)
    if not sent_tokens:
        return False

    left = [nw for (idx, nw) in sent_tokens if idx < start_char]
    right = [nw for (idx, nw) in sent_tokens if idx >= end_char]
    if window_tokens and window_tokens > 0:
        left = left[-window_tokens:]
        right = right[:window_tokens]

    neg_unigrams, neg_phrases = split_negation_patterns(list(neg_list))

    def _has_phrase(tokens: list[str], phrase: tuple[str, ...]) -> bool:
        k = len(phrase)
        if k == 0 or len(tokens) < k:
            return False
        for i in range(len(tokens) - k + 1):
            if tuple(tokens[i : i + k]) == phrase:
                return True
        return False

    hit_toks = [nw for (idx, nw) in sent_tokens if idx >= start_char and idx < end_char]
    if hit_toks:
        if hit_toks[0] in neg_unigrams:
            return True
        for ph in neg_phrases:
            if len(hit_toks) >= len(ph) and tuple(hit_toks[: len(ph)]) == ph:
                return True

    if any(t in neg_unigrams for t in left):
        return True
    if any(_has_phrase(left, ph) for ph in neg_phrases):
        return True
    if any(t in neg_unigrams for t in right):
        return True
    if any(_has_phrase(right, ph) for ph in neg_phrases):
        return True
    return False


def is_negated_in_sentence_config(
    sentence: str,
    start_char: int,
    end_char: int,
    negation_cfg: dict,
) -> bool:
    """Atalho: ``negation_cfg`` com chaves ``tokens`` e opcional ``window_tokens``."""
    tokens = negation_cfg.get("tokens") or ()
    window = int(negation_cfg.get("window_tokens", 7))
    return is_negated_in_sentence_plain(sentence, start_char, end_char, tokens, window)
