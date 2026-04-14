"""Score continuo 0.0–1.0 (S02 T02.3), parametrizado via resultado rule-based."""

from __future__ import annotations


def confidence_rule_based(*, n_positive_spans: int, n_negated_spans: int) -> float:
    """Heuristica alinhada ao Grupo 1b: 0.9 com achado nao-negado; 0.35 so com evidencia negada."""
    if n_positive_spans > 0:
        return 0.9
    if n_negated_spans > 0:
        return 0.35
    return 0.0


def fl_relevante_from_counts(n_positive_spans: int) -> int:
    """1 se existir pelo menos um span de achado nao-negado."""
    return 1 if n_positive_spans > 0 else 0
