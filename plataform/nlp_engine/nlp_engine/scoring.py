"""Score continuo 0.0–1.0 (S02 T02.3), politicas versionadas via config."""

from __future__ import annotations

SCORE_POLICY_V1_BINS = "v1_bins_legacy"
SCORE_POLICY_V2_DENSITY = "v2_density"


def confidence_rule_based(
    *,
    n_positive_spans: int,
    n_negated_spans: int,
    policy: str = SCORE_POLICY_V1_BINS,
) -> float:
    """Politicas explicitas; default alinhada ao Grupo 1b (0.9 / 0.35 / 0.0)."""
    if policy == SCORE_POLICY_V2_DENSITY:
        if n_positive_spans > 0:
            return min(1.0, 0.52 + 0.09 * min(n_positive_spans, 6))
        return 0.35 if n_negated_spans > 0 else 0.0
    if n_positive_spans > 0:
        return 0.9
    if n_negated_spans > 0:
        return 0.35
    return 0.0


def fl_relevante_from_counts(n_positive_spans: int) -> int:
    """1 se existir pelo menos um span de achado nao-negado."""
    return 1 if n_positive_spans > 0 else 0


def confidence_hybrid(
    *,
    rule_score: float,
    semantic_score: float,
    weight_rule: float,
    weight_semantic: float,
) -> float:
    """Combina score rule-based e score semantico em [0, 1]."""
    wr = max(0.0, float(weight_rule))
    ws = max(0.0, float(weight_semantic))
    den = wr + ws
    if den <= 0.0:
        return max(0.0, min(1.0, float(rule_score)))
    score = (wr * float(rule_score) + ws * float(semantic_score)) / den
    return max(0.0, min(1.0, score))


def confidence_calibrated_meta(
    *,
    rule_score: float,
    semantic_score: float,
    n_positive_spans: int,
    n_negated_spans: int,
    modality: str = "",
) -> float:
    """Meta-calibracao leve e deterministica para reduzir flutuacao inter-lotes.

    A funcao evita dependencias externas e fornece comportamento estavel:
    - ancora em ``rule_score`` e ``semantic_score``
    - bonifica evidencia positiva
    - penaliza negacao e modalidades tipicamente mais ruidosas
    """
    rs = max(0.0, min(1.0, float(rule_score)))
    ss = max(0.0, min(1.0, float(semantic_score)))
    score = 0.62 * rs + 0.38 * ss
    score += 0.03 * min(max(int(n_positive_spans), 0), 4)
    score -= 0.02 * min(max(int(n_negated_spans), 0), 4)
    m = (modality or "").strip().upper()
    if m in ("US", "ULTRASSONOGRAFIA"):
        score -= 0.01
    elif m in ("RM", "RESSONANCIA", "RESSONÂNCIA"):
        score += 0.005
    return max(0.0, min(1.0, score))


def normalize_score_policy(raw: str | None) -> str:
    """Normaliza valor vindo do YAML; desconhecido cai no v1."""
    if not raw:
        return SCORE_POLICY_V1_BINS
    s = str(raw).strip()
    if s == SCORE_POLICY_V2_DENSITY:
        return SCORE_POLICY_V2_DENSITY
    return SCORE_POLICY_V1_BINS
