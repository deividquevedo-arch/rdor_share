"""Métricas gold 1/2/3 no pareamento audit vs legacy (sem PHI)."""

from __future__ import annotations

from pathlib import Path

from scripts.run_hepatologia_strategy_matrix import _metrics_by_cod_123


def test_metrics_by_cod_123_per_class_and_aggregate(tmp_path: Path) -> None:
    leg = tmp_path / "legacy.csv"
    leg.write_text(
        "id_exame;cod_achado_relevante\n"
        "E1;1 - positivo\n"
        "E2;2 - outro\n"
        "E3;3 - negativo\n"
        "E4;\n",
        encoding="utf-8-sig",
    )
    aud = tmp_path / "audit.csv"
    aud.write_text(
        "id_exame;fl_relevante\n"
        "E1;1\n"
        "E2;0\n"
        "E3;0\n"
        "E4;1\n",
        encoding="utf-8-sig",
    )
    out = _metrics_by_cod_123(aud, leg)
    assert out["n_labeled_123"] == 3
    assert out["cod_counts"] == {"1": 1, "2": 1, "3": 1}
    assert out["by_cod"]["1"]["correct"] == 1 and out["by_cod"]["1"]["wrong"] == 0
    assert out["by_cod"]["2"]["wrong"] == 1
    assert out["by_cod"]["3"]["correct"] == 1
    agg = out["aggregate_sn_on_labeled"]
    assert agg["tp"] == 1 and agg["fn"] == 1 and agg["tn"] == 1 and agg["fp"] == 0
