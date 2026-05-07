from __future__ import annotations

import json
from pathlib import Path

from scripts.fn_deep_dive_hepatologia import run_cluster_explore, run_deep_dive, triage_biliary_fn_text


def test_run_deep_dive_counts_fn_clusters(tmp_path: Path) -> None:
    audit_csv = tmp_path / "audit.csv"
    audit_csv.write_text(
        "id_exame;fl_relevante\n"
        "A1;0\n"
        "A2;0\n"
        "A3;1\n",
        encoding="utf-8-sig",
    )
    expected_csv = tmp_path / "expected.csv"
    expected_csv.write_text(
        "id_exame;expected_encaminhamento\n"
        "A1;S\n"
        "A2;S\n"
        "A3;N\n",
        encoding="utf-8-sig",
    )

    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "id_exame;exm_laudo_texto\n"
        "A1;ultrassonografia abdome total com colelitiase.\n"
        "A2;tomografia com sinais hepatopatia cronica.\n"
        "A3;ressonancia sem achado.\n",
        encoding="utf-8-sig",
    )
    out_json = tmp_path / "deep.json"
    result = run_deep_dive(
        audit_csv=audit_csv,
        expected_csv=expected_csv,
        input_csv=input_csv,
        out_json=out_json,
        top_n=10,
    )
    assert result["n_joined"] == 3
    assert result["fn_rows_analyzed"] == 2
    assert result["exam_bucket_distribution"].get("US") == 1
    assert result["exam_bucket_distribution"].get("TC") == 1
    assert result["cluster_distribution"].get("biliar_litiase") == 1
    assert result["cluster_distribution"].get("hepatopatia_cronica") == 1
    assert result["biliar_fn_in_scope"] == 1
    assert result["biliar_triage_distribution"].get("likely_true_miss") == 1
    assert "likely_true_miss" in result["biliar_ids_by_triage"]
    assert result["biliar_ids_by_triage"]["likely_true_miss"] == ["A1"]
    assert out_json.is_file()


def test_triage_biliary_fn_text_likely_miss() -> None:
    assert triage_biliary_fn_text("US abdome: colelitíase na vesícula.") == "likely_true_miss"


def test_triage_biliary_fn_text_negated() -> None:
    assert triage_biliary_fn_text("Vesícula biliar sem cálculos.") == "noise_negated_stones"


def test_triage_biliary_fn_text_elastography_calculado() -> None:
    t = (
        "Vesícula biliar. Elastografia hepatica: valor calculado 6 kPa no segmento VI."
    )
    assert triage_biliary_fn_text(t) == "noise_elastography_calculado"


def test_triage_biliary_fn_text_not_cluster() -> None:
    assert triage_biliary_fn_text("Figado com esteatose.") == "not_biliar_cluster"


def test_triage_biliary_fn_text_typo_vesiculs_multicalculus() -> None:
    """Grafia ``vesiculs biliar`` em exports Diamond (sem acento)."""
    t = "vesiculs biliar multiplos calculos paredes finas"
    assert triage_biliary_fn_text(t) == "likely_true_miss"


def test_run_deep_dive_comma_delimiter_and_proced_column(tmp_path: Path) -> None:
    audit_csv = tmp_path / "audit.csv"
    audit_csv.write_text("id_exame,fl_relevante\nX1,0\n", encoding="utf-8-sig")
    expected_csv = tmp_path / "expected.csv"
    expected_csv.write_text("id_exame,expected_encaminhamento\nX1,S\n", encoding="utf-8-sig")
    input_csv = tmp_path / "in.csv"
    input_csv.write_text(
        "id_exame,proced_laudo_exame\n"
        'X1,"tomografia. Vesícula biliar com colelitíase."\n',
        encoding="utf-8-sig",
    )
    r = run_deep_dive(
        audit_csv=audit_csv,
        expected_csv=expected_csv,
        input_csv=input_csv,
        top_n=50,
        csv_delimiter=",",
        input_text_column="proced_laudo_exame",
    )
    assert r["fn_rows_analyzed"] == 1
    assert r["biliar_triage_distribution"].get("likely_true_miss") == 1


def test_run_cluster_explore_counts_all_rows(tmp_path: Path) -> None:
    p = tmp_path / "diamond_like.csv"
    p.write_text(
        "id_exame,proced_laudo_exame\n"
        'R1,"tomografia. Vesícula biliar sem cálculos."\n'
        'R2,"vesiculs biliar multiplos calculos."\n',
        encoding="utf-8-sig",
    )
    r = run_cluster_explore(
        input_csv=p,
        csv_delimiter=",",
        input_text_column="proced_laudo_exame",
    )
    assert r["mode"] == "explore_only"
    assert r["rows_scored"] == 2
    assert r["biliar_fn_in_scope"] == 2
    assert r["biliar_triage_distribution"].get("noise_negated_stones") == 1
    assert r["biliar_triage_distribution"].get("likely_true_miss") == 1

