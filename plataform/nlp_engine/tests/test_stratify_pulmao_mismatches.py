"""stratify_pulmao_mismatches: agregados sem PHI."""

from __future__ import annotations

from pathlib import Path

from scripts.stratify_pulmao_mismatches import run_stratify


def test_run_stratify_fn_fp_counts(tmp_path: Path) -> None:
    buck = tmp_path / "b.csv"
    buck.write_text(
        "id_exame;bucket;legacy_flag;motor_flag\n"
        "X1;lexico_pulmao_ou_gate_regra;S;N\n"
        "X2;contexto_comparativo_sem_achado_lexical_obvio;S;N\n"
        "X3;fp_outro;N;S\n",
        encoding="utf-8-sig",
    )
    exp = tmp_path / "e.csv"
    exp.write_text(
        "id_exame;legacy_class;expected_encaminhamento\n"
        "X1;A01 //-// A;S\n"
        "X2;B;S\n"
        "X3;C;N\n",
        encoding="utf-8-sig",
    )
    inp = tmp_path / "i.csv"
    inp.write_text(
        "id_exame;exm_laudo_texto\n"
        "X1;TOMOGRAFIA COMPUTADORIZADA DO TÓRAX com nodulo.\n"
        "X2;TOMOGRAFIA DO ABDOME E DA PELVE sem achado.\n"
        "X3;TOMOGRAFIA DO TÓRAX normal.\n",
        encoding="utf-8-sig",
    )
    r = run_stratify(buckets_csv=buck, expected_csv=exp, input_csv=inp)
    assert r["n_fn"] == 2
    assert r["n_fp"] == 1
    assert r["fn_by_bucket"].get("lexico_pulmao_ou_gate_regra") == 1
