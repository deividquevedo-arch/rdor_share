"""Pareamento amostra legado vs audit (sem PHI)."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from scripts.build_pulmao_standard_sample import run_build
from scripts.compare_pulmao_audit_vs_legacy import _read_rows_pulmao, run_compare


def test_build_unique_id_exame_when_id_pct_repeated(tmp_path: Path) -> None:
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=["id_pct", "exm_an", "exm_mod", "exm_tipo", "exm_data", "exm_laudo_texto", "exm_encaminhamento_nlp", "exm_class", "exm_frase_selec"],
    )
    w.writeheader()
    w.writerow(
        {
            "id_pct": "P1",
            "exm_an": "A1",
            "exm_mod": "m",
            "exm_tipo": "t",
            "exm_data": "d",
            "exm_laudo_texto": "s1",
            "exm_encaminhamento_nlp": "N",
            "exm_class": "a",
            "exm_frase_selec": "",
        }
    )
    w.writerow(
        {
            "id_pct": "P1",
            "exm_an": "A2",
            "exm_mod": "m",
            "exm_tipo": "t",
            "exm_data": "d",
            "exm_laudo_texto": "s2",
            "exm_encaminhamento_nlp": "S",
            "exm_class": "b",
            "exm_frase_selec": "",
        }
    )
    w.writerow(
        {
            "id_pct": "P2",
            "exm_an": "A3",
            "exm_mod": "m",
            "exm_tipo": "t",
            "exm_data": "d",
            "exm_laudo_texto": "s3",
            "exm_encaminhamento_nlp": "N",
            "exm_class": "c",
            "exm_frase_selec": "",
        }
    )
    p = tmp_path / "saida.csv"
    p.write_text(buf.getvalue(), encoding="utf-8")

    summary = run_build(source_legacy_csv=p, out_dir=tmp_path, max_rows=None)
    assert summary["sample_rows"] == 3

    inputs = (tmp_path / "pulmao_standard_input.csv").read_text(encoding="utf-8-sig")
    ids = [line.split(";")[0] for line in inputs.strip().splitlines()[1:]]
    assert len(ids) == len(set(ids)), f"chaves unicas: {ids}"


def test_compare_uses_id_exame_and_expected_encaminhamento(tmp_path: Path) -> None:
    e = tmp_path / "expected.csv"
    e.write_text(
        "id_exame;id_pct;expected_encaminhamento;legacy_class;legacy_frase_selec\n"
        "E1;P1;S;X;f1\n"
        "E2;P2;N;Y;f2\n",
        encoding="utf-8-sig",
    )
    a = tmp_path / "audit.csv"
    a.write_text(
        "id_exame;fl_relevante;confidence_score;exm_laudo_texto_tratado;summary_compact_json;"
        "n_positive_spans;n_negated_spans;rule_engine_version;score_policy_version;exm_laudo_resultado\n"
        "E1;0;0;;[];0;0;;;{}\n"
        "E2;0;0;;[];0;0;;;{}\n",
        encoding="utf-8-sig",
    )
    res = run_compare(audit_csv=a, legacy_csv=e, out_json=None)
    assert res["n_joined"] == 2
    assert res["n_match"] == 1
    assert res["n_mismatch"] == 1


def test_read_pulmao_csv_semico_depois_de_json_com_virgulas(tmp_path: Path) -> None:
    """Regressao: Sniffer com `,` partia colunas se o JSON tiver muitas virgulas."""
    e = tmp_path / "expected.csv"
    e.write_text("id_exame;expected_encaminhamento\nK1;S\n", encoding="utf-8-sig")
    a = tmp_path / "audit.csv"
    a.write_text(
        "id_exame;fl_relevante;exm_laudo_resultado\n"
        'K1;0;"{""a"":1,""b"":2,""c"":3,""d"":4}"\n',
        encoding="utf-8-sig",
    )
    aud = _read_rows_pulmao(a)
    assert aud[0].get("id_exame") == "K1"
    res = run_compare(audit_csv=a, legacy_csv=e, out_json=None)
    assert res["n_joined"] == 1
    assert res["status"] == "ok"
