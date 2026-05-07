"""Pareamento amostra legado vs audit Hepatologia (sem PHI)."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from scripts.audit_legacy_compare import read_rows_semico_first, run_compare_audit_vs_legacy
from scripts.build_hepatologia_standard_sample import read_diamond_hepatologia, run_build
from scripts.compare_hepatologia_audit_vs_legacy import run_compare


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

    summary = run_build(
        source_legacy_csv=p, out_dir=tmp_path, max_rows=None, source="diamond"
    )
    assert summary["sample_rows"] == 3

    inputs = (tmp_path / "hepatologia_standard_input.csv").read_text(encoding="utf-8-sig")
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
    g = run_compare_audit_vs_legacy(audit_csv=a, legacy_csv=e, out_json=None)
    assert g == res


def test_compare_prefers_id_predicao_when_id_exame_is_duplicated(tmp_path: Path) -> None:
    e = tmp_path / "expected.csv"
    e.write_text(
        "id_predicao;id_exame;expected_encaminhamento\n"
        "P1;EX1;S\n"
        "P2;EX1;N\n",
        encoding="utf-8-sig",
    )
    a = tmp_path / "audit.csv"
    a.write_text(
        "id_exame;id_predicao;fl_relevante;confidence_score;exm_laudo_texto_tratado;summary_compact_json;"
        "n_positive_spans;n_negated_spans;rule_engine_version;score_policy_version;exm_laudo_resultado\n"
        "EX1;P1;1;0.8;;[];1;0;;;{}\n"
        "EX1;P2;0;0.2;;[];0;0;;;{}\n",
        encoding="utf-8-sig",
    )
    res = run_compare_audit_vs_legacy(audit_csv=a, legacy_csv=e, out_json=None)
    assert res["n_joined"] == 2
    assert res["n_mismatch"] == 0
    assert res["match_rate"] == 1.0


def test_read_csv_semico_depois_de_json_com_virgulas(tmp_path: Path) -> None:
    e = tmp_path / "expected.csv"
    e.write_text("id_exame;expected_encaminhamento\nK1;S\n", encoding="utf-8-sig")
    a = tmp_path / "audit.csv"
    a.write_text(
        "id_exame;fl_relevante;exm_laudo_resultado\n"
        'K1;0;"{""a"":1,""b"":2,""c"":3,""d"":4}"\n',
        encoding="utf-8-sig",
    )
    aud = read_rows_semico_first(a)
    assert aud[0].get("id_exame") == "K1"
    res = run_compare_audit_vs_legacy(audit_csv=a, legacy_csv=e, out_json=None)
    assert res["n_joined"] == 1
    assert res["status"] == "ok"


def test_compare_only_cod_123_filters_non_labeled_rows(tmp_path: Path) -> None:
    e = tmp_path / "expected.csv"
    e.write_text(
        "id_exame;cod_achado_relevante\n"
        "E1;1 - positivo\n"
        "E2;2 - positivo sem doenca ativa\n"
        "E3;3 - negativo\n"
        "E4;\n",
        encoding="utf-8-sig",
    )
    a = tmp_path / "audit.csv"
    a.write_text(
        "id_exame;fl_relevante\n"
        "E1;1\n"
        "E2;1\n"
        "E3;0\n"
        "E4;0\n",
        encoding="utf-8-sig",
    )
    all_rows = run_compare_audit_vs_legacy(audit_csv=a, legacy_csv=e, out_json=None, only_cod_123=False)
    labeled_only = run_compare_audit_vs_legacy(audit_csv=a, legacy_csv=e, out_json=None, only_cod_123=True)
    assert all_rows["n_joined"] == 4
    assert labeled_only["n_joined"] == 3
    assert labeled_only["gold_filter"]["only_cod_123"] is True
    assert labeled_only["gold_filter"]["n_skipped_legacy_non_123"] == 1


def test_diamond_lake_schema_maps_columns(tmp_path: Path) -> None:
    """``tb_diamond_mod_*_saida``: id_exame, proced_laudo_exame, fl_relevante."""
    header = (
        "dt_execucao,id_predicao,id_exame,id_paciente,proced_laudo_exame,fl_relevante\n"
    )
    body = (
        "2024-01-01,u1,E1,P1,texto sintetico positivo.,TRUE\n"
        "2024-01-02,u2,E2,P2,texto sintetico negativo.,FALSE\n"
    )
    p = tmp_path / "tb_diamond_mod_hepatologia_saida.csv"
    p.write_text(header + body, encoding="utf-8-sig")
    rows = read_diamond_hepatologia(p)
    assert len(rows) == 2
    assert rows[0]["exm_encaminhamento_nlp"] == "S"
    assert rows[1]["exm_encaminhamento_nlp"] == "N"
    assert rows[0]["exm_class"] == "DIAMOND_LAKE"

    summary = run_build(
        source_legacy_csv=p,
        out_dir=tmp_path / "out",
        source="diamond",
        max_positive=1,
        max_negative=1,
        random_seed=0,
    )
    assert summary["sample_rows"] == 2
    assert summary["enc_distribution"]["S"] == 1
    assert summary["enc_distribution"]["N"] == 1


def test_build_stratified_caps_positive_and_negative(tmp_path: Path) -> None:
    rows = []
    for i in range(10):
        rows.append(
            {
                "id_pct": f"P{i}",
                "exm_an": f"A{i}",
                "exm_mod": "",
                "exm_tipo": "",
                "exm_data": "",
                "exm_laudo_texto": f"t{i}",
                "exm_encaminhamento_nlp": "S" if i < 6 else "N",
                "exm_class": "x",
                "exm_frase_selec": "",
            }
        )
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=[
            "id_pct",
            "exm_an",
            "exm_mod",
            "exm_tipo",
            "exm_data",
            "exm_laudo_texto",
            "exm_encaminhamento_nlp",
            "exm_class",
            "exm_frase_selec",
        ],
    )
    w.writeheader()
    for r in rows:
        w.writerow(r)
    p = tmp_path / "mix.csv"
    p.write_text(buf.getvalue(), encoding="utf-8")

    summary = run_build(
        source_legacy_csv=p,
        out_dir=tmp_path,
        max_positive=2,
        max_negative=3,
        random_seed=0,
        source="diamond",
    )
    assert summary["sample_rows"] == 5
    assert summary["pick"]["mode"] == "stratified"
    assert summary["pick"]["picked_positive"] == 2
    assert summary["pick"]["picked_negative"] == 3
    enc = summary["enc_distribution"]
    assert enc.get("S") == 2 and enc.get("N") == 3


def test_diamond_merges_two_lake_files_dedup_id_exame(tmp_path: Path) -> None:
    """Dois exports Diamond (ex.: so true + so false); dedup por id_exame."""
    header = "dt_execucao,id_predicao,id_exame,id_paciente,proced_laudo_exame,fl_relevante\n"
    a = tmp_path / "true.csv"
    b = tmp_path / "false.csv"
    a.write_text(
        header
        + "2024-01-01,u1,EX-S1,P1,laudo sintetico curto.,true\n"
        + "2024-01-01,u2,EX-DUP,P2,duplicado primeiro.,true\n",
        encoding="utf-8-sig",
    )
    b.write_text(
        header
        + "2024-01-02,v1,EX-N1,P3,laudo negativo sintetico.,false\n"
        + "2024-01-02,v2,EX-DUP,P2,duplicado ignora-se segundo.,false\n",
        encoding="utf-8-sig",
    )
    s = run_build(
        source_legacy_csv=[a, b],
        out_dir=tmp_path / "out",
        source="diamond",
        max_positive=2,
        max_negative=1,
        random_seed=0,
    )
    assert s["n_legacy_files"] == 2
    assert s["source_rows"] == 3
    assert s["sample_rows"] == 3
    enc = s["enc_distribution"]
    assert enc.get("S") == 2 and enc.get("N") == 1


def test_gold_hive_merges_two_saida_files_dedup_id_exame(tmp_path: Path) -> None:
    h1 = "dataExecucaoModelo,idExame,idPaciente,laudoExame,flgRelevante\n"
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text(
        h1
        + "2026-01-01,EX-A,P1,Texto sintetico A sem achado alvo.,FALSE\n",
        encoding="utf-8",
    )
    b.write_text(
        h1
        + "2026-01-01,EX-B,P2,Outro texto B sintetico.,FALSE\n"
        + "2026-01-01,EX-A,P1,duplicado chave,TRUE\n",
        encoding="utf-8",
    )
    s = run_build(
        source_legacy_csv=[a, b],
        out_dir=tmp_path,
        max_rows=None,
        source="gold_hive",
    )
    assert s["n_legacy_files"] == 2
    assert s["source_rows"] == 2
    assert s["sample_rows"] == 2
