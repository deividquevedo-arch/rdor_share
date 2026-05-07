#!/usr/bin/env python3
"""Demo E2E do fluxo do motor: entrada -> to_plain -> rule_engine -> scoring -> saida.

Dados sinteticos no repositorio (sem PHI). CSV externo: so caminho local fornecido por voce. Para gerar CSV sintetico: `scripts/gen_synthetic_motor_export_csv.py`.

Executar a partir de `plataform/nlp_engine`:

    .venv\\Scripts\\python.exe scripts\\demo_engine_e2e.py
    .venv\\Scripts\\python.exe scripts\\demo_engine_e2e.py --scenario broad
    .venv\\Scripts\\python.exe scripts\\demo_engine_e2e.py --csv tests\\fixtures\\hml_parity_minimal.csv
    .venv\\Scripts\\python.exe scripts\\demo_engine_e2e.py --csv "D:\\pastas\\ficheiro_real.csv"
"""

from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.rule_engine import process_rule_based
from nlp_engine.scoring import (
    confidence_rule_based,
    fl_relevante_from_counts,
    normalize_score_policy,
)
from nlp_engine.text_pipeline.to_plain import to_plain


def _raw_laudo_text(row: Mapping[str, Any]) -> str:
    primary = row.get("exm_laudo_texto")
    if primary is not None and str(primary).strip():
        return str(primary)
    fallback = row.get("Laudo")
    if fallback is not None:
        return str(fallback)
    return ""


def _trailing_line_patterns(nlp_config: Mapping[str, Any]) -> list[str] | None:
    tp = nlp_config.get("text_pipeline")
    if not isinstance(tp, Mapping):
        return None
    raw = tp.get("trailing_line_patterns")
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        return [str(p) for p in raw]
    return None


def _minimal_pair() -> tuple[dict[str, Any], dict[str, Any], str, str]:
    row: dict[str, Any] = {
        "id_exame": "EX-SYN-001",
        "id_paciente": "P-SYN-001",
        "id_unidade": "U-SYN-01",
        "exm_laudo_texto": (
            "<p>Figado com lesao focal.</p>\n"
            "<p>Rodape institucional x</p>"
        ),
        "exm_mod": "CT",
        "exm_tipo": "abdome",
        "dt_exame": "2026-04-14",
    }
    nlp_config: dict[str, Any] = {
        "findings": {"achado": ["lesao"]},
        "target_organs": ["figado"],
        "organs": {"figado": {"seeds": ["figado", "hepatica"]}},
        "negation_phrases": ["sem"],
        "negation_window": 7,
        "text_pipeline": {
            "trailing_line_patterns": [r"(?i)^rodape institucional x$"],
        },
        "score_policy_version": "v1_bins_legacy",
    }
    return row, nlp_config, "demo", "1.0.0-demo"


def _broad_pair() -> tuple[dict[str, Any], dict[str, Any], str, str]:
    """Laudo longo sintetico (multi-secao HTML) + config alinhada ao padrao colon (YAML plano)."""
    row = {
        "id_exame": "EX-SYN-BROAD-001",
        "id_paciente": "P-SYN-BROAD",
        "id_unidade": "U-SYN-01",
        "exm_laudo_texto": (
            "<p><strong>Tecnica</strong> TC abdome com contraste — cenario sintetico.</p>"
            "<p>Colon: evidencia-se lesao sesil na curvatura hepatica, 6 mm.</p>"
            "<p>Reto: sem polipo radiologico no segmento distal estudado.</p>"
            "<p>Figado e vias biliares sem alteracoes agudas no trecho sintetico.</p>"
            "<p>Medico responsavel</p>"
        ),
        "exm_mod": "CT",
        "exm_tipo": "abdome",
        "dt_exame": "2026-04-14",
    }
    nlp_config: dict[str, Any] = {
        "findings": {
            "lesao": ["lesao", "nodulo", "polipo", "massa"],
        },
        "target_organs": ["colon", "reto"],
        "organs": {
            "colon": {"seeds": ["colon", "colico"]},
            "reto": {"seeds": ["reto", "retoide"]},
        },
        "negation_phrases": ["sem"],
        "negation_window": 7,
        "text_pipeline": {
            "trailing_line_patterns": [r"(?i)^\s*medico\s+responsavel\s*$"],
        },
        "score_policy_version": "v1_bins_legacy",
    }
    return row, nlp_config, "colon", "1.0.0-demo-broad"


def _csv_fixture_config() -> dict[str, Any]:
    """Config generica para linhas do CSV sintetico `hml_parity_minimal.csv` (bexiga)."""
    return {
        "findings": {
            "calculos": ["calculos"],
            "parede": ["paredes"],
        },
        "target_organs": ["bexiga"],
        "organs": {"bexiga": {"seeds": ["bexiga"]}},
        "negation_phrases": ["sem"],
        "negation_window": 7,
        "score_policy_version": "v1_bins_legacy",
    }


def _sniff_delimiter(path: Path) -> str:
    with path.open(encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        return ";"


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    delim = _sniff_delimiter(path)
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=delim))


def _row_from_csv_record(rec: Mapping[str, str], *, fallback_id: str) -> dict[str, Any]:
    text = (
        (rec.get("exm_laudo_texto") or "").strip()
        or (rec.get("Laudo") or "").strip()
        or (rec.get("laudo_texto") or "").strip()
    )
    rid = (rec.get("id_exame") or rec.get("id_pct") or rec.get("id") or fallback_id).strip()
    return {
        "id_exame": rid or fallback_id,
        "id_paciente": (rec.get("id_paciente") or "P-SYN-CSV").strip() or "P-SYN-CSV",
        "id_unidade": (rec.get("id_unidade") or "U-SYN-CSV").strip() or "U-SYN-CSV",
        "exm_laudo_texto": text,
        "exm_mod": (rec.get("exm_mod") or "SYN").strip() or "SYN",
        "exm_tipo": (rec.get("exm_tipo") or "csv_fixture").strip() or "csv_fixture",
        "dt_exame": (rec.get("dt_exame") or "2026-04-14").strip() or "2026-04-14",
    }


def trace_pipeline(
    row: Mapping[str, Any],
    nlp_config: Mapping[str, Any],
    *,
    specialty_id: str,
    config_version: str,
    engine_version: str,
    banner: str | None = None,
) -> None:
    if banner:
        print("\n" + "=" * 72)
        print(banner)
        print("=" * 72)

    print("=" * 72)
    print("PASSO 1 — Linha de entrada (contrato data_manage -> motor)")
    print("=" * 72)
    print(json.dumps(dict(row), ensure_ascii=False, indent=2))

    print("\n" + "=" * 72)
    print("PASSO 2 — Texto bruto extraido (`exm_laudo_texto` ou fallback `Laudo`)")
    print("=" * 72)
    raw = _raw_laudo_text(row)
    print(repr(raw))

    trailing = _trailing_line_patterns(nlp_config)
    print("\n" + "=" * 72)
    print("PASSO 3 — `to_plain` (TextPipeline): HTML/RTF/plain -> texto limpo")
    print("=" * 72)
    print(f"trailing_line_patterns: {trailing}")
    treated = to_plain(raw, trailing_line_patterns=trailing)
    print("--- texto tratado ---")
    print(treated)

    print("\n" + "=" * 72)
    print("PASSO 4 — `process_rule_based` (S02 T02.2): achados + negação + orgaos")
    print("=" * 72)
    print("nlp_config:", json.dumps(dict(nlp_config), ensure_ascii=False, indent=2))
    rb = process_rule_based(treated, nlp_config)
    print(json.dumps(rb, ensure_ascii=False, indent=2))

    print("\n" + "=" * 72)
    print("PASSO 5 — Scoring (S02 T02.3)")
    print("=" * 72)
    policy = normalize_score_policy(nlp_config.get("score_policy_version"))
    n_pos = int(rb["n_positive_spans"])
    n_neg = int(rb["n_negated_spans"])
    score = confidence_rule_based(
        n_positive_spans=n_pos, n_negated_spans=n_neg, policy=policy
    )
    fl = fl_relevante_from_counts(n_pos)
    print(f"score_policy_version: {policy}")
    print(f"n_positive_spans={n_pos}, n_negated_spans={n_neg}")
    print(f"confidence_score={score}, fl_relevante={fl}")

    print("\n" + "=" * 72)
    print("PASSO 6 — `ClinicalNlpEngine.process` (saida contrato completa)")
    print("=" * 72)
    eng = ClinicalNlpEngine(engine_version=engine_version)
    out = eng.process(
        [dict(row)], nlp_config, specialty_id=specialty_id, config_version=config_version
    )
    print(json.dumps(out[0], ensure_ascii=False, indent=2))
    print("\n" + "=" * 72)
    print("FIM — `exm_laudo_resultado` (JSON parseado)")
    print("=" * 72)
    print(json.dumps(json.loads(out[0]["exm_laudo_resultado"]), ensure_ascii=False, indent=2))


def _run_csv(path: Path, max_rows: int, engine_version: str) -> None:
    rows_csv = _read_csv_rows(path)
    cfg = _csv_fixture_config()
    specialty_id = "csv_demo"
    config_version = "1.0.0-csv-demo"
    n = min(max_rows, len(rows_csv))
    print("=" * 72)
    print(f"MODO CSV — {path} ({n} linha(s), config fixa para demo bexiga/calculos)")
    print("=" * 72)
    for i, rec in enumerate(rows_csv[:n], start=1):
        row = _row_from_csv_record(rec, fallback_id=f"ROW-{i}")
        trace_pipeline(
            row,
            cfg,
            specialty_id=specialty_id,
            config_version=config_version,
            engine_version=engine_version,
            banner=f"LINHA {i}/{n} — id_exame={row['id_exame']}",
        )


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Trace E2E do nlp_engine (sintetico / CSV local).")
    p.add_argument(
        "--scenario",
        choices=("minimal", "broad"),
        default="minimal",
        help="minimal: demo curto HTML+rodape. broad: laudo longo sintetico estilo colon/reto.",
    )
    p.add_argument(
        "--csv",
        type=Path,
        metavar="PATH",
        help=(
            "Caminho absoluto ou relativo a um CSV real (nao use placeholders). "
            "Amostra sintetica no repo: tests/fixtures/hml_parity_minimal.csv"
        ),
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=10,
        metavar="N",
        help="Com --csv: no maximo N linhas de dados (default 10).",
    )
    p.add_argument(
        "--engine-version",
        default="demo-9.9.9",
        help="Versao de motor gravada na saida (default demo-9.9.9).",
    )
    args = p.parse_args(list(argv) if argv is not None else None)

    if args.csv is not None:
        if not args.csv.is_file():
            raise SystemExit(
                "CSV nao encontrado: "
                f"{args.csv.resolve()}\n"
                "Use um caminho de ficheiro que exista (ex.: "
                "tests\\fixtures\\hml_parity_minimal.csv a partir de plataform\\nlp_engine)."
            )
        _run_csv(args.csv, max(1, args.max_rows), args.engine_version)
        return

    if args.scenario == "minimal":
        row, cfg, sid, cv = _minimal_pair()
    else:
        row, cfg, sid, cv = _broad_pair()

    trace_pipeline(
        row,
        cfg,
        specialty_id=sid,
        config_version=cv,
        engine_version=args.engine_version,
        banner=f"CENARIO — {args.scenario}",
    )


if __name__ == "__main__":
    main()
