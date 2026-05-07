#!/usr/bin/env python3
"""Auditoria S02: aplica ``ClinicalNlpEngine.process`` a linhas de CSV e grava CSV de saida.

Colunas de entrada: ``exm_laudo_texto`` ou ``Laudo`` (e opcionalmente ids). Saida: texto tratado,
flags, score e JSON de resultado (para diff vs gold quando existir coluna no mesmo join).

Config:
  * Sem ``--config-yaml``: usa config fixa de demo alinhada a bexiga/calculos (igual ``demo_engine_e2e`` modo CSV).
  * Com ``--config-yaml``: carrega YAML de especialidade (top-level ``specialty_id``, ``config_version``, ``nlp``),
    opcionalmente faz merge de ``shared_organs_path`` se o ficheiro existir.

Exemplo:

    .venv\\Scripts\\python.exe scripts\\audit_engine_from_csv.py --csv tests\\fixtures\\hml_parity_minimal.csv -o _local_samples\\exports\\audit_out.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import yaml

from nlp_engine.config_loader import load, merge_with_shared_organs
from nlp_engine.engine import ClinicalNlpEngine
from nlp_engine.output_invariants import validate_engine_output_row


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


def _row_from_csv_record(rec: dict[str, str], *, fallback_id: str) -> dict[str, Any]:
    text = (
        (rec.get("exm_laudo_texto") or "").strip()
        or (rec.get("Laudo") or "").strip()
        or (rec.get("laudo_texto") or "").strip()
        or (rec.get("proced_laudo_exame") or "").strip()
    )
    rid = (rec.get("id_exame") or rec.get("id_pct") or rec.get("id") or fallback_id).strip()
    pred_id = (rec.get("id_predicao") or rec.get("idPredicao") or "").strip()
    return {
        "id_exame": rid or fallback_id,
        "id_predicao": pred_id,
        "id_paciente": (rec.get("id_paciente") or "P-SYN-CSV").strip() or "P-SYN-CSV",
        "id_unidade": (rec.get("id_unidade") or "U-SYN-CSV").strip() or "U-SYN-CSV",
        "exm_laudo_texto": text,
        "exm_mod": (rec.get("exm_mod") or "SYN").strip() or "SYN",
        "exm_tipo": (rec.get("exm_tipo") or "csv_fixture").strip() or "csv_fixture",
        "dt_exame": (rec.get("dt_exame") or "2026-04-14").strip() or "2026-04-14",
    }


def _demo_nlp_config() -> dict[str, Any]:
    return {
        "findings": {"calculos": ["calculos"], "parede": ["paredes"]},
        "target_organs": ["bexiga"],
        "organs": {"bexiga": {"seeds": ["bexiga"]}},
        "negation_phrases": ["sem"],
        "negation_window": 7,
        "score_policy_version": "v1_bins_legacy",
    }


def _load_nlp_from_specialty_yaml(path: Path) -> tuple[dict[str, Any], str, str]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict) or "nlp" not in raw:
        raise ValueError("YAML deve conter chave top-level 'nlp'")
    conf = load(raw)
    shared_path = conf["nlp"].get("shared_organs_path")
    shared: dict[str, Any] | None = None
    if isinstance(shared_path, str) and shared_path.strip():
        p = (path.parent / shared_path).resolve()
        if p.is_file():
            shared = yaml.safe_load(p.read_text(encoding="utf-8-sig"))
            if not isinstance(shared, dict):
                shared = None
    merged = merge_with_shared_organs(shared, raw)
    nlp = dict(merged["nlp"])
    return (
        nlp,
        str(merged["specialty_id"]),
        str(merged["config_version"]),
    )


def run_audit(
    csv_in: Path,
    csv_out: Path,
    *,
    max_rows: int = 10_000,
    config_yaml: Path | None = None,
    engine_version: str = "audit-local",
    validate_output_invariants: bool = False,
) -> int:
    """Processa ``csv_in`` e grava auditoria em ``csv_out``. Retorna numero de linhas escritas.

    Se ``validate_output_invariants`` for True, cada linha de saida e validada com
    ``validate_engine_output_row`` (T02.4a + JSON minimo); falha na primeira violacao.
    """
    if not csv_in.is_file():
        raise FileNotFoundError(f"CSV nao encontrado: {csv_in}")

    if config_yaml is not None:
        if not config_yaml.is_file():
            raise FileNotFoundError(f"YAML nao encontrado: {config_yaml}")
        nlp_cfg, specialty_id, config_version = _load_nlp_from_specialty_yaml(config_yaml)
    else:
        nlp_cfg = _demo_nlp_config()
        specialty_id = "csv_demo"
        config_version = "1.0.0-audit-demo"

    rows_in = _read_csv_rows(csv_in)[: max(1, max_rows)]
    in_rows = [_row_from_csv_record(dict(r), fallback_id=f"ROW-{i}") for i, r in enumerate(rows_in, start=1)]

    eng = ClinicalNlpEngine(engine_version=engine_version)
    out_rows = eng.process(in_rows, nlp_cfg, specialty_id=specialty_id, config_version=config_version)

    if validate_output_invariants:
        for i, row in enumerate(out_rows, start=1):
            inv_errs = validate_engine_output_row(row)
            if inv_errs:
                raise ValueError(f"linha {i} invariantes: {inv_errs}")

    fieldnames = [
        "id_exame",
        "id_predicao",
        "fl_relevante",
        "confidence_score",
        "exm_laudo_texto_tratado",
        "summary_compact_json",
        "n_positive_spans",
        "n_negated_spans",
        "rule_engine_version",
        "score_policy_version",
        "exm_laudo_resultado",
    ]
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    with csv_out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for rec_in, r in zip(rows_in, out_rows):
            payload = json.loads(r["exm_laudo_resultado"])
            source_pred_id = (
                (rec_in.get("id_predicao") or "").strip()
                or (rec_in.get("idPredicao") or "").strip()
            )
            w.writerow(
                {
                    "id_exame": r.get("id_exame", ""),
                    # Preserva id_predicao de entrada para pareamento 1:1 no compare Diamond.
                    "id_predicao": source_pred_id or r.get("id_predicao", ""),
                    "fl_relevante": r.get("fl_relevante", ""),
                    "confidence_score": r.get("confidence_score", ""),
                    "exm_laudo_texto_tratado": r.get("exm_laudo_texto_tratado", ""),
                    "summary_compact_json": json.dumps(
                        payload.get("summary_compact", []), ensure_ascii=False
                    ),
                    "n_positive_spans": payload.get("n_positive_spans", ""),
                    "n_negated_spans": payload.get("n_negated_spans", ""),
                    "rule_engine_version": payload.get("rule_engine_version", ""),
                    "score_policy_version": payload.get("score_policy_version", ""),
                    "exm_laudo_resultado": r.get("exm_laudo_resultado", ""),
                }
            )
    return len(out_rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Auditoria ClinicalNlpEngine a partir de CSV")
    ap.add_argument("--csv", type=Path, required=True)
    ap.add_argument("-o", "--output", type=Path, required=True)
    ap.add_argument("--max-rows", type=int, default=10_000)
    ap.add_argument("--config-yaml", type=Path, default=None)
    ap.add_argument("--engine-version", default="audit-local")
    ap.add_argument(
        "--validate-output-invariants",
        action="store_true",
        help="Falha se alguma linha de saida violar output_invariants (T02.4a + JSON minimo)",
    )
    args = ap.parse_args()

    n = run_audit(
        args.csv,
        args.output,
        max_rows=args.max_rows,
        config_yaml=args.config_yaml,
        engine_version=args.engine_version,
        validate_output_invariants=args.validate_output_invariants,
    )
    print(f"Escrito: {args.output.resolve()} ({n} linhas)")


if __name__ == "__main__":
    main()
