#!/usr/bin/env python3
"""Deep-dive de FN Hepatologia (audit+expected+input) ou exploração só do input (cluster/triagem)."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.audit_legacy_compare import legacy_s_n_from_row, read_rows_semico_first

_RE_VESICULA = re.compile(r"ves[ií]cul\w*(?:\s+biliar)?", re.IGNORECASE)


def _text_matches_biliar_litiase_cluster(text_lower: str) -> bool:
    """Colelitíase ou menção à vesícula biliar (tolera typo tipo ``vesiculs biliar``)."""
    return (
        "colelitiase" in text_lower
        or "colelitíase" in text_lower
        or bool(re.search(r"ves[ií]cul\w*\s+biliar", text_lower))
    )
# Negacao de litiase/calculos perto da vesicula (trecho curto apos menção).
_RE_NEGATED_STONES_LINE = re.compile(
    r"(sem\s+c[áa]lculos|sem\s+lit[ií]ase|aus[êe]ncia\s+de\s+c[áa]lculos|"
    r"n[aã]o\s+h[áa]\s+c[áa]lculos|sem\s+sinais\s+de\s+colelit|"
    r"n[aã]o\s+se\s+observam\s+c[áa]lculos|livre\s+de\s+c[áa]lculos)",
    re.IGNORECASE,
)
# "calculado" em contexto de elastografia/medidas — não é colelitíase.
_RE_ELASTO_CONTEXT = re.compile(
    r"elastograf|fibrose|fibroscan|\bkpa\b|rigidez|shear|te[sś]\s+de|"
    r"stiffness|metavir|point\s+shear",
    re.IGNORECASE,
)
_RE_POSITIVE_LITHIASIS = re.compile(
    r"colelit|microlitiase|colecistolitiase|c[áa]lculos?\s+na\s+ves[ií]cul\w*|"
    r"c[áa]lculos?\s+à\s+ves[ií]cul\w*|ves[ií]cul\w*[^.]{0,70}"
    r"(c[áa]lculos|colecistolitiase|microlitiase|lit[ií]ase|barro\s+biliar|lama\s+biliar)|"
    r"barro\s+biliar|lama\s+biliar|cole[cç][aã]o\s+biliar\s+calcul",
    re.IGNORECASE,
)


def triage_biliary_fn_text(text: str) -> str:
    """Classifica texto já rotulado como cluster ``biliar_litiase`` (heurística de triagem).

    Objetivo: separar FNs potencialmente corrigíveis por YAML de ruído (negação,
    elastografia com 'calculado', vesícula sem achado calculoso).
    """
    raw = text or ""
    t = raw.lower()
    if not _text_matches_biliar_litiase_cluster(t):
        return "not_biliar_cluster"

    if re.search(r"calculad[oa]", t) and _RE_ELASTO_CONTEXT.search(t):
        return "noise_elastography_calculado"

    if _RE_POSITIVE_LITHIASIS.search(raw):
        # Ainda pode estar negado na mesma frase; checa janela curta.
        for m in _RE_POSITIVE_LITHIASIS.finditer(raw):
            span = raw[max(0, m.start() - 40) : m.end() + 40].lower()
            if _RE_NEGATED_STONES_LINE.search(span):
                return "noise_negated_stones"
        return "likely_true_miss"

    for vm in _RE_VESICULA.finditer(raw):
        window = raw[vm.start() : vm.start() + 160]
        if _RE_NEGATED_STONES_LINE.search(window.lower()):
            return "noise_negated_stones"

    if ("sem cálculos" in t or "sem calculos" in t) and (
        "vesicula" in t
        or "vesícula" in t
        or "colecisto" in t
        or re.search(r"ves[ií]cul", t)
    ):
        return "noise_negated_stones"

    if "vesicula" in t or "vesícula" in t:
        return "ambiguous_vesicula_only"

    return "ambiguous_other"


def _read_csv(path: Path, *, delimiter: str) -> list[dict[str, str]]:
    """Mantém compatibilidade com CLI e prioriza leitura robusta semicolon-first."""
    _ = delimiter  # Compat legacy: o leitor real faz sniff com fallback robusto.
    return read_rows_semico_first(path)


def _motor_sn_from_row(row: dict[str, str]) -> str:
    raw = str(row.get("fl_relevante", "") or "").strip()
    return "S" if raw == "1" else "N"


def _legacy_sn_from_row(row: dict[str, str]) -> str:
    return legacy_s_n_from_row(row)


def _strip(v: Any) -> str:
    return str(v or "").strip()


def _key_motor(row: dict[str, str]) -> str:
    pred = _strip(row.get("id_predicao"))
    if pred:
        return f"PRED::{pred}"
    exam = _strip(row.get("id_exame"))
    if exam:
        return f"EX::{exam}"
    return ""


def _key_legacy(row: dict[str, str]) -> str:
    pred = _strip(row.get("id_predicao") or row.get("idPredicao"))
    if pred:
        return f"PRED::{pred}"
    exam = _strip(row.get("id_exame") or row.get("id_pct") or row.get("id"))
    if exam:
        return f"EX::{exam}"
    return ""


def _exam_bucket(text: str) -> str:
    t = (text or "").lower()
    if "ultrassonografia" in t:
        return "US"
    if "ressonancia" in t or "ressonância" in t:
        return "RM"
    if "tomografia" in t:
        return "TC"
    return "OUTROS"


def _cluster_bucket(text: str) -> str:
    t = (text or "").lower()
    if _text_matches_biliar_litiase_cluster(t):
        return "biliar_litiase"
    if "vias biliares" in t or "coledoco" in t or "colang" in t:
        return "vias_biliares_colangio"
    if "hepatopatia" in t or "cirrose" in t or "hipertensao portal" in t:
        return "hepatopatia_cronica"
    if "esteatose" in t:
        return "esteatose"
    if "nodulo" in t or "nódulo" in t or "lesao" in t or "lesão" in t or "massa" in t:
        return "lesao_focal"
    if "cisto" in t:
        return "cisto"
    return "outros"


def run_deep_dive(
    *,
    audit_csv: Path,
    expected_csv: Path,
    input_csv: Path,
    out_json: Path | None = None,
    top_n: int = 300,
    csv_delimiter: str = ";",
    input_text_column: str = "exm_laudo_texto",
) -> dict[str, Any]:
    audit_rows = _read_csv(audit_csv, delimiter=csv_delimiter)
    expected_rows = _read_csv(expected_csv, delimiter=csv_delimiter)
    motor_by_key = {_key_motor(r): r for r in audit_rows if _key_motor(r)}
    legacy_by_key = {_key_legacy(r): r for r in expected_rows if _key_legacy(r)}
    pred_keys = sorted(
        k for k in (set(motor_by_key) & set(legacy_by_key)) if k.startswith("PRED::")
    )
    pairs: list[tuple[dict[str, str], dict[str, str]]] = [(motor_by_key[k], legacy_by_key[k]) for k in pred_keys]

    used_motor = {id(m) for m, _ in pairs}
    used_legacy = {id(l) for _, l in pairs}
    motor_ex = {
        _strip(r.get("id_exame")): r
        for r in audit_rows
        if _strip(r.get("id_exame")) and id(r) not in used_motor
    }
    legacy_ex = {
        _strip(r.get("id_exame") or r.get("id_pct") or r.get("id")): r
        for r in expected_rows
        if _strip(r.get("id_exame") or r.get("id_pct") or r.get("id")) and id(r) not in used_legacy
    }
    ex_keys = sorted(set(motor_ex) & set(legacy_ex))
    pairs.extend((motor_ex[k], legacy_ex[k]) for k in ex_keys)

    fn_ids = []
    for m, l in pairs:
        if _legacy_sn_from_row(l) == "S" and _motor_sn_from_row(m) == "N":
            exam_id = _strip(m.get("id_exame")) or _strip(l.get("id_exame") or l.get("id_pct") or l.get("id"))
            if exam_id:
                fn_ids.append(exam_id)
        if len(fn_ids) >= max(0, top_n):
            break

    input_by_id = {
        _strip(r.get("id_exame") or r.get("id_pct") or r.get("id")): r
        for r in _read_csv(input_csv, delimiter=csv_delimiter)
        if _strip(r.get("id_exame") or r.get("id_pct") or r.get("id"))
    }
    exam_counter: Counter[str] = Counter()
    cluster_counter: Counter[str] = Counter()
    samples: list[dict[str, str]] = []
    biliary_triage_counter: Counter[str] = Counter()
    biliary_samples_by_tier: dict[str, list[dict[str, str]]] = {}
    biliary_ids_by_triage: dict[str, list[str]] = {}

    for exam_id in fn_ids:
        row = input_by_id.get(exam_id)
        if not row:
            continue
        text = str(row.get(input_text_column, "") or "")
        exam_bucket = _exam_bucket(text)
        cluster_bucket = _cluster_bucket(text)
        exam_counter[exam_bucket] += 1
        cluster_counter[cluster_bucket] += 1
        if cluster_bucket == "biliar_litiase":
            tier = triage_biliary_fn_text(text)
            biliary_triage_counter[tier] += 1
            biliary_ids_by_triage.setdefault(tier, []).append(exam_id)
            bucket = biliary_samples_by_tier.setdefault(tier, [])
            if len(bucket) < 15:
                bucket.append(
                    {
                        "id_exame": exam_id,
                        "triage": tier,
                        "exam_bucket": exam_bucket,
                        "snippet": text[:280].replace("\n", " | "),
                    }
                )
        if len(samples) < 30:
            samples.append(
                {
                    "id_exame": exam_id,
                    "exam_bucket": exam_bucket,
                    "cluster_bucket": cluster_bucket,
                    "snippet": text[:220].replace("\n", " | "),
                }
            )

    result: dict[str, Any] = {
        "top_n_requested": top_n,
        "n_joined": len(pairs),
        "fn_rows_analyzed": len(fn_ids),
        "exam_bucket_distribution": dict(exam_counter),
        "cluster_distribution": dict(cluster_counter),
        "sample_rows": samples,
        "biliar_fn_in_scope": int(cluster_counter.get("biliar_litiase", 0)),
        "biliar_triage_distribution": dict(biliary_triage_counter),
        "biliar_triage_samples": biliary_samples_by_tier,
        "biliar_ids_by_triage": biliary_ids_by_triage,
    }
    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def run_cluster_explore(
    *,
    input_csv: Path,
    out_json: Path | None = None,
    csv_delimiter: str = ",",
    input_text_column: str = "proced_laudo_exame",
    row_limit: int | None = None,
) -> dict[str, Any]:
    """Varre o CSV de laudos: distribuição de exame/cluster e triagem biliar (sem FN vs legado)."""
    rows = _read_csv(input_csv, delimiter=csv_delimiter)
    if row_limit is not None and row_limit >= 0:
        rows = rows[:row_limit]
    exam_counter: Counter[str] = Counter()
    cluster_counter: Counter[str] = Counter()
    biliary_triage_counter: Counter[str] = Counter()
    biliary_samples_by_tier: dict[str, list[dict[str, str]]] = {}
    biliary_ids_by_triage: dict[str, list[str]] = {}
    n_empty = 0
    for row in rows:
        text = str(row.get(input_text_column, "") or "")
        if not text.strip():
            n_empty += 1
            continue
        exam_id = str(row.get("id_exame", "")).strip() or "?"
        exam_bucket = _exam_bucket(text)
        exam_counter[exam_bucket] += 1
        cluster_bucket = _cluster_bucket(text)
        cluster_counter[cluster_bucket] += 1
        if cluster_bucket == "biliar_litiase":
            tier = triage_biliary_fn_text(text)
            biliary_triage_counter[tier] += 1
            biliary_ids_by_triage.setdefault(tier, []).append(exam_id)
            buf = biliary_samples_by_tier.setdefault(tier, [])
            if len(buf) < 12:
                buf.append(
                    {
                        "id_exame": exam_id,
                        "triage": tier,
                        "exam_bucket": exam_bucket,
                        "snippet": text[:280].replace("\n", " | "),
                    }
                )
    result: dict[str, Any] = {
        "mode": "explore_only",
        "rows_read": len(rows),
        "rows_with_empty_text": n_empty,
        "rows_scored": len(rows) - n_empty,
        "exam_bucket_distribution": dict(exam_counter),
        "cluster_distribution": dict(cluster_counter),
        "biliar_fn_in_scope": int(cluster_counter.get("biliar_litiase", 0)),
        "biliar_triage_distribution": dict(biliary_triage_counter),
        "biliar_triage_samples": biliary_samples_by_tier,
        "biliar_ids_by_triage": biliary_ids_by_triage,
    }
    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Deep-dive de FN para Hepatologia")
    ap.add_argument("--audit-csv", type=Path, default=None)
    ap.add_argument("--expected-csv", type=Path, default=None)
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--out-json", type=Path, default=None)
    ap.add_argument("--top-n", type=int, default=300)
    ap.add_argument(
        "--delimiter",
        default=";",
        help=(
            'Separador CSV nos três ficheiros. No PowerShell, vírgula não pode ser só `--delimiter ,` '
            '(o `,` é operador); use `--comma-csv` ou `--delimiter=,` ou `--delimiter \',\'`.'
        ),
    )
    ap.add_argument(
        "--comma-csv",
        action="store_true",
        help="Atalho: delimitador vírgula (exports Diamond/Spark) sem passar o caractere `,` no PowerShell.",
    )
    ap.add_argument(
        "--input-text-column",
        default="exm_laudo_texto",
        metavar="COL",
        help="Coluna do texto do laudo no input (ex.: proced_laudo_exame na query Diamond).",
    )
    ap.add_argument(
        "--explore-only",
        action="store_true",
        help="Só lê --input-csv: clusters e triagem biliar em toda a amostra (sem audit/expected).",
    )
    ap.add_argument(
        "--row-limit",
        type=int,
        default=None,
        metavar="N",
        help="Com --explore-only, processa só as primeiras N linhas (debug).",
    )
    ap.add_argument(
        "--filter-triage",
        default=None,
        metavar="TIER",
        help=(
            "Imprime só id_exame (um por linha) das FN no cluster biliar_litiase "
            "com este rótulo (ex.: likely_true_miss, noise_negated_stones)."
        ),
    )
    args = ap.parse_args()
    csv_delimiter = "," if args.comma_csv else args.delimiter
    if args.explore_only:
        result = run_cluster_explore(
            input_csv=args.input_csv,
            out_json=args.out_json,
            csv_delimiter=csv_delimiter,
            input_text_column=args.input_text_column,
            row_limit=args.row_limit,
        )
    else:
        if args.audit_csv is None or args.expected_csv is None:
            ap.error("use --audit-csv e --expected-csv, ou então --explore-only + --input-csv")
        result = run_deep_dive(
            audit_csv=args.audit_csv,
            expected_csv=args.expected_csv,
            input_csv=args.input_csv,
            out_json=args.out_json,
            top_n=args.top_n,
            csv_delimiter=csv_delimiter,
            input_text_column=args.input_text_column,
        )
    if args.filter_triage:
        for exam_id in result.get("biliar_ids_by_triage", {}).get(args.filter_triage, []):
            print(exam_id)
        return
    if args.explore_only:
        print(
            f"explore rows_read={result['rows_read']} scored={result['rows_scored']} "
            f"empty_text={result['rows_with_empty_text']} "
            f"exam={result['exam_bucket_distribution']} cluster={result['cluster_distribution']}"
        )
        if result.get("biliar_fn_in_scope"):
            print(
                f"biliar_n={result['biliar_fn_in_scope']} "
                f"biliar_triage={result['biliar_triage_distribution']}"
            )
        return
    print(
        f"fn_rows={result['fn_rows_analyzed']} "
        f"exam={result['exam_bucket_distribution']} "
        f"cluster={result['cluster_distribution']}"
    )
    if result.get("biliar_fn_in_scope"):
        print(
            f"biliar_fn_n={result['biliar_fn_in_scope']} "
            f"biliar_triage={result['biliar_triage_distribution']}"
        )


if __name__ == "__main__":
    main()

