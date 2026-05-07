#!/usr/bin/env python3
"""Comparacao generica: audit motor (fl_relevante) vs CSV legado/expected (S/N).

Usado por Pulmao, Hepatologia e outras especialidades com o mesmo esquema de colunas.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def _read_rows(path: Path, delimiter: str) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def _sniff_delimiter(path: Path) -> str:
    with path.open(encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        return ";"


def read_rows_semico_first(path: Path) -> list[dict[str, str]]:
    """Export audit/expected usa `;` primeiro; evita sniffer a escolher `,` com JSON no CSV."""
    sc = _read_rows(path, ";")
    if sc and "id_exame" in (sc[0] or {}):
        return sc
    return _read_rows(path, _sniff_delimiter(path))


def _motor_flag(row: dict[str, str]) -> str:
    raw = str(row.get("fl_relevante", "")).strip()
    return "S" if raw == "1" else "N"


def _sn_from_flg_relevante(flg: str) -> str:
    t = (flg or "").strip().upper()
    if t in ("TRUE", "1", "1.0", "S", "T", "YES"):
        return "S"
    if t in ("FALSE", "0", "0.0", "N", "F", "NO", ""):
        return "N"
    return "N"


def legacy_s_n_from_row(row: dict[str, str]) -> str:
    """Legado / gold: S vs N a partir de encaminhamento ou ``flgRelevante`` (TRUE/FALSE)."""
    cod_achado = str(row.get("cod_achado_relevante", "") or "").strip()
    if cod_achado:
        # Diamond: 1/2 = positivo, 3 = negativo (aceita "1 - ...", "2 - ...", "3 - ...").
        lead = cod_achado[:1]
        if lead in ("1", "2"):
            return "S"
        if lead == "3":
            return "N"
    a = (row.get("exm_encaminhamento_nlp") or row.get("expected_encaminhamento") or "").strip()
    u = a.upper()
    if u in ("S", "N"):
        return u
    if u in ("1", "TRUE", "1.0", "T", "YES"):
        return "S"
    if u in ("0", "FALSE", "0.0", "F", "NO"):
        return "N"
    if a:
        return "N"
    cap = str(row.get("flgRelevante", "") or "").strip()
    if cap:
        return _sn_from_flg_relevante(cap)
    low = str(row.get("fl_relevante", "") or "").strip()
    if low == "1":
        return "S"
    if low == "0":
        return "N"
    return "N"


def _legacy_flag(row: dict[str, str]) -> str:
    return legacy_s_n_from_row(row)


def _legacy_cod_bucket(row: dict[str, str]) -> str:
    raw = str(row.get("cod_achado_relevante", "") or "").strip()
    lead = raw[:1]
    if lead in ("1", "2", "3"):
        return lead
    return "other"


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


def run_compare_audit_vs_legacy(
    *,
    audit_csv: Path,
    legacy_csv: Path,
    out_json: Path | None = None,
    only_cod_123: bool = False,
) -> dict[str, Any]:
    if not audit_csv.is_file():
        raise FileNotFoundError(f"audit nao encontrado: {audit_csv}")
    if not legacy_csv.is_file():
        raise FileNotFoundError(f"legacy nao encontrado: {legacy_csv}")

    motor_rows = read_rows_semico_first(audit_csv)
    legacy_rows = read_rows_semico_first(legacy_csv)

    motor_by_id = {_key_motor(r): r for r in motor_rows if _key_motor(r)}
    legacy_by_id = {_key_legacy(r): r for r in legacy_rows if _key_legacy(r)}

    # 1) Pareamento por id_predicao quando presente em ambos.
    pred_keys = sorted(
        k for k in (set(motor_by_id) & set(legacy_by_id)) if k.startswith("PRED::")
    )
    pairs: list[tuple[dict[str, str], dict[str, str], str]] = [
        (motor_by_id[k], legacy_by_id[k], k) for k in pred_keys
    ]

    # 2) Fallback por id_exame para linhas não cobertas por id_predicao.
    used_motor = {id(m) for m, _, _ in pairs}
    used_legacy = {id(l) for _, l, _ in pairs}
    motor_ex = {
        _strip(r.get("id_exame")): r
        for r in motor_rows
        if _strip(r.get("id_exame")) and id(r) not in used_motor
    }
    legacy_ex = {
        _strip(r.get("id_exame") or r.get("id_pct") or r.get("id")): r
        for r in legacy_rows
        if _strip(r.get("id_exame") or r.get("id_pct") or r.get("id")) and id(r) not in used_legacy
    }
    ex_keys = sorted(set(motor_ex) & set(legacy_ex))
    pairs.extend((motor_ex[k], legacy_ex[k], f"EX::{k}") for k in ex_keys)

    if not pairs:
        result: dict[str, Any] = {
            "n_joined": 0,
            "n_match": 0,
            "n_mismatch": 0,
            "match_rate": 0.0,
            "status": "bloqueado_sem_chave_comum",
            "diagnostic": {
                "audit_key": "id_predicao ou id_exame",
                "legacy_key": "id_predicao ou id_exame/id_pct",
                "audit_rows": len(motor_rows),
                "legacy_rows": len(legacy_rows),
                "audit_key_examples": sorted([k for k in motor_by_id if k][:5]),
                "legacy_key_examples": sorted([k for k in legacy_by_id if k][:5]),
            },
            "note": "nenhuma chave comum; revisar pareamento (regerar amostra se id_exame era duplicado)",
        }
        if out_json is not None:
            out_json.parent.mkdir(parents=True, exist_ok=True)
            out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result

    matches = 0
    mismatches = 0
    cm: Counter[tuple[str, str]] = Counter()
    mismatch_examples: list[dict[str, str]] = []
    n_skipped_legacy_non_123 = 0
    for m, l, k in pairs:
        if only_cod_123 and _legacy_cod_bucket(l) == "other":
            n_skipped_legacy_non_123 += 1
            continue
        m_flag = _motor_flag(m)
        l_flag = _legacy_flag(l)
        cm[(l_flag, m_flag)] += 1
        if m_flag == l_flag:
            matches += 1
            continue
        mismatches += 1
        if len(mismatch_examples) < 20:
            frase = l.get("exm_frase_selec") or l.get("legacy_frase_selec") or ""
            cls = l.get("exm_class") or l.get("legacy_class") or ""
            mismatch_examples.append(
                {
                    "id_exame": k,
                    "legacy_encaminhamento": l_flag,
                    "motor_relevante": m_flag,
                    "legacy_class": str(cls),
                    "legacy_frase": str(frase)[:220],
                }
            )

    n_joined_effective = matches + mismatches
    result2: dict[str, Any] = {
        "n_joined": n_joined_effective,
        "n_match": matches,
        "n_mismatch": mismatches,
        "match_rate": round(matches / n_joined_effective, 4) if n_joined_effective else 0.0,
        "status": "ok",
        "legacy_distribution": dict(
            Counter(
                _legacy_flag(l)
                for _, l, _ in pairs
                if not only_cod_123 or _legacy_cod_bucket(l) != "other"
            )
        ),
        "motor_distribution": dict(
            Counter(
                _motor_flag(m)
                for _, l, _ in pairs
                if not only_cod_123 or _legacy_cod_bucket(l) != "other"
            )
        ),
        "confusion_matrix_legacy_vs_motor": {
            "legacy_S_motor_S": cm[("S", "S")],
            "legacy_S_motor_N": cm[("S", "N")],
            "legacy_N_motor_S": cm[("N", "S")],
            "legacy_N_motor_N": cm[("N", "N")],
        },
        "mismatch_examples": mismatch_examples,
        "gold_filter": {
            "only_cod_123": only_cod_123,
            "n_pairs_before_filter": len(pairs),
            "n_skipped_legacy_non_123": n_skipped_legacy_non_123,
        },
    }
    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(result2, ensure_ascii=False, indent=2), encoding="utf-8")
    return result2
