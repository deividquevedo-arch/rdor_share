#!/usr/bin/env python3
"""Monta amostra padrao Hepatologia (input + expected) a partir do legado disponivel.

Fontes suportadas:
* ``gold_hive`` — export Spark/Hive (ex.: ``hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv``):
  ``idExame``, ``idPaciente``, ``laudoExame``, ``flgRelevante``, ...
* ``diamond`` — export lake tipo ``tb_diamond_mod_*_saida`` (snake_case):
  ``id_exame``, ``id_paciente``, ``proced_laudo_exame``, ``fl_relevante``;
  ou formato antigo tipo Pulmao (``id_pct``, ``exm_encaminhamento_nlp``, ...).
  Repita ``--legacy-csv`` para juntar varios ficheiros (ex.: so ``true`` + so ``false``),
  com dedup por ``id_exame``.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Literal

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.build_pulmao_standard_sample import _behavior_label, _fl_from_enc, _pick_rows

SourceKind = Literal["diamond", "gold_hive"]


def _flg_relevante_to_enc(flg: str) -> str:
    t = (flg or "").strip().upper()
    if t in ("TRUE", "1", "1.0", "S", "T", "YES"):
        return "S"
    if t in ("FALSE", "0", "0.0", "N", "F", "NO"):
        return "N"
    return "N"


def read_hive_gold_hepatologia(path: Path) -> list[dict[str, str]]:
    """Lê CSV gold Hive (`,`, UTF-8, multilinha em campos citados). Mapeia para o schema interno."""
    with path.open(encoding="utf-8-sig", newline="") as f:
        raw_rows = list(csv.DictReader(f, delimiter=","))
    if not raw_rows:
        return []
    # Cabeçalhos mínimos (case-sensitive como no export Spark/Hive)
    out: list[dict[str, str]] = []
    for row in raw_rows:
        r = {k: (v if v is not None else "") for k, v in row.items()}
        id_pct = str(r.get("idPaciente", "") or "").strip()
        exm_an = str(r.get("idExame", "") or "").strip()
        laudo = r.get("laudoExame") or ""
        if not (id_pct or exm_an):
            continue
        flg = str(r.get("flgRelevante", "") or "")
        dt = str(r.get("dataExecucaoModelo", "") or "").strip()
        out.append(
            {
                "id_pct": id_pct,
                "exm_an": exm_an,
                "exm_laudo_texto": laudo,
                "exm_encaminhamento_nlp": _flg_relevante_to_enc(flg),
                "exm_data": dt,
                "exm_mod": "",
                "exm_tipo": "",
                "exm_class": "GOLD_HIVE",
                "exm_frase_selec": "",
            }
        )
    return out


def _map_rows_lake_diamond_hepatologia(raw_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Schema típico: ``tb_diamond_mod_hepatologia_saida`` (lake)."""
    out: list[dict[str, str]] = []
    for raw in raw_rows:
        r = {str(k).strip(): (v if v is not None else "") for k, v in raw.items()}
        id_pct = str(r.get("id_paciente", "") or "").strip()
        exm_an = str(r.get("id_exame", "") or "").strip()
        laudo = str(r.get("proced_laudo_exame") or r.get("laudo_exame") or "")
        flg = str(r.get("fl_relevante", "") or "")
        dt = str(r.get("dt_execucao", "") or "").strip()
        if not (id_pct or exm_an):
            continue
        out.append(
            {
                "id_pct": id_pct,
                "exm_an": exm_an,
                "exm_laudo_texto": laudo,
                "exm_encaminhamento_nlp": _flg_relevante_to_enc(flg),
                "exm_data": dt,
                "exm_mod": "",
                "exm_tipo": "",
                "exm_class": "DIAMOND_LAKE",
                "exm_frase_selec": "",
            }
        )
    return out


def read_diamond_hepatologia(path: Path) -> list[dict[str, str]]:
    """Lê CSV Diamond (lake snake_case ou legado Pulmao-like) e normaliza para o schema interno."""
    with path.open(encoding="utf-8-sig", newline="") as f:
        raw_rows = list(csv.DictReader(f, delimiter=","))
    if not raw_rows:
        return []
    keys = {str(k).strip() for k in raw_rows[0].keys() if k}
    lake_like = "proced_laudo_exame" in keys or (
        "id_exame" in keys and "fl_relevante" in keys and "id_paciente" in keys
    )
    if lake_like:
        return _map_rows_lake_diamond_hepatologia(raw_rows)

    out: list[dict[str, str]] = []
    for raw in raw_rows:
        r = {str(k).strip(): (v if v is not None else "") for k, v in raw.items()}
        enc_txt = str(r.get("exm_encaminhamento_nlp", "") or "").strip().upper()
        if enc_txt in ("S", "N"):
            enc = enc_txt
        else:
            enc = _flg_relevante_to_enc(str(r.get("fl_relevante", "") or ""))
        id_pct = str(r.get("id_pct") or r.get("id_paciente") or "").strip()
        exm_an = str(r.get("exm_an") or r.get("id_exame") or "").strip()
        laudo = str(r.get("exm_laudo_texto") or r.get("Laudo") or r.get("proced_laudo_exame") or "")
        if not (id_pct or exm_an):
            continue
        out.append(
            {
                "id_pct": id_pct,
                "exm_an": exm_an,
                "exm_laudo_texto": laudo,
                "exm_encaminhamento_nlp": enc,
                "exm_data": str(r.get("exm_data") or r.get("dt_exame") or "").strip(),
                "exm_mod": str(r.get("exm_mod") or "").strip(),
                "exm_tipo": str(r.get("exm_tipo") or "").strip(),
                "exm_class": str(r.get("exm_class") or "DIAMOND").strip(),
                "exm_frase_selec": str(r.get("exm_frase_selec") or "").strip(),
            }
        )
    return out


def read_diamond_hepatologia_many(paths: list[Path], *, dedup_id_exame: bool = True) -> list[dict[str, str]]:
    """Varios CSVs lake Diamond concatenados; dedup por ``id_exame`` (``exm_an`` interno)."""
    seen: set[str] = set()
    merged: list[dict[str, str]] = []
    for path in paths:
        for r in read_diamond_hepatologia(path):
            k = (r.get("exm_an") or "").strip()
            if dedup_id_exame and k and k in seen:
                continue
            if k and dedup_id_exame:
                seen.add(k)
            merged.append(r)
    return merged


def read_hive_gold_hepatologia_many(paths: list[Path], *, dedup_id_exame: bool = True) -> list[dict[str, str]]:
    """Vários CSVs com o schema ``saida`` (``laudoExame``, ``flgRelevante``) concatenados; dedup por ``idExame``."""
    seen: set[str] = set()
    merged: list[dict[str, str]] = []
    for path in paths:
        for r in read_hive_gold_hepatologia(path):
            k = (r.get("exm_an") or "").strip()
            if dedup_id_exame and k and k in seen:
                continue
            if k and dedup_id_exame:
                seen.add(k)
            merged.append(r)
    return merged


def _normalize_legacy_paths(legacy: Path | list[Path]) -> list[Path]:
    return [legacy] if isinstance(legacy, Path) else list(legacy)


def _is_positive_row(r: dict[str, str]) -> bool:
    return (r.get("exm_encaminhamento_nlp") or "").strip().upper() == "S"


def _pick_rows_hepatologia(
    rows: list[dict[str, str]],
    *,
    max_rows: int | None = None,
    max_positive: int | None = None,
    max_negative: int | None = None,
    random_seed: int = 42,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Recorte da amostra: proporcional (``max_rows``) ou estratificado S/N."""
    stratified = max_positive is not None or max_negative is not None
    meta: dict[str, Any] = {"mode": "stratified" if stratified else "proportional"}
    if not stratified:
        picked = _pick_rows(rows, max_rows=max_rows)
        meta["max_rows"] = max_rows
        return picked, meta

    if max_positive is None or max_negative is None:
        raise ValueError(
            "amostra estratificada: passe max_positive e max_negative juntos (inteiros >= 0)."
        )

    rng = random.Random(random_seed)
    s_rows = [r for r in rows if _is_positive_row(r)]
    n_rows = [r for r in rows if not _is_positive_row(r)]
    rng.shuffle(s_rows)
    rng.shuffle(n_rows)
    take_s = len(s_rows) if max_positive is None else min(max_positive, len(s_rows))
    take_n = len(n_rows) if max_negative is None else min(max_negative, len(n_rows))
    meta.update(
        {
            "random_seed": random_seed,
            "requested_positive": max_positive,
            "requested_negative": max_negative,
            "available_positive": len(s_rows),
            "available_negative": len(n_rows),
            "picked_positive": take_s,
            "picked_negative": take_n,
        }
    )
    picked = s_rows[:take_s] + n_rows[:take_n]
    rng.shuffle(picked)
    return picked, meta


def run_build(
    *,
    source_legacy_csv: Path | list[Path],
    out_dir: Path,
    max_rows: int | None = None,
    max_positive: int | None = None,
    max_negative: int | None = None,
    random_seed: int = 42,
    source: SourceKind = "diamond",
) -> dict[str, Any]:
    paths = _normalize_legacy_paths(source_legacy_csv)
    for p in paths:
        if not p.is_file():
            raise FileNotFoundError(f"CSV legado nao encontrado: {p}")

    if source == "gold_hive":
        rows = read_hive_gold_hepatologia_many(paths) if len(paths) > 1 else read_hive_gold_hepatologia(paths[0])
    else:
        rows = read_diamond_hepatologia_many(paths) if len(paths) > 1 else read_diamond_hepatologia(paths[0])
    if not rows:
        raise ValueError("CSV legado sem linhas")

    picked, pick_meta = _pick_rows_hepatologia(
        rows,
        max_rows=max_rows,
        max_positive=max_positive,
        max_negative=max_negative,
        random_seed=random_seed,
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    id_pct_counts: Counter[str] = Counter(
        (r.get("id_pct") or "").strip() for r in picked if (r.get("id_pct") or "").strip()
    )

    input_csv = out_dir / "hepatologia_standard_input.csv"
    expected_csv = out_dir / "hepatologia_standard_expected.csv"

    input_fields = [
        "id_exame",
        "id_pct",
        "exm_an",
        "exm_mod",
        "exm_tipo",
        "dt_exame",
        "exm_laudo_texto",
    ]
    expected_fields = [
        "id_exame",
        "id_pct",
        "exm_an",
        "expected_encaminhamento",
        "expected_fl_relevante",
        "expected_behavior",
        "legacy_class",
        "legacy_frase_selec",
    ]

    with input_csv.open("w", encoding="utf-8-sig", newline="") as f_in, expected_csv.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f_exp:
        w_in = csv.DictWriter(f_in, fieldnames=input_fields, delimiter=";")
        w_exp = csv.DictWriter(f_exp, fieldnames=expected_fields, delimiter=";")
        w_in.writeheader()
        w_exp.writeheader()

        for i, r in enumerate(picked):
            id_pct = (r.get("id_pct") or "").strip()
            exm_an = (r.get("exm_an") or "").strip()
            enc = (r.get("exm_encaminhamento_nlp") or "").strip().upper()
            if id_pct and id_pct_counts.get(id_pct, 0) > 1:
                exm_key = exm_an or "noexm"
                id_exame = f"{id_pct}__{exm_key}__{i}"
            else:
                id_exame = id_pct or exm_an

            w_in.writerow(
                {
                    "id_exame": id_exame,
                    "id_pct": id_pct,
                    "exm_an": exm_an,
                    "exm_mod": (r.get("exm_mod") or "").strip(),
                    "exm_tipo": (r.get("exm_tipo") or "").strip(),
                    "dt_exame": (r.get("exm_data") or "").strip(),
                    "exm_laudo_texto": r.get("exm_laudo_texto") or "",
                }
            )
            w_exp.writerow(
                {
                    "id_exame": id_exame,
                    "id_pct": id_pct,
                    "exm_an": exm_an,
                    "expected_encaminhamento": enc,
                    "expected_fl_relevante": _fl_from_enc(enc),
                    "expected_behavior": _behavior_label(enc),
                    "legacy_class": (r.get("exm_class") or "").strip(),
                    "legacy_frase_selec": (r.get("exm_frase_selec") or "").strip(),
                }
            )

    enc_counter = Counter((r.get("exm_encaminhamento_nlp") or "").strip().upper() for r in picked)
    class_counter = Counter((r.get("exm_class") or "").strip() for r in picked)
    summary: dict[str, Any] = {
        "source": source,
        "n_legacy_files": len(paths),
        "legacy_file_paths": [str(p) for p in paths],
        "source_rows": len(rows),
        "sample_rows": len(picked),
        "pick": pick_meta,
        "enc_distribution": dict(enc_counter),
        "top_classes": class_counter.most_common(10),
        "input_csv": str(input_csv),
        "expected_csv": str(expected_csv),
    }
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Gerar amostra padrao Hepatologia para validacao local")
    ap.add_argument(
        "--source",
        choices=["gold_hive", "diamond"],
        default="gold_hive",
        help="Formato do CSV: gold lake Hive (default) ou Diamond legado",
    )
    ap.add_argument(
        "--legacy-csv",
        type=Path,
        action="append",
        dest="legacy_csvs",
        default=None,
        metavar="PATH",
        help="CSV legado (repetir para juntar: gold dedup idExame; diamond lake dedup id_exame). Default gold local.",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path("_local_samples/standard/hepatologia"),
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="0 = usar todas as linhas disponiveis (proporcional S/N). Ignorado se --max-positive/--max-negative.",
    )
    ap.add_argument(
        "--max-positive",
        type=int,
        default=None,
        metavar="N",
        help="No maximo N linhas com encaminhamento S (estratificado). Requer fonte com rotulos S/N.",
    )
    ap.add_argument(
        "--max-negative",
        type=int,
        default=None,
        metavar="N",
        help="No maximo N linhas com encaminhamento N (estratificado).",
    )
    ap.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="Seed para embaralhar antes do recorte estratificado (reprodutibilidade).",
    )
    args = ap.parse_args()

    default_saida = Path(
        "_local_samples/gold/hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida.csv"
    )
    legacy_paths = list(args.legacy_csvs) if args.legacy_csvs else [default_saida]

    summary = run_build(
        source_legacy_csv=legacy_paths[0] if len(legacy_paths) == 1 else legacy_paths,
        out_dir=args.out_dir,
        max_rows=args.max_rows if args.max_rows > 0 else None,
        max_positive=args.max_positive,
        max_negative=args.max_negative,
        random_seed=args.random_seed,
        source=args.source,
    )

    pick = summary.get("pick") or {}
    extra = ""
    if pick.get("mode") == "stratified":
        extra = (
            f" pick_S={pick.get('picked_positive')}/{pick.get('available_positive')} "
            f"pick_N={pick.get('picked_negative')}/{pick.get('available_negative')}"
        )
    print(
        f"files={summary.get('n_legacy_files', 1)} sample_rows={summary['sample_rows']} enc={summary['enc_distribution']}{extra} "
        f"input={summary['input_csv']} expected={summary['expected_csv']}"
    )


if __name__ == "__main__":
    main()
