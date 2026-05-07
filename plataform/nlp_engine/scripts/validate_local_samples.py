#!/usr/bin/env python3
"""Validacao em lote sobre ``_local_samples/`` (S01 paridade + S02 auditoria).

Percorre recursivamente ``*.csv`` / ``*.tsv`` sob a raiz (default ``_local_samples``),
deteta colunas e executa o que for aplicavel:

* **Paridade S01:** cabecalho com ``Laudo`` e ``laudo_tratado`` (nomes exactos do export HML).
* **Auditoria S02:** coluna de texto ``exm_laudo_texto`` ou ``Laudo`` ou ``laudo_texto``.

Exemplos (a partir de ``plataform/nlp_engine``):

    .venv\\Scripts\\python.exe scripts\\validate_local_samples.py
    .venv\\Scripts\\python.exe scripts\\validate_local_samples.py --write-audits --min-parity-rate 0.95
    .venv\\Scripts\\python.exe scripts\\validate_local_samples.py --root _local_samples\\diamond --no-audit
    .venv\\Scripts\\python.exe scripts\\validate_local_samples.py --verbose
    .venv\\Scripts\\python.exe scripts\\validate_local_samples.py --config-yaml ..\\..\\colon_config.yaml
"""

from __future__ import annotations

import argparse
import csv
import runpy
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _sniff_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        return ";"


def _peek_fieldnames(path: Path) -> list[str] | None:
    with path.open(encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    if not sample.strip():
        return None
    delim = _sniff_delimiter(sample)
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        if not reader.fieldnames:
            return None
        return [str(h) for h in reader.fieldnames]


def _is_validate_export_artifact(p: Path) -> bool:
    """Evita reprocessar saidas de corridas anteriores (exports/validate_*/*_audit.csv)."""
    parts_lower = [x.lower() for x in p.parts]
    if "exports" not in parts_lower:
        return False
    i = parts_lower.index("exports")
    if i + 1 < len(parts_lower) and parts_lower[i + 1].startswith("validate_"):
        return True
    return False


def _discover_csvs(root: Path) -> list[Path]:
    out: list[Path] = []
    seen: set[Path] = set()
    if not root.is_dir():
        return out
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".csv", ".tsv"):
            continue
        if "__pycache__" in p.parts or ".pytest_cache" in p.parts or ".ruff_cache" in p.parts:
            continue
        if _is_validate_export_artifact(p):
            continue
        r = p.resolve()
        if r not in seen:
            seen.add(r)
            out.append(p)
    return sorted(out)


def _load_script_functions() -> tuple[object, object]:
    base = Path(__file__).resolve().parent
    rtp = runpy.run_path(str(base / "report_to_plain_parity.py"))
    aud = runpy.run_path(str(base / "audit_engine_from_csv.py"))
    return rtp["run_parity"], aud["run_audit"]


@dataclass
class FileResult:
    rel: str
    parity: str
    parity_rate: str
    audit: str
    audit_rows: str
    note: str


def main() -> None:
    ap = argparse.ArgumentParser(description="Validar CSVs em _local_samples (paridade + auditoria)")
    ap.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Raiz a percorrer (default: _local_samples junto a esta lib)",
    )
    ap.add_argument("--max-rows", type=int, default=50_000, help="Max linhas por ficheiro (paridade e audit)")
    ap.add_argument("--no-parity", action="store_true", help="Nao correr paridade S01")
    ap.add_argument("--no-audit", action="store_true", help="Nao correr auditoria S02")
    ap.add_argument(
        "--write-audits",
        action="store_true",
        help="Gravar CSVs de auditoria em _local_samples/exports/validate_<timestamp>/",
    )
    ap.add_argument(
        "--min-parity-rate",
        type=float,
        default=None,
        help="Se definido, exit code 1 se algum ficheiro com paridade tiver taxa inferior",
    )
    ap.add_argument("--config-yaml", type=Path, default=None, help="YAML de especialidade para auditoria (opcional)")
    ap.add_argument(
        "--validate-output-invariants",
        action="store_true",
        help="Auditoria: valida cada linha com output_invariants apos process (falha na primeira violacao)",
    )
    ap.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Imprime o primeiro diff de paridade (to_plain vs laudo_tratado) por ficheiro com mismatch",
    )
    ap.add_argument(
        "--strip-rtf-notice",
        action="store_true",
        help="Paridade: remove linha de aviso RTF/WebRIS em ambos os lados (ver report_to_plain_parity.py)",
    )
    ap.add_argument(
        "--normalize-rtf-list-markers",
        action="store_true",
        help="Paridade: trata '\\-' como '- ' no inicio da linha (lista RTF vs plain)",
    )
    ap.add_argument(
        "--parity-relaxed",
        action="store_true",
        help="Paridade: --strip-rtf-notice + --normalize-rtf-list-markers",
    )
    args = ap.parse_args()

    if args.config_yaml is not None and not args.config_yaml.is_file():
        raise SystemExit(
            f"--config-yaml nao aponta para um ficheiro existente: {args.config_yaml}\n"
            "Use um caminho real (ex.: Resolve-Path para YAML na raiz do repo de plataforma)."
        )

    engine_root = Path(__file__).resolve().parents[1]
    root = args.root if args.root is not None else engine_root / "_local_samples"
    root = root.resolve()

    run_parity, run_audit = _load_script_functions()

    strip_rtf = args.strip_rtf_notice or args.parity_relaxed
    norm_bullets = args.normalize_rtf_list_markers or args.parity_relaxed

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") if args.write_audits else ""
    audit_base = engine_root / "_local_samples" / "exports" / f"validate_{stamp}" if args.write_audits else None

    results: list[FileResult] = []
    parity_rates: list[float] = []

    paths = _discover_csvs(root)
    if not paths:
        print(f"Nenhum CSV/TSV encontrado sob: {root}")
        print("Coloque exports em _local_samples/diamond|exports|gold (gitignored).")
        sys.exit(0)

    for path in paths:
        try:
            rel = str(path.relative_to(root))
        except ValueError:
            rel = path.name
        fnames = _peek_fieldnames(path)
        if not fnames:
            results.append(FileResult(rel, "skip", "-", "skip", "-", "sem cabecalho"))
            continue
        keys = {f.lower(): f for f in fnames}
        has_laudo = "laudo" in keys
        has_tratado = "laudo_tratado" in keys
        has_text = has_laudo or "exm_laudo_texto" in keys or "laudo_texto" in keys

        p_note = ""
        p_rate_str = "-"
        parity_status = "skip"
        if not args.no_parity and has_laudo and has_tratado:
            try:
                s = run_parity(
                    path,
                    max_rows=args.max_rows,
                    strip_rtf_notice=strip_rtf,
                    normalize_rtf_list_markers=norm_bullets,
                )
                parity_status = "ok" if s.n_mismatch == 0 else "mismatch"
                p_rate_str = f"{s.match_rate:.4f}"
                if s.n_compared > 0:
                    parity_rates.append(s.match_rate)
                if s.first_mismatch:
                    p_note = "usar --verbose para ver diff" if not args.verbose else ""
                    if args.verbose and s.first_mismatch:
                        print(f"\n--- [verbose] paridade: {rel} ---\n{s.first_mismatch}\n", file=sys.stderr)
            except Exception as e:
                parity_status = "error"
                p_note = str(e)[:120]
        elif not args.no_parity:
            parity_status = "skip"
            p_note = "falta Laudo+laudo_tratado"

        a_note = ""
        audit_status = "skip"
        audit_rows_str = "-"
        if not args.no_audit and has_text:
            if audit_base is not None:
                safe = rel.replace("\\", "_").replace("/", "_").replace(":", "_")
                out_path = audit_base / f"{safe}_audit.csv"
                try:
                    n = run_audit(
                        path,
                        out_path,
                        max_rows=args.max_rows,
                        config_yaml=args.config_yaml,
                        engine_version="validate-local-samples",
                        validate_output_invariants=args.validate_output_invariants,
                    )
                    audit_status = "ok"
                    audit_rows_str = str(n)
                except Exception as e:
                    audit_status = "error"
                    a_note = str(e)[:120]
            else:
                try:
                    with tempfile.TemporaryDirectory(dir=str(engine_root)) as td:
                        tmp_path = Path(td) / "_audit_once.csv"
                        n = run_audit(
                            path,
                            tmp_path,
                            max_rows=args.max_rows,
                            config_yaml=args.config_yaml,
                            engine_version="validate-local-samples",
                            validate_output_invariants=args.validate_output_invariants,
                        )
                    audit_status = "ok"
                    audit_rows_str = str(n)
                except Exception as e:
                    audit_status = "error"
                    a_note = str(e)[:120]
        elif not args.no_audit:
            audit_status = "skip"
            a_note = "sem coluna de texto"

        note = "; ".join(x for x in (p_note, a_note) if x)
        results.append(
            FileResult(rel, parity_status, p_rate_str, audit_status, audit_rows_str, note)
        )

    w = max(len(r.rel) for r in results) + 2
    print(f"Raiz: {root}")
    print(f"Ficheiros: {len(paths)}")
    print()
    hdr = f"{'ficheiro':<{w}} {'parity':<10} {'taxa':<8} {'audit':<8} {'linhas':<8} nota"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        print(
            f"{r.rel:<{w}} {r.parity:<10} {r.parity_rate:<8} {r.audit:<8} {r.audit_rows:<8} {r.note}"
        )

    if args.write_audits and audit_base is not None:
        print()
        print(f"Auditorias gravadas em: {audit_base.resolve()}")

    if args.min_parity_rate is not None:
        bad = [pr for pr in parity_rates if pr < args.min_parity_rate]
        if bad:
            print()
            print(f"ERRO: {len(bad)} execucao(oes) de paridade abaixo de {args.min_parity_rate}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
