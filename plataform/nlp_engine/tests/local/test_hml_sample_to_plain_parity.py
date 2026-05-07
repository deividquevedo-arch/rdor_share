"""Paridade opcional: CSV sintetico HML (`Laudo` -> `to_plain`) vs `laudo_tratado`.

Nao commitar o CSV. Defina ``NLP_HML_LAUDOS_CSV`` (caminho absoluto) para executar.
Sem a variavel, o teste e ignorado e a CI permanece verde.
Separador: ``;``, ``,`` ou TAB (detetado automaticamente a partir do cabecalho).

``NLP_HML_MAX_ROWS`` (opcional): inteiro positivo; avalia apenas as primeiras N linhas de dados
apos o cabecalho (util para CSVs Diamond grandes).

``NLP_HML_PARITY_REPORT_ONLY=1`` (opcional): em vez de falhar na primeira divergencia agregada,
emite um **relatorio** (taxa de match) no stdout. Use com ``pytest -s`` para ver o texto.
Opcional: ``NLP_HML_MIN_MATCH_RATE`` (0..1) — no modo relatorio, falha se a taxa for inferior.

Limitacoes:
- Compara com o texto gravado em ``laudo_tratado`` (proxy da saida HML na amostra).
- Divergencias por Pandoc/footer/config nao alinhados sao esperadas ate paridade total.
"""

from __future__ import annotations

import csv
import difflib
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from nlp_engine.text_pipeline import to_plain

_CSV_ENV = "NLP_HML_LAUDOS_CSV"
_STRICT_ENV = "NLP_HML_PARITY_STRICT"
_MAX_ROWS_ENV = "NLP_HML_MAX_ROWS"
_REPORT_ONLY_ENV = "NLP_HML_PARITY_REPORT_ONLY"
_MIN_RATE_ENV = "NLP_HML_MIN_MATCH_RATE"
_MAX_DIFF_LINES = 40
_MAX_SNIP = 400


def _norm_for_compare(s: str, *, strict: bool) -> str:
    """Normaliza texto antes de comparar ``to_plain`` com ``laudo_tratado``.

    Modo **relaxado** (default): alinha comparacao com export HML que remove linhas em branco
    extras e espacos horizontais irregulares entre ``Laudo`` e ``laudo_tratado``.

    - ``\\r\\n`` / ``\\r`` -> ``\\n``
    - modo relaxado: cada linha como ``' '.join(line.split())``; linhas vazias omitidas;
      junta com ``\\n`` simples.
    - modo **strict** (``NLP_HML_PARITY_STRICT=1``): apenas normaliza quebras de linha e
      ``strip()`` no bloco inteiro (sem colapso de linhas vazias).
    """
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    if strict:
        return t.strip()
    lines_out: list[str] = []
    for raw in t.split("\n"):
        part = " ".join(raw.split())
        if part:
            lines_out.append(part)
    return "\n".join(lines_out)


def _sniff_delimiter(path: Path) -> str:
    with path.open(encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    except csv.Error:
        return ";"


def _read_laudo_rows(path: Path) -> list[dict[str, str]]:
    delim = _sniff_delimiter(path)
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=delim))


def _max_rows_limit() -> int | None:
    raw = os.environ.get(_MAX_ROWS_ENV, "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return n if n > 0 else None


def _apply_max_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    lim = _max_rows_limit()
    if lim is None:
        return rows
    return rows[:lim]


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class ParityBatchResult:
    """Resultado de comparar ``to_plain(Laudo)`` com ``laudo_tratado`` num CSV."""

    path: Path
    strict_norm: bool
    n_rows_in_batch: int
    n_skipped_empty: int
    n_compared: int
    n_match: int
    n_mismatch: int
    mismatch_samples: list[str]

    @property
    def match_rate(self) -> float:
        if self.n_compared == 0:
            return 0.0
        return self.n_match / self.n_compared


def _run_to_plain_parity_batch(path: Path, *, strict: bool) -> ParityBatchResult:
    rows = _apply_max_rows(_read_laudo_rows(path))
    skipped = 0
    mismatches: list[str] = []
    n_match = 0
    n_compared = 0

    for i, row in enumerate(rows, start=1):
        laudo = (row.get("Laudo") or "").strip()
        tratado = (row.get("laudo_tratado") or "").strip()
        if not laudo or not tratado:
            skipped += 1
            continue

        n_compared += 1
        got = to_plain(laudo)
        exp_n = _norm_for_compare(tratado, strict=strict)
        got_n = _norm_for_compare(got, strict=strict)
        if exp_n == got_n:
            n_match += 1
            continue

        excerpt = _diff_excerpt(exp_n, got_n)
        snip_exp = exp_n[:_MAX_SNIP].replace("\n", "\\n")
        snip_got = got_n[:_MAX_SNIP].replace("\n", "\\n")
        mismatches.append(
            f"Registro {i}/{len(rows)} (ordem no CSV apos cabecalho):\n"
            f"  esperado (inicio): {snip_exp!r}\n"
            f"  obtido   (inicio): {snip_got!r}\n"
            f"  diff (truncado):\n{excerpt[:_MAX_DIFF_LINES * 50]}"
        )

    return ParityBatchResult(
        path=path,
        strict_norm=strict,
        n_rows_in_batch=len(rows),
        n_skipped_empty=skipped,
        n_compared=n_compared,
        n_match=n_match,
        n_mismatch=len(mismatches),
        mismatch_samples=mismatches,
    )


def _diff_excerpt(a: str, b: str) -> str:
    a_lines = (a + "\n").splitlines(keepends=True)
    b_lines = (b + "\n").splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            a_lines,
            b_lines,
            fromfile="laudo_tratado(norm)",
            tofile="to_plain(norm)",
            n=2,
            lineterm="\n",
        )
    )[:2000]


@pytest.mark.skipif(
    not os.environ.get(_CSV_ENV),
    reason=f"Defina {_CSV_ENV} com o caminho do CSV HML sintetico para executar.",
)
@pytest.mark.skipif(
    _env_flag(_REPORT_ONLY_ENV),
    reason=f"Modo relatorio ({_REPORT_ONLY_ENV}); corra test_hml_sample_to_plain_parity_legacy_report.",
)
def test_hml_sample_to_plain_parity_vs_laudo_tratado() -> None:
    path = Path(os.environ[_CSV_ENV]).expanduser().resolve()
    assert path.is_file(), f"Ficheiro nao encontrado: {path}"

    strict = _env_flag(_STRICT_ENV)
    batch = _run_to_plain_parity_batch(path, strict=strict)
    assert batch.n_rows_in_batch, "CSV sem linhas de dados"

    if batch.n_mismatch:
        msg = (
            f"Paridade to_plain vs laudo_tratado: {batch.n_mismatch} falha(s) "
            f"em {batch.n_rows_in_batch} linha(s) no lote "
            f"(comparadas: {batch.n_compared}; ignoradas sem Laudo/tratado: {batch.n_skipped_empty}).\n"
            f"Modo strict={strict} ({_STRICT_ENV}).\n---\n"
            + "\n---\n".join(batch.mismatch_samples[:5])
        )
        if len(batch.mismatch_samples) > 5:
            msg += f"\n---\n... e mais {len(batch.mismatch_samples) - 5} falha(s)."
        pytest.fail(msg)


@pytest.mark.skipif(
    not os.environ.get(_CSV_ENV),
    reason=f"Defina {_CSV_ENV} com o caminho do CSV para executar.",
)
@pytest.mark.skipif(
    not _env_flag(_REPORT_ONLY_ENV),
    reason=f"Defina {_REPORT_ONLY_ENV}=1 para relatorio de paridade vs legado (stdout).",
)
def test_hml_sample_to_plain_parity_legacy_report() -> None:
    """Valida amostra contra legado: imprime taxa de match; opcional minimo via env."""
    path = Path(os.environ[_CSV_ENV]).expanduser().resolve()
    assert path.is_file(), f"Ficheiro nao encontrado: {path}"

    strict = _env_flag(_STRICT_ENV)
    batch = _run_to_plain_parity_batch(path, strict=strict)

    rate = batch.match_rate
    lines = [
        "=== Paridade text_pipeline (to_plain) vs laudo_tratado (legado) ===",
        f"Ficheiro: {batch.path}",
        f"Linhas no lote (apos {_MAX_ROWS_ENV}): {batch.n_rows_in_batch}",
        f"Ignoradas (Laudo ou laudo_tratado vazio): {batch.n_skipped_empty}",
        f"Comparadas: {batch.n_compared}",
        f"Match: {batch.n_match}",
        f"Mismatch: {batch.n_mismatch}",
        f"Taxa de match: {rate:.4f} ({batch.n_match}/{batch.n_compared})",
        f"Normalizacao strict={batch.strict_norm} ({_STRICT_ENV})",
    ]
    report = "\n".join(lines)
    print(report)

    if batch.n_compared == 0:
        pytest.fail("Nenhuma linha com Laudo e laudo_tratado preenchidos para comparar.")

    raw_min = os.environ.get(_MIN_RATE_ENV, "").strip()
    if raw_min:
        try:
            min_rate = float(raw_min)
        except ValueError as exc:
            raise AssertionError(f"{_MIN_RATE_ENV} invalido: {raw_min!r}") from exc
        if not 0.0 <= min_rate <= 1.0:
            pytest.fail(f"{_MIN_RATE_ENV} deve estar entre 0 e 1, obteve {min_rate}")
        assert rate + 1e-12 >= min_rate, (
            f"Taxa {rate:.4f} abaixo do minimo {_MIN_RATE_ENV}={min_rate} "
            f"({batch.n_mismatch} mismatches em {batch.n_compared} comparadas)"
        )

    if batch.mismatch_samples:
        tail = "\n---\n".join(batch.mismatch_samples[:3])
        more = ""
        if len(batch.mismatch_samples) > 3:
            more = f"\n---\n... e mais {len(batch.mismatch_samples) - 3} exemplo(s) de mismatch."
        print(f"\n--- Primeiros exemplos de mismatch ---\n{tail}{more}")


def test_run_to_plain_parity_batch_counts(tmp_path: Path) -> None:
    p = tmp_path / "parity.csv"
    p.write_text("Laudo,laudo_tratado\nhello,hello\nx,y\n,\n", encoding="utf-8")
    r = _run_to_plain_parity_batch(p, strict=False)
    assert r.n_rows_in_batch == 3
    assert r.n_skipped_empty == 1
    assert r.n_compared == 2
    assert r.n_match == 1
    assert r.n_mismatch == 1
    assert len(r.mismatch_samples) == 1


def test_apply_max_rows_truncates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_MAX_ROWS_ENV, "2")
    rows = [{"Laudo": "a", "laudo_tratado": "a"}] * 5
    assert len(_apply_max_rows(rows)) == 2


def test_apply_max_rows_invalid_env_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_MAX_ROWS_ENV, "not-a-number")
    rows = [{"Laudo": "a", "laudo_tratado": "a"}]
    assert _apply_max_rows(rows) is rows


def test_read_laudo_rows_sniffs_comma_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "comma.csv"
    p.write_text("id,Laudo,laudo_tratado\n1,alfa,beta\n", encoding="utf-8")
    rows = _read_laudo_rows(p)
    assert len(rows) == 1
    assert rows[0].get("Laudo") == "alfa"
    assert rows[0].get("laudo_tratado") == "beta"


def test_read_laudo_rows_sniffs_semicolon_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "semi.csv"
    p.write_text("id;Laudo;laudo_tratado\n1;alfa;beta\n", encoding="utf-8")
    rows = _read_laudo_rows(p)
    assert len(rows) == 1
    assert rows[0].get("Laudo") == "alfa"
    assert rows[0].get("laudo_tratado") == "beta"


def test_read_laudo_rows_sniffs_tab_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "tab.tsv"
    p.write_text("id\tLaudo\tlaudo_tratado\n1\talfa\tbeta\n", encoding="utf-8")
    rows = _read_laudo_rows(p)
    assert len(rows) == 1
    assert rows[0].get("Laudo") == "alfa"
    assert rows[0].get("laudo_tratado") == "beta"
