"""Testes do modulo boilerplate (S01 T01.4), dados sinteticos sem PHI."""

from __future__ import annotations

from nlp_engine.text_pipeline import (
    drop_boilerplate_lines,
    strip_trailing_line_patterns,
    to_plain,
)


def test_drop_empty() -> None:
    assert drop_boilerplate_lines("") == ""


def test_drop_font_table_like_line() -> None:
    raw = "Corpo do laudo limpo.\ntimes new roman; arial; calibri; symbol; wingdings;"
    out = drop_boilerplate_lines(raw)
    assert "Corpo do laudo limpo" in out
    assert "times new roman" not in out.lower()


def test_drop_heading_enum_line() -> None:
    raw = "Texto util.\nheading 1; heading 2; heading 3;"
    assert "Texto util" in drop_boilerplate_lines(raw)
    assert "heading 1" not in drop_boilerplate_lines(raw).lower()


def test_drop_generator_metadata_line() -> None:
    raw = "Achado descritivo.\ncreated by html to rtf converter"
    out = drop_boilerplate_lines(raw)
    assert "Achado" in out
    assert "created by" not in out.lower()


def test_drop_generic_viewer_notice_line() -> None:
    raw = "Secao tecnica.\nLaudo parcial: favor visualizar no viewer do sistema."
    out = drop_boilerplate_lines(raw)
    assert "Laudo parcial" not in out
    assert "Secao tecnica" in out


def test_strip_trailing_crm_lines_config_patterns() -> None:
    raw = "Pancreas sem alteracoes.\nDr. Sintetico Silva\nCRM MG 123456"
    # Apos "dr." nao use \b: o ponto nao e \\w, entao nao ha word boundary antes do espaco.
    patterns = [r"(?i)^\s*(dr\.|dra\.|doutor|doutora)(?=\s|$)", r"(?i)\bCRM\b"]
    out = strip_trailing_line_patterns(raw, patterns)
    assert "Pancreas" in out
    assert "CRM" not in out
    assert "Sintetico" not in out


def test_strip_trailing_only_peels_from_end() -> None:
    raw = "Achado com mencao de CRM em hipotese clinica.\nAssinatura sintetica\nCRM XX 000000"
    patterns = [r"(?i)\bCRM\b"]
    out = strip_trailing_line_patterns(raw, patterns)
    assert "hipotese" in out
    assert "000000" not in out
    assert "Assinatura sintetica" in out


def test_to_plain_trailing_line_patterns_optional() -> None:
    raw = "Exame estavel sem alteracoes.\nDr. Exemplo Sintetico\nCRM RJ 111222"
    out = to_plain(
        raw,
        trailing_line_patterns=[r"(?i)^\s*dr\.", r"(?i)\bCRM\b"],
    )
    assert "estavel" in out.lower()
    assert "crm" not in out.lower()
    assert "sintetico" not in out.lower()