"""Testes sinteticos para segmentacao por cabecalhos (S01 T01.2); sem PHI."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from nlp_engine.text_pipeline.by_headers import (
    extract_section_lines,
    segment_by_headers_plain,
)


@pytest.fixture
def sample_aliases() -> dict[str, tuple[str, ...]]:
    return {
        "figado": ("figado", "fígado"),
        "vias_biliares": ("vias biliares", "vias biliares"),
    }


def test_segment_empty_returns_empty() -> None:
    assert segment_by_headers_plain("", ["figado"], {}) == []
    assert segment_by_headers_plain("linha sem padrao de titulo.", [], {}) == []


def test_segment_no_matching_header(sample_aliases: dict[str, tuple[str, ...]]) -> None:
    raw = "Texto livre sem estrutura.\nMais linhas."
    assert segment_by_headers_plain(raw, ["figado"], sample_aliases) == []


def test_segment_two_sections(sample_aliases: dict[str, tuple[str, ...]]) -> None:
    raw = (
        "FIGADO: achado sintetico A.\n"
        "VIAS BILIARES: achado sintetico B.\n"
        "CONCLUSAO: texto final.\n"
    )
    blocks = segment_by_headers_plain(raw, ["figado", "vias_biliares"], sample_aliases)
    assert len(blocks) == 2
    assert blocks[0]["organ"] == "figado"
    assert "achado sintetico A" in blocks[0]["text"]
    assert blocks[0]["start"] < blocks[0]["end"]
    assert blocks[1]["organ"] == "vias_biliares"
    assert "achado sintetico B" in blocks[1]["text"]
    assert blocks[1]["end"] <= len(raw)


def test_default_alias_is_organ_key() -> None:
    # Nome do cabecalho segue o charset do HEADER_RX (sem underscore).
    raw = "ALVO: conteudo unico.\nOUTRO: outro.\n"
    blocks = segment_by_headers_plain(raw, ["alvo"], {})
    assert len(blocks) == 1
    assert blocks[0]["organ"] == "alvo"
    assert "conteudo unico" in blocks[0]["text"].lower()


def test_extract_section_lines(sample_aliases: dict[str, tuple[str, ...]]) -> None:
    raw = "FIGADO: linha um.\nlinha dois.\nVIAS: ignorar.\n"
    lines = extract_section_lines(raw, "figado")
    assert any("linha um" in ln for ln in lines)
    assert any("linha dois" in ln for ln in lines)


def test_bullet_header(sample_aliases: dict[str, tuple[str, ...]]) -> None:
    raw = "- FIGADO: texto apos bullet.\nOUTRA: secao\n"
    blocks = segment_by_headers_plain(raw, ["figado"], sample_aliases)
    assert len(blocks) == 1
    assert "texto apos bullet" in blocks[0]["text"]


_FIXTURE_YAML = Path(__file__).resolve().parent.parent / "fixtures" / (
    "text_pipeline_header_aliases_minimal.yaml"
)


def test_segment_with_header_aliases_from_yaml_fixture() -> None:
    """O notebook faria load YAML e injetaria dict; aqui o teste carrega a fixture."""
    payload = yaml.safe_load(_FIXTURE_YAML.read_text(encoding="utf-8"))
    assert payload.get("config_version")
    aliases = payload["nlp"]["header_aliases"]
    raw = "FIGADO: trecho A.\nVIAS BILIARES: trecho B.\n"
    blocks = segment_by_headers_plain(raw, ["figado", "vias_biliares"], aliases)
    assert len(blocks) == 2
    assert blocks[0]["organ"] == "figado"
    assert blocks[1]["organ"] == "vias_biliares"
