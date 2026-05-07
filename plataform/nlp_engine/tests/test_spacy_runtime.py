"""Valida o extra [dev]: spaCy importa e pipeline minimo ``blank('pt')`` (sem modelo externo).

Import dentro do teste para nao falhar na recolha do pytest se thinc/spacy estiverem partidos
(p.ex. DLL ``numpy_ops`` no Windows).
"""

from __future__ import annotations

from types import ModuleType

import pytest


def _import_spacy_or_fail() -> ModuleType:
    try:
        import spacy
    except ImportError as e:
        pytest.fail(
            "spacy nao importou (cadeia thinc/numpy_ops). No Windows, falha de DLL e comum: "
            "instale o Microsoft Visual C++ Redistributable (x64), depois tente "
            "`pip install --force-reinstall --no-cache-dir thinc spacy`. "
            f"Erro: {e}"
        )
    return spacy


def test_spacy_runtime_import_version_and_blank_pt() -> None:
    spacy = _import_spacy_or_fail()
    assert getattr(spacy, "__version__", None)

    nlp = spacy.blank("pt")
    nlp.add_pipe("sentencizer")
    doc = nlp("O figado esta normal. Bexiga cheia.")
    assert len(list(doc.sents)) >= 2
