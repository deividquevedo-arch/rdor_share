import nlp_engine


def test_version_is_semver_string() -> None:
    assert isinstance(nlp_engine.__version__, str)
    parts = nlp_engine.__version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)
