"""Validacao e normalizacao da secao `nlp` recebida como dict (sem I/O de ficheiro)."""


def load(config: dict) -> dict:
    if not isinstance(config, dict):
        raise TypeError("config must be a dict")
    return config
