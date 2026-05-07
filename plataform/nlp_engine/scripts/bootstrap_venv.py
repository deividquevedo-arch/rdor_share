"""Cria .venv e instala o pacote em modo editable (evita problemas de quoting no shell)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    if sys.version_info[:2] != (3, 12):
        raise SystemExit(
            "nlp_engine requer Python 3.12.x. Use por exemplo: py -3.12 scripts/bootstrap_venv.py "
            f"(atual: {sys.version_info.major}.{sys.version_info.minor})."
        )
    root = Path(__file__).resolve().parent.parent
    venv_dir = root / ".venv"
    py = venv_dir / "Scripts" / "python.exe"

    if not venv_dir.is_dir():
        subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
    if not py.is_file():
        raise SystemExit(f"python em venv inexistente: {py}")

    subprocess.run([str(py), "-m", "pip", "install", "--upgrade", "pip"], check=True)
    subprocess.run(
        [str(py), "-m", "pip", "install", "-e", f"{root}[dev]"],
        check=True,
    )
    print("OK: .venv com nlp_engine editable (.[dev]; inclui spacy para testes/ancoras).")
    print(f"Interpretador do venv: {py}")
    print("Use este Python para ruff/pytest (nao o global):")
    print(f'  "{py}" -m ruff check .')
    print(f'  "{py}" -m pytest tests -q')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
