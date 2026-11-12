#!/usr/bin/env python
"""Valida arquivos da plataforma NLP contra o padrão do time (regra 09-plataforma-padrao-codigo).

Checks determinísticos — o que exige julgamento fica na skill `validar-padrao-plataforma`.

    # arquivos alterados na branch, contra a hml (padrão)
    python .claude/scripts/validar-padrao-plataforma.py --repo _worktrees/motor-regras-estruturadas
    # arquivos específicos
    python .claude/scripts/validar-padrao-plataforma.py --repo fabrica-ia-nlp-platform jobs/definicoes/x.json
    # prova que cada check dispara e que arquivo correto passa
    python .claude/scripts/validar-padrao-plataforma.py --autoteste

Saída: uma linha por achado (ERRO bloqueia, ALERTA pede olhar) e código 1 se houver ERRO.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAIZ = Path(__file__).resolve().parents[2]
CABECALHO = "# Databricks notebook source"
CPF = re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")
BLOCO_MORTO = re.compile(r"""^\s*['"](catalog|monitoring|distribution)['"]\s*:""", re.M)
SPARK = re.compile(r"gold_filter|keywords|rlike|RLIKE|regexp_")
BARRA_B = re.compile(r"""['"][^'"\n]*\\{1,2}b[^'"\n]*['"]""")


@dataclass
class Achado:
    nivel: str
    arquivo: str
    linha: int
    regra: str
    texto: str

    def __str__(self) -> str:
        return f"{self.nivel:6} {self.arquivo}:{self.linha}  [{self.regra}] {self.texto}"


# --- checks por tipo de arquivo ------------------------------------------------------------


def _check_notebook(rel: str, texto: str) -> list[Achado]:
    """Módulo/notebook de `plataform/` (exceto testes): cabeçalho, %md, sem sys.path, dbutils fora do nível do módulo."""
    out: list[Achado] = []
    linhas = texto.splitlines()
    # Módulo de biblioteca (carregado por %run e importado pelos testes): não é runs/ nem notebook de task com widgets.
    biblioteca = "/tools/" in rel and "/runs/" not in rel and "dbutils.widgets" not in texto
    # Cabeçalho %md é o padrão dos módulos de tools/ e dos notebooks de task; pipeline_e2e/, data/ e configs não usam.
    cobra_md = "/tools/" in rel or rel.count("/") == 1
    if not linhas or linhas[0].strip() != CABECALHO:
        out.append(Achado("ERRO", rel, 1, "§1", f"1ª linha deve ser '{CABECALHO}' (código da plataforma é notebook)"))
    if cobra_md and "# MAGIC %md" not in "\n".join(linhas[:12]):
        out.append(Achado("ALERTA", rel, 2, "§1", "sem cabeçalho '# MAGIC %md' (título, papel, alçada, ordem de carga)"))
    for n, txt in enumerate(linhas, 1):
        if re.search(r"\bsys\.path\b", txt) and not txt.lstrip().startswith("#"):
            out.append(Achado("ERRO", rel, n, "§2", "sys.path dentro de plataform/: carregar por %run com caminho literal"))
        if biblioteca and re.match(r"^dbutils\.", txt):
            out.append(Achado("ERRO", rel, n, "§2", "dbutils no nível do módulo em tools/: o teste não consegue executar o notebook"))
    return out


def _check_notebook_de_task(rel: str, texto: str) -> list[Achado]:
    """Notebook de task (raiz de plataform/): células com DBTITLE."""
    celulas = texto.count("# COMMAND ----------")
    titulos = texto.count("# DBTITLE")
    if celulas and titulos < celulas // 2:
        return [Achado("ALERTA", rel, 1, "§1", f"{titulos} DBTITLE para {celulas} células (notebook de task titula as células)")]
    return []


def _check_docstrings(rel: str, texto: str) -> list[Achado]:
    """Funções e classes públicas com docstring."""
    out: list[Achado] = []
    linhas = texto.splitlines()
    for n, txt in enumerate(linhas, 1):
        m = re.match(r"^(?:    )?(def|class) ([A-Za-z]\w*)", txt)
        if not m:
            continue
        j = n
        while j < len(linhas) and not linhas[j - 1].rstrip().endswith(":"):
            j += 1
        prox = linhas[j].strip() if j < len(linhas) else ""
        if not prox.startswith(('"""', "'''")):
            out.append(Achado("ALERTA", rel, n, "§3", f"{m.group(1)} pública '{m.group(2)}' sem docstring"))
    return out


def _check_config_especialidade(rel: str, texto: str) -> list[Achado]:
    """Config da especialidade: sem bloco morto, com cabeçalho e changelog."""
    out = [Achado("ERRO", rel, texto[: m.start()].count("\n") + 1, "§4", f"bloco morto '{m.group(1)}' (o runner não lê)")
           for m in BLOCO_MORTO.finditer(texto)]
    if "changelog" not in texto.lower():
        out.append(Achado("ALERTA", rel, 1, "§4", "sem changelog no arquivo"))
    if "dbutils.notebook.exit" not in texto:
        out.append(Achado("ERRO", rel, 1, "§4", "CONFIG não é devolvido por dbutils.notebook.exit"))
    return out


def _check_regex_spark(rel: str, texto: str) -> list[Achado]:
    """`\\b` em literal de regex: no Spark 18 é ASCII e quebra perto de acento."""
    # Só a regex que vai para o Spark (gold_filter, RLIKE): as das configs do nlp_engine rodam no `re` do Python.
    return [Achado("ALERTA", rel, n, "§6", "\\b em regex que vai para o Spark: usar fronteira explícita (^|[^a-zà-ÿ])")
            for n, txt in enumerate(texto.splitlines(), 1) if SPARK.search(txt) and BARRA_B.search(txt)]


def _check_job(rel: str, texto: str) -> list[Achado]:
    """Definição de job: pause_status obrigatório, versão literal, disabled no envio, env_motor."""
    try:
        job = json.loads(texto)
    except json.JSONDecodeError as e:
        return [Achado("ERRO", rel, e.lineno, "§5", f"JSON inválido: {e.msg}")]
    out: list[Achado] = []
    linha = lambda trecho: next((i for i, txt in enumerate(texto.splitlines(), 1) if trecho in txt), 1)  # noqa: E731
    pause = (job.get("schedule") or {}).get("pause_status")
    if pause is not None and (not re.fullmatch(r"\$\{job_status_\w+_batch\}", str(pause))):
        out.append(Achado("ERRO", rel, linha("pause_status"), "§5",
                          f"pause_status '{pause}': deve ser ${{job_status_<linha>_batch}} sem '?' (spec 31)"))
    for t in job.get("tasks", []):
        params = (t.get("notebook_task") or {}).get("base_parameters") or {}
        versao = params.get("nlp_engine_version")
        if versao is not None and (versao.startswith("${") or versao == "latest"):
            out.append(Achado("ERRO", rel, linha('"nlp_engine_version"'), "§5",
                              f"task {t.get('task_key')}: versão do motor '{versao}' — deve ser literal"))
        if re.match(r"(onedrive|api)_", t.get("task_key", "")) and "disabled" not in t:
            out.append(Achado("ERRO", rel, linha(t["task_key"]), "§5", f"task {t['task_key']} de envio sem 'disabled'"))
        env = params.get("environment")
        if env is not None and env != "${env_motor}":
            out.append(Achado("ALERTA", rel, linha('"environment"'), "§5", f"environment '{env}' (padrão: ${{env_motor}})"))
    return out


def _check_cluster(rel: str, texto: str) -> list[Achado]:
    """Definição de cluster: JSON válido e policy declarada."""
    try:
        json.loads(texto)
    except json.JSONDecodeError as e:
        return [Achado("ERRO", rel, e.lineno, "§5", f"JSON inválido: {e.msg}")]
    if "policy_id" not in texto:
        return [Achado("ALERTA", rel, 1, "§5", "cluster sem policy_id (alçada do Administrador)")]
    return []


def _check_dados(rel: str, texto: str) -> list[Achado]:
    """Dado sensível: CPF formatado e artefatos que não sobem."""
    return [Achado("ERRO", rel, texto[: m.start()].count("\n") + 1, "§7", "CPF formatado no arquivo")
            for m in CPF.finditer(texto)]


def validar_arquivo(rel: str, texto: str) -> list[Achado]:
    """Despacha os checks pelo caminho do arquivo (relativo à raiz do repositório da plataforma)."""
    rel = rel.replace("\\", "/")
    out = _check_dados(rel, texto)
    if "__pycache__" in rel or rel.endswith(".pyc"):
        return out + [Achado("ERRO", rel, 1, "§8", "cache do Python não sobe")]
    if rel.startswith("jobs/definicoes/") and rel.endswith(".json"):
        return out + _check_job(rel, texto)
    if rel.startswith("jobs/clusters/") and rel.endswith(".json"):
        return out + _check_cluster(rel, texto)
    if not (rel.startswith("plataform/") and rel.endswith(".py")) or "/tests/" in rel:
        return out
    out += _check_notebook(rel, texto) + _check_regex_spark(rel, texto)
    if rel.startswith("plataform/config/speciality/"):
        out += _check_config_especialidade(rel, texto)
    elif rel.startswith("plataform/tools/") or rel.startswith("plataform/pipeline_e2e/"):
        out += _check_docstrings(rel, texto)
    elif rel.count("/") == 1:
        out += _check_notebook_de_task(rel, texto)
    return out


# --- execução ------------------------------------------------------------------------------


def arquivos_alterados(repo: Path, base: str) -> list[str]:
    """Arquivos adicionados/alterados contra o merge-base com `base`, mais os não rastreados."""
    def git(*args: str) -> list[str]:
        r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8")
        if r.returncode:
            raise SystemExit(f"git {' '.join(args)} falhou: {r.stderr.strip()}")
        return [x for x in r.stdout.splitlines() if x]
    mb = git("merge-base", "HEAD", base)[0]
    return sorted(set(git("diff", "--name-only", "--diff-filter=AM", mb) + git("ls-files", "--others", "--exclude-standard")))


def autoteste() -> int:
    """Controle positivo (cada check dispara) e negativo (arquivo correto passa) — regra 08 #10."""
    bom_tools = f'{CABECALHO}\n# MAGIC %md\n# MAGIC # X\n\n# COMMAND ----------\n\ndef f():\n    """ok."""\n'
    bom_job = json.dumps({"tasks": [{"task_key": "onedrive_x", "disabled": "${disabled?}",
                                     "notebook_task": {"base_parameters": {"environment": "${env_motor}",
                                                                           "nlp_engine_version": "0.12.3"}}}],
                          "schedule": {"pause_status": "${job_status_x_batch}"}})
    casos = [
        ("negativo tools", "plataform/tools/x/m.py", bom_tools, set()),
        ("negativo job", "jobs/definicoes/x-batch.json", bom_job, set()),
        ("sem cabeçalho", "plataform/tools/x/m.py", '"""doc."""\n', {"§1"}),
        ("sys.path", "plataform/tools/x/m.py", bom_tools + "import sys\nsys.path.insert(0, 'x')\n", {"§2"}),
        ("dbutils no módulo", "plataform/tools/x/m.py", bom_tools + "CFG = 1\ndbutils.notebook.run('./c', 60, {})\n", {"§2"}),
        ("sem docstring", "plataform/tools/x/m.py", bom_tools + "def g():\n    return 1\n", {"§3"}),
        ("bloco morto", "plataform/config/speciality/c.py", f"{CABECALHO}\n# MAGIC %md\nCONFIG = {{\n    'catalog': 'x',\n}}\n# changelog\ndbutils.notebook.exit('{{}}')\n", {"§4"}),
        ("pause opcional", "jobs/definicoes/x-batch.json", bom_job.replace("_batch}", "_batch?}"), {"§5"}),
        ("versão latest", "jobs/definicoes/x-batch.json", bom_job.replace('"0.12.3"', '"latest"'), {"§5"}),
        ("envio sem disabled", "jobs/definicoes/x-batch.json", bom_job.replace('"disabled": "${disabled?}", ', ""), {"§5"}),
        ("\\b em regex Spark", "plataform/tools/x/m.py", bom_tools + "P = \"x RLIKE '\\\\bpes\\\\b'\"\n", {"§6"}),
        ("\\b em regex Python", "plataform/tools/x/m.py", bom_tools + "P = re.compile('\\\\bpes\\\\b')\n", set()),
        ("dbutils em runs/", "plataform/tools/x/runs/r.py", bom_tools + "dbutils.widgets.text('a', '')\n", set()),
        ("pipeline sem %md", "plataform/pipeline_e2e/p.py", f"{CABECALHO}\nimport logging\n", set()),
        ("task sem %md", "plataform/ntb_ia_x.py", f"{CABECALHO}\nimport json\n", {"§1"}),
        ("CPF", "plataform/tools/x/m.py", bom_tools + "# 123.456.789-09\n", {"§7"}),
    ]
    falhas = 0
    for nome, rel, texto, esperado in casos:
        obtido = {a.regra for a in validar_arquivo(rel, texto)}
        ok = obtido == esperado if not esperado else esperado <= obtido
        falhas += not ok
        print(f"{'ok   ' if ok else 'FALHA'} {nome:22} esperado={sorted(esperado)} obtido={sorted(obtido)}")
    print(f"\nautoteste: {len(casos) - falhas}/{len(casos)}")
    return 1 if falhas else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("arquivos", nargs="*", help="caminhos relativos ao repositório; vazio = alterados contra --base")
    ap.add_argument("--repo", default="fabrica-ia-nlp-platform", help="raiz do repositório da plataforma")
    ap.add_argument("--base", default="origin/hml", help="referência para os arquivos alterados")
    ap.add_argument("--autoteste", action="store_true", help="roda os controles positivo e negativo")
    args = ap.parse_args()
    if args.autoteste:
        return autoteste()
    repo = (RAIZ / args.repo).resolve()
    arquivos = args.arquivos or arquivos_alterados(repo, args.base)
    achados = [a for rel in arquivos if (repo / rel).is_file()
               for a in validar_arquivo(rel, (repo / rel).read_text(encoding="utf-8", errors="replace"))]
    for a in sorted(achados, key=lambda a: (a.nivel != "ERRO", a.arquivo, a.linha)):
        print(a)
    erros = sum(a.nivel == "ERRO" for a in achados)
    print(f"\n{len(arquivos)} arquivo(s) · {erros} ERRO · {len(achados) - erros} ALERTA")
    return 1 if erros else 0


if __name__ == "__main__":
    sys.exit(main())
