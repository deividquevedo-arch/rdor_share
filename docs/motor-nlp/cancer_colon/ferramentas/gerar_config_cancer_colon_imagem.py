"""Gera `ntb_ia_cancer_colon_imagem_config.py` a partir da régua de TC do legado (SPEC do card 420309).

Lê o notebook legado `colon/model/ntb_ia_colon_algoritmo.ipynb` (IAAzureDatabricksColon, branch `refatoracao`
@15849354 — a que o job `ia-colon` rodava), acha a célula ativa da régua, extrai `CONFIG` por `ast` (sem executar
nada) e escreve a config da plataforma. Termos, seeds, regex e negadores entram como estão; só `colon_reto`.

Uso:
    python -I gerar_config_cancer_colon_imagem.py <notebook.ipynb> <saida.py> [--negacao left|both]
        [--specialty-id cancer_colon_imagem] [--model-version v0]

`--specialty-id`/`--model-version` existem para a bancada em dev (gravar no schema de outra linha enquanto o schema
`cancer_colon_imagem` não existe); o arquivo do PR usa os padrões.
"""
from __future__ import annotations

import argparse
import ast
import json
import pprint
from pathlib import Path

ORGAO = "colon_reto"
FILTRO = "^(?=.*(tomo|tc))(?=.*(abd|pelve))(?!.*(angio|superior))"
ROTULOS = {"lesao": "Lesão", "polipo": "Pólipo", "ulceracao": "Ulceração", "tumor_ou_massa": "Tumor ou massa"}
VERSAO = "0.1.0"


def celula_ativa(nb_path: Path) -> str:
    """Célula com `CONFIG` e mais de 500 linhas não comentadas (as versões antigas estão inteiras comentadas)."""
    celulas = ["".join(c["source"]) for c in json.loads(nb_path.read_text(encoding="utf-8"))["cells"]]
    ativas = [s for s in celulas if "CONFIG" in s
              and sum(1 for ln in s.splitlines() if ln.strip() and not ln.lstrip().startswith("#")) > 500]
    assert len(ativas) == 1, f"esperada 1 célula ativa com CONFIG, achadas {len(ativas)}"
    return ativas[0]


def extrair(nb_path: Path) -> dict:
    fonte = "\n".join(ln for ln in celula_ativa(nb_path).splitlines() if not ln.lstrip().startswith(("%", "!")))
    alvo = {}
    for no in ast.parse(fonte).body:
        if isinstance(no, ast.Assign) and isinstance(no.targets[0], ast.Name):
            nome = no.targets[0].id
            if nome in ("CONFIG", "TARGET_ORGAN") or (nome == "FORCE_FULL_DOC_FOR" and not isinstance(no.value, ast.Call)):
                alvo[nome] = ast.literal_eval(no.value)
            elif nome == "FORCE_FULL_DOC_FOR":
                alvo[nome] = set()
    assert alvo.get("TARGET_ORGAN") == ORGAO, f"TARGET_ORGAN inesperado: {alvo.get('TARGET_ORGAN')}"
    assert not alvo.get("FORCE_FULL_DOC_FOR"), "FORCE_FULL_DOC_FOR não vazio — revisar a segmentação"
    return alvo["CONFIG"]


def montar(leg: dict, negacao: str, specialty_id: str, model_version: str) -> dict:
    org = leg["organs"][ORGAO]
    achados = leg["findings"][ORGAO]
    assert set(achados) == set(ROTULOS), f"famílias diferentes do esperado: {sorted(achados)}"
    return {
        "specialty_id": specialty_id,
        "config_version": f"{VERSAO}-cancer_colon_imagem",
        "model_version": model_version,
        "description": (
            "Motor NLP de Ca de Cólon por imagem (v0): laudo de TC de abdome/pelve com lesão, pólipo, ulceração ou "
            "tumor/massa no cólon ou reto. Porte fiel da régua de TC do legado (IAAzureDatabricksColon); roda "
            "rule_only (embeddings e juiz LLM desligados). Card 420309."
        ),
        "data": {
            "gold_domains": ["exame.identificacao", "exame.procedimento", "exame.datas", "exame.laudos"],
            "filters": {"gold_filter": {"keywords": [FILTRO], "mode": "any"}},
            "column_map": {
                "id_exame": "id_exame",
                "id_paciente": ["id_paciente", "id_patient"],
                "id_unidade": "id_unidade",
                "exm_laudo_texto": ["proced_laudo_exame_original", "proced_laudo_exame"],
                "exm_mod": ["cod_procedimento", "tp_codigo_procedimento"],
                "exm_tipo": "proced_descricao",
                "dt_exame": "dt_exame",
            },
        },
        "nlp": {
            "target_organs": [ORGAO],
            "organs": {ORGAO: {"seeds": list(org["seeds"]), "regex": list(org["regex"])}},
            "segmentation": {"mode": "full_doc", "force_full_doc_for": []},
            "findings_policy": {"organ": {"scope": "block", "max_chars": 200}},
            "findings": {k: {"label": ROTULOS[k], "terms": list(v), "regex": []} for k, v in achados.items()},
            "negation": {"phrases": list(leg["negation"]["tokens"]), "window": int(leg["negation"]["window_tokens"])},
            "negation_direction": {"_default": negacao},
            "embeddings": {"use_embeddings": False},
            "llm_router": {"enabled": False},
            "feature_flags": {"calibrated_hybrid": False},
        },
        "runtime": {"profile": "rule_only", "llm_router": {"enabled": False}},
    }


CABECALHO = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Config da especialidade `cancer_colon_imagem`
# MAGIC
# MAGIC Define `CONFIG` e o devolve via `dbutils.notebook.exit(json...)`; o runner `ntb_ia_motor_e2e` carrega com
# MAGIC `dbutils.notebook.run`. **Gerada** por `docs/motor-nlp/cancer_colon/ferramentas/gerar_config_cancer_colon_imagem.py`
# MAGIC (repositório de documentação do time de DS) — não editar a régua à mão; regenerar e anotar.
# MAGIC
# MAGIC **O que a linha decide:** laudo de **tomografia de abdome/pelve** entra na captação de Ca de Cólon quando cita
# MAGIC lesão, pólipo, ulceração ou tumor/massa no cólon ou reto, não negado. Colonoscopia NÃO é desta linha
# MAGIC (`cancer_colon`); biópsia também não (motor de regras estruturadas). Card `420309` — *[Ca de Colon] Sem envios
# MAGIC de TC e RM*. SPEC: `docs/motor-nlp/cancer_colon/spec-correcao-420309-cancer-colon-tc-v0.md`.
# MAGIC
# MAGIC **Origem:** job `ia-colon` (workspace antigo), task `algoritmo_tc_rm`, clone
# MAGIC `/Repos/AzureDatabricksMLOps/IAAzureDatabricksColon` na branch **`refatoracao`** (@`15849354`, 05/09/2026).
# MAGIC `colon/model/ntb_ia_colon_algoritmo.ipynb`: a célula ativa da régua é idêntica à da `hml@999bdf39` (as outras
# MAGIC duas versões estão inteiramente comentadas); `CONFIG`, `TARGET_ORGAN = 'colon_reto'`, `FORCE_FULL_DOC_FOR = set()`.
# MAGIC As diferenças entre as branches são só infraestrutura (catálogo por ambiente, cache do modelo em Volume).
# MAGIC Último run do legado: 01/10/2026; job pausado em 02/10 (card `360625`).
# MAGIC
# MAGIC **Regras clínicas NÃO alteradas.** Seeds, regex, achados (4 famílias, 53 termos) e negadores entram como estão,
# MAGIC erros de grafia inclusive (`ulcero infitrativa`, `polipo sesseil`).
# MAGIC
# MAGIC **Adaptações para esta plataforma (motor, não régua):**
# MAGIC
# MAGIC 1. `gold_filter`: o legado filtrava `IMG/IMA` + (`tomografia|tomo|tc`) + (`abd|pelve`) − `angio` − `superior` sobre
# MAGIC    3 colunas; aqui é uma expressão com lookahead sobre `proced_descricao` (única coluna). Medido na gold (exames
# MAGIC    01–28/09/2026): 44.080 selecionados; cobre 43.314 de 44.635 exames do legado (97,0%) e 161 de 163 relevantes.
# MAGIC 2. Eixo de data: o legado janelava por liberação do laudo; a plataforma por `dt_dia_exame`.
# MAGIC 3. Segmentação `full_doc`: o legado tenta cabeçalho (`HEADER_ALIASES`) e cai para âncora de órgão; a lib não tem os
# MAGIC    apelidos e `auto` leria 2–22% do laudo (medido nas biliares). O recorte fica no gate de órgão.
# MAGIC 4. `findings_policy.organ` = bloco + 200 caracteres: gate (A) do legado.
# MAGIC 5. 🔴 Desempate "órgão mais próximo vence" do legado (`_snippet_mentions_other_organs`, 19 órgãos, margem 12
# MAGIC    caracteres) **não tem equivalente na lib**. Por isso os outros órgãos do `CONFIG` legado não foram portados:
# MAGIC    sem a regra eles não teriam efeito. Efeito medido na paridade (changelog).
# MAGIC 6. Negação: 23 negadores, janela 7. O legado lê os dois lados; a direção aqui está no changelog, com o número.
# MAGIC 7. Expansão semântica do legado (`semantic_expand`) exige sobreposição lexical: gera variantes morfológicas, não
# MAGIC    termos novos. `use_embeddings` desligado; variante que faltar entra como `regex`, medida.
# MAGIC 8. Texto: a plataforma lê `proced_laudo_exame_original`; o legado juntava `laudo_transformado`.
# MAGIC
# MAGIC ## Changelog
# MAGIC
# MAGIC ### `0.1.0-cancer_colon_imagem` (26/11/2026)
# MAGIC Porte inicial, gerado do legado. Medição contra o legado: ver a seção de medição abaixo (preenchida no run).

# COMMAND ----------

'''

RODAPE = '''
# COMMAND ----------

import json  # noqa: E402

try:
    dbutils.notebook.exit(json.dumps(CONFIG, ensure_ascii=False))  # noqa: F821 - dbutils so existe no Databricks
except NameError:
    pass
'''


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("notebook", type=Path)
    ap.add_argument("saida", type=Path)
    ap.add_argument("--negacao", choices=["left", "both"], default="left")
    ap.add_argument("--specialty-id", default="cancer_colon_imagem")
    ap.add_argument("--model-version", default="v0")
    a = ap.parse_args()
    cfg = montar(extrair(a.notebook), a.negacao, a.specialty_id, a.model_version)
    corpo = "CONFIG = " + pprint.pformat(cfg, width=118, sort_dicts=False) + "\n"
    a.saida.write_text(CABECALHO + corpo + RODAPE, encoding="utf-8", newline="\n")
    n = sum(len(f["terms"]) for f in cfg["nlp"]["findings"].values())
    print(f"ok {a.saida.name}: {len(cfg['nlp']['findings'])} famílias, {n} termos, "
          f"{len(cfg['nlp']['organs'][ORGAO]['seeds'])} seeds, {len(cfg['nlp']['organs'][ORGAO]['regex'])} regex, "
          f"{len(cfg['nlp']['negation']['phrases'])} negadores, direção {a.negacao}")


if __name__ == "__main__":
    main()
