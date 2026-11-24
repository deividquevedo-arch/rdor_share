"""Historico mensal v2: mesmo universo da v1, com duas correcoes de precisao.

1. Mes pela DATA DO EXAME, alem da data de execucao. A v1 atribuia o exame ao mes da primeira
   execucao do modelo, e carga retroativa (estreia de linha, reprocessamento) inflava o mes da carga.
2. Ateromatose legado pelo espelho `diamond_fabrica_ia.legado`, que vai ate 01/10 (62 dias de
   execucao). A copia `diamond_fabrica_ia_hml.legado` usada na v1 para em 02/09 e criava uma lacuna
   falsa em setembro. Nas demais linhas a copia hml e superconjunto do espelho (conferido em 19/11).

Gera `mensal-por-linha-unidade-v2.json` com {linha, fonte, recorte, mes_execucao, mes_exame,
id_unidade, exames, relevantes}. Um exame conta uma vez: data do exame = a menor registrada;
mes_execucao = mes da primeira execucao; unidade = a da primeira execucao.
So agregados saem do warehouse.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date

import extrair_historico as v1

DATA_EXAME_PLATAFORMA = "to_date(substr(cast(dt_exame as string), 1, 10))"
ESPELHO_ATERO = "diamond_fabrica_ia.legado.tb_diamond_mod_ateromatose_coronariana_saida"
HEPATO_JOIN = (
    "s LEFT JOIN (SELECT cast(id_exame as string) id_exame, max(cast(id_unidade as string)) un, "
    "min(to_date(substr(cast(dt_exame as string), 1, 10))) de "
    "FROM diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_entrada_v2 GROUP BY 1) u "
    "ON cast(s.id_exame as string) = u.id_exame"
)


def _data_exame(f: dict) -> str:
    if f["fonte"] == "plataforma_prd":
        return DATA_EXAME_PLATAFORMA
    especiais = {"legado_pacs_enave": "DataHora"}
    if f["fonte"] in especiais:
        return especiais[f["fonte"]]
    por_linha = {"mama": "dt_exame", "hepatologia": "u.de", "pulmao_nodulo": "e.dataexame",
                 "nefrologia": "dataExameRecente"}
    return por_linha.get(f["linha"], "exm_data")


def fontes_v2() -> list[dict]:
    out = []
    for f in v1.FONTES:
        g = dict(f)
        if g["linha"] == "ateromatose" and g["fonte"] == "legado_motor":
            g["tabela"] = ESPELHO_ATERO
        if g["linha"] == "hepatologia" and g["fonte"] == "legado_motor":
            g["join"] = HEPATO_JOIN
        g["data_exame"] = _data_exame(g)
        out.append(g)
    return out


def sql_v2(f: dict, ini: str, fim: str) -> str:
    join = f.get("join") or ""
    rel = f["rel"] or "false"
    extras = f.get("extras", {})
    ext_base = "".join(f", CASE WHEN {e} THEN 1 ELSE 0 END x_{k}" for k, e in extras.items())
    ext_ex = "".join(f", max(x_{k}) x_{k}" for k in extras)
    ext_out = "".join(f", sum(x_{k}) {k}" for k in extras)
    return f"""
WITH base AS (
  SELECT cast({f['id']} as string) id, cast({f['unidade']} as string) un,
         to_date(cast({f['data']} as string)) d, to_date(cast({f['data_exame']} as string)) de,
         {f['recorte']} rec, CASE WHEN {rel} THEN 1 ELSE 0 END r{ext_base}
  FROM {f['tabela']} {join}
  WHERE {f['id']} IS NOT NULL
),
ex AS (
  SELECT rec, id, min(d) d0, min(de) de0, min_by(un, d) un, max(r) r{ext_ex}
  FROM base WHERE d IS NOT NULL GROUP BY rec, id
)
SELECT rec recorte, date_format(d0, 'yyyy-MM') mes_execucao,
       coalesce(date_format(de0, 'yyyy-MM'), '(sem data)') mes_exame,
       coalesce(un, '(nulo)') id_unidade, count(*) exames, sum(r) relevantes{ext_out}
FROM ex WHERE (d0 BETWEEN '{ini}' AND '{fim}') OR (de0 BETWEEN '{ini}' AND '{fim}')
GROUP BY 1, 2, 3, 4"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inicio", default="2026-01-01")
    ap.add_argument("--fim", default="2026-10-31")
    ap.add_argument("--so", default="")
    a = ap.parse_args()
    so = set(x for x in a.so.split(",") if x) or None
    linhas, falhas = [], []
    for f in fontes_v2():
        if so and f["tabela"] not in so and f["linha"] not in so:
            continue
        print(f"[v2] {f['linha']} | {f['tabela']} | data_exame={f['data_exame']}", flush=True)
        try:
            rows = v1.q(sql_v2(f, a.inicio, a.fim), f["profile"])
        except Exception as e:
            falhas.append({"linha": f["linha"], "tabela": f["tabela"], "erro": str(e)[:400]})
            print(f"   ERRO {e}", flush=True)
            continue
        for r in rows:
            item = {"linha": f["linha"], "fonte": f["fonte"], "tabela": f["tabela"], "recorte": r["recorte"],
                    "mes_execucao": r["mes_execucao"], "mes_exame": r["mes_exame"],
                    "id_unidade": r["id_unidade"], "exames": v1._num(r["exames"]),
                    "relevantes": v1._num(r["relevantes"]) if f["rel"] else None}
            for k in f.get("extras", {}):
                item[f"relevantes_proxy_{k}"] = v1._num(r[k])
            linhas.append(item)
        print(f"   {len(rows)} grupos", flush=True)
    meta = {"gerado_em": date.today().isoformat(), "periodo": [a.inicio, a.fim], "falhas": falhas}
    (v1.OUT / "mensal-por-linha-unidade-v2.json").write_text(
        json.dumps({"meta": meta, "linhas": linhas}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"gravado: {len(linhas)} registros; FALHAS: {len(falhas)}", flush=True)
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
