"""Panorama INFORMATIVO: variação do valor em três recortes (total, sem diárias, produtos da linha). Só agregados."""
import json
import os
import subprocess

T = os.path.dirname(os.path.abspath(__file__))
ns = {"__file__": os.path.join(T, "poder_params.py")}
exec(open(os.path.join(T, "poder_params.py"), encoding="utf-8").read().split("def sql(")[0], ns)
CO, CONS = ns["COORTES"], ns["CONS"]

DIARIA = "lower(c.nme_tipo_produto) rlike 'di[aá]ria'"
LINHA = {
    "reumato_legado": ("lower(c.nme_produto) rlike 'simponi|golimum|remicade|infliximab|remsima|actemra|tocilizum|orencia|"
                       "abatacep|mabthera|rituxim|humira|adalimum|enbrel|etanercep|cimzia|certolizum|cosentyx|secukinum|"
                       "stelara|ustekinum|benlysta|belimum|taltz|ixekizum|metotrex|ciclofosf|pulsoterap|infus'"),
    "tireoide_plataforma": "lower(c.nme_produto) rlike 'tireo|tiroi|paaf|pun[cç][aã]o|cell block'",
}
JANELAS = {"reumato_legado": (30, 60, 90), "tireoide_plataforma": (30, 60)}


def sql(coorte):
    ws = JANELAS[coorte]
    rec = {"total": "TRUE", "sem_diarias": f"NOT ({DIARIA})", "linha": LINHA[coorte]}
    cols = []
    for r, cond in rec.items():
        cols.append(f"coalesce(sum(CASE WHEN c.Dt_Consumo > date_sub(coh.d0, 180) AND c.Dt_Consumo <= coh.d0 AND {cond} THEN c.Vl_Produto END), 0) AS pre_{r}")
        for w in ws:
            cols.append(f"coalesce(sum(CASE WHEN c.Dt_Consumo > coh.d0 AND c.Dt_Consumo <= date_add(coh.d0, {w}) AND {cond} THEN c.Vl_Produto END), 0) AS {r}_{w}")
    sel = []
    for r in rec:
        for w in ws:
            y = f"{r}_{w}"
            sel.append(f"SELECT '{coorte}' AS coorte, '{r}' AS recorte, {w} AS janela, count(*) AS n, "
                       f"round(avg(CASE WHEN {y} > 0 THEN 1.0 ELSE 0 END), 3) AS com_valor, round(avg({y}), 0) AS media, "
                       f"round(stddev({y}) / avg({y}), 2) AS cv, "
                       f"round(stddev(least({y}, p99)) / avg(least({y}, p99)), 2) AS cv_teto, "
                       f"round(corr(least({y}, p99), pre_{r}), 2) AS rho_pre "
                       f"FROM pp CROSS JOIN (SELECT percentile({y}, 0.99) AS p99 FROM pp WHERE date_add(d0, {w}) <= DATE'2026-09-29') q "
                       f"WHERE date_add(d0, {w}) <= DATE'2026-09-29'")
    return f"""
WITH coh AS (SELECT cpf, d0 FROM ({CO[coorte]}) WHERE cpf rlike '^[0-9]{{11}}$' AND cpf <> '00000000000' AND d0 >= DATE'2026-01-01'),
cons AS (SELECT lpad(regexp_replace(num_cpf_paciente_d, '[^0-9]', ''), 11, '0') AS cpf, Dt_Consumo, Vl_Produto, nme_tipo_produto, nme_produto
         FROM {CONS} WHERE trim(Cd_Destino) <> 'H' AND Dt_Consumo >= DATE'2025-07-01'),
cs AS (SELECT x.* FROM cons x LEFT SEMI JOIN coh ON coh.cpf = x.cpf),
pp AS (SELECT coh.cpf, coh.d0, {", ".join(cols)} FROM coh LEFT JOIN cs c ON c.cpf = coh.cpf GROUP BY 1, 2)
{" UNION ALL ".join(sel)}
"""


args = []
for k in CO:
    f = os.path.join(T, f"rec_{k}.sql")
    open(f, "w", encoding="utf-8").write(sql(k))
    args += ["--file", f]
r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", "--profile", "nlp-platform", "--output", "json", *args],
                   capture_output=True, text=True, encoding="utf-8", env={**os.environ, "DATABRICKS_WAREHOUSE_ID": "5db9022c9b724e7a"})
try:
    out = json.loads(r.stdout)
except Exception:
    print(r.stdout[:2000], r.stderr[:2000])
    raise SystemExit
for o in out:
    print(o["state"], (o.get("error") or "")[:400])
    print(o.get("columns"))
    for row in o.get("rows") or []:
        print(row)
