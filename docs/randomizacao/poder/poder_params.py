"""Parâmetros do desfecho por janela, medidos em coortes reais (só agregados)."""
import json
import os
import subprocess

T = os.path.dirname(os.path.abspath(__file__))
CONS = "gold_corporativo_ia.corporativo.vw_mov_fato_consumo"
DEC = "lpad(regexp_replace(security.prd.rdsl_decrypt({c}, 1), '[^0-9]', ''), 11, '0')"

COORTES = {
    "reumato_legado": f"""
SELECT {DEC.format(c='pct_cpf')} AS cpf, min(exm_data) AS d0
FROM diamond_fabrica_ia.legado.tb_diamond_mod_reumatologia_saida WHERE fl_relevante = 1 GROUP BY 1""",
    "tireoide_plataforma": f"""
SELECT {DEC.format(c='p.doc_cpf')} AS cpf, min(to_date(s.dt_exame)) AS d0
FROM diamond_fabrica_ia_hml.tirads.tb_mod_diamond_tirads_saida_v0 s
JOIN gold_corporativo_ia.corporativo.tb_gold_mov_paciente p ON p.id_paciente = cast(s.id_paciente AS STRING)
WHERE s.fl_relevante = 1 GROUP BY 1""",
}


def sql(coorte):
    return f"""
WITH coh AS (SELECT cpf, d0 FROM ({COORTES[coorte]}) WHERE cpf rlike '^[0-9]{{11}}$' AND cpf <> '00000000000'),
lim AS (SELECT DATE'2026-09-29' AS dmax),
cons AS (
  SELECT lpad(regexp_replace(num_cpf_paciente_d, '[^0-9]', ''), 11, '0') AS cpf, Dt_Consumo, Vl_Produto
  FROM {CONS} WHERE trim(Cd_Destino) <> 'H' AND Dt_Consumo >= DATE'2025-07-01'
),
cs AS (SELECT c.* FROM cons c LEFT SEMI JOIN coh ON coh.cpf = c.cpf),
pp AS (
  SELECT coh.cpf, coh.d0, max(CASE WHEN cs.cpf IS NOT NULL THEN 1 ELSE 0 END) AS casou,
         coalesce(sum(CASE WHEN cs.Dt_Consumo > date_sub(coh.d0, 180) AND cs.Dt_Consumo <= coh.d0 THEN cs.Vl_Produto END), 0) AS pre180,
         coalesce(sum(CASE WHEN cs.Dt_Consumo > date_sub(coh.d0, 30) AND cs.Dt_Consumo <= coh.d0 THEN cs.Vl_Produto END), 0) AS pre30,
         {", ".join(f"coalesce(sum(CASE WHEN cs.Dt_Consumo > coh.d0 AND cs.Dt_Consumo <= date_add(coh.d0, {w}) THEN cs.Vl_Produto END), 0) AS pos{w}" for w in (30, 60, 90, 180))}
  FROM coh LEFT JOIN cs ON cs.cpf = coh.cpf GROUP BY 1, 2
),
j AS (
  SELECT pp.*, w.janela, CASE w.janela WHEN 30 THEN pos30 WHEN 60 THEN pos60 WHEN 90 THEN pos90 ELSE pos180 END AS y
  FROM pp CROSS JOIN (SELECT explode(array(30, 60, 90, 180)) AS janela) w CROSS JOIN lim
  WHERE date_add(pp.d0, w.janela) <= lim.dmax AND pp.d0 >= DATE'2026-01-01'
),
cap AS (SELECT janela, percentile(y, 0.99) AS p99y, percentile(pre180, 0.99) AS p99x FROM j GROUP BY 1)
SELECT '{coorte}' AS coorte, j.janela, count(*) AS n, round(avg(casou), 3) AS casou,
       round(avg(CASE WHEN y > 0 THEN 1.0 ELSE 0 END), 4) AS p_retorno,
       round(avg(y), 1) AS media, round(stddev(y), 1) AS dp,
       round(avg(least(y, p99y)), 1) AS media_cap, round(stddev(least(y, p99y)), 1) AS dp_cap,
       round(corr(y, pre180), 3) AS rho_pre180, round(corr(least(y, p99y), least(pre180, p99x)), 3) AS rho_cap,
       round(corr(CASE WHEN y > 0 THEN 1.0 ELSE 0 END, CASE WHEN pre180 > 0 THEN 1.0 ELSE 0 END), 3) AS rho_retorno,
       round(percentile(y, 0.5), 1) AS mediana
FROM j JOIN cap ON cap.janela = j.janela
GROUP BY 1, 2 ORDER BY 2
"""


args = []
for k in COORTES:
    f = os.path.join(T, f"pp_{k}.sql")
    open(f, "w", encoding="utf-8").write(sql(k))
    args += ["--file", f]
r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", "--profile", "nlp-platform", "--output", "json", *args],
                   capture_output=True, text=True, encoding="utf-8", env={**os.environ, "DATABRICKS_WAREHOUSE_ID": "5db9022c9b724e7a"})
try:
    out = json.loads(r.stdout)
except Exception:
    print(r.stdout[:2000], r.stderr[:2000])
    raise SystemExit
res = []
for o in out:
    print(o["state"], (o.get("error") or "")[:400])
    print(o.get("columns"))
    for row in o.get("rows") or []:
        print(row)
        res.append(dict(zip(o["columns"], row)))
json.dump(res, open(os.path.join(T, "poder_params.json"), "w"), ensure_ascii=False, indent=1)
