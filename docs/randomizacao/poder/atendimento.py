"""Panorama INFORMATIVO, valor por ATENDIMENTO: todo o consumo do atendimento que contém procedimento da linha.
Separa o atendimento do exame (com consumo até o dia do laudo) dos atendimentos novos depois dele. Só agregados."""
import json
import os
import subprocess

T = os.path.dirname(os.path.abspath(__file__))
ns = {"__file__": os.path.join(T, "poder_params.py")}
exec(open(os.path.join(T, "poder_params.py"), encoding="utf-8").read().split("def sql(")[0], ns)
CO, CONS = ns["COORTES"], ns["CONS"]
LINHA = {
    "reumato_legado": ("lower(nme_produto) rlike 'simponi|golimum|remicade|infliximab|remsima|actemra|tocilizum|orencia|"
                       "abatacep|mabthera|rituxim|humira|adalimum|enbrel|etanercep|cimzia|certolizum|cosentyx|secukinum|"
                       "stelara|ustekinum|benlysta|belimum|taltz|ixekizum|metotrex|ciclofosf|pulsoterap|infus'"),
    "tireoide_plataforma": "lower(nme_produto) rlike 'tireo|tiroi|paaf|pun[cç][aã]o|cell block'",
}
JANELAS = {"reumato_legado": (30, 60, 90), "tireoide_plataforma": (30, 60)}


def sql(coorte):
    ws = JANELAS[coorte]
    blocos = []
    for w in ws:
        blocos.append(f"""
SELECT '{coorte}' AS coorte, {w} AS janela, count(*) AS n,
       round(avg(fez_linha), 4) AS fez_linha, round(avg(linha_no_atend_do_exame), 4) AS linha_no_atend_do_exame,
       round(avg(CASE WHEN fez_linha = 1 THEN valor_ep END), 0) AS ticket_atend_linha,
       round(avg(CASE WHEN fez_linha = 1 THEN valor_item END), 0) AS ticket_so_item,
       round(avg(valor_ep), 0) AS media_por_paciente,
       round(stddev(valor_ep) / avg(valor_ep), 2) AS cv,
       round(stddev(least(valor_ep, p99)) / avg(least(valor_ep, p99)), 2) AS cv_teto,
       round(avg(CASE WHEN fez_linha = 1 THEN dias_diaria END), 1) AS diarias_medias_quem_fez
FROM (
  SELECT coh.cpf,
         max(CASE WHEN a.novo = 1 AND a.tem_linha = 1 AND a.d_ini <= date_add(coh.d0, {w}) THEN 1 ELSE 0 END) AS fez_linha,
         max(CASE WHEN a.novo = 0 AND a.tem_linha = 1 THEN 1 ELSE 0 END) AS linha_no_atend_do_exame,
         coalesce(sum(CASE WHEN a.novo = 1 AND a.tem_linha = 1 AND a.d_ini <= date_add(coh.d0, {w}) THEN a.valor_ate_{w} END), 0) AS valor_ep,
         coalesce(sum(CASE WHEN a.novo = 1 AND a.tem_linha = 1 AND a.d_ini <= date_add(coh.d0, {w}) THEN a.item_ate_{w} END), 0) AS valor_item,
         coalesce(sum(CASE WHEN a.novo = 1 AND a.tem_linha = 1 AND a.d_ini <= date_add(coh.d0, {w}) THEN a.diarias END), 0) AS dias_diaria
  FROM coh LEFT JOIN atend a ON a.cpf = coh.cpf
  WHERE date_add(coh.d0, {w}) <= DATE'2026-09-29'
  GROUP BY 1
) pp CROSS JOIN (SELECT 1) x
CROSS JOIN (SELECT percentile(v, 0.99) AS p99 FROM (
  SELECT coh.cpf, coalesce(sum(CASE WHEN a.novo = 1 AND a.tem_linha = 1 AND a.d_ini <= date_add(coh.d0, {w}) THEN a.valor_ate_{w} END), 0) AS v
  FROM coh LEFT JOIN atend a ON a.cpf = coh.cpf WHERE date_add(coh.d0, {w}) <= DATE'2026-09-29' GROUP BY 1)) q""")
    valores = ", ".join(
        f"sum(CASE WHEN Dt_Consumo <= date_add(d0, {w}) THEN Vl_Produto ELSE 0 END) AS valor_ate_{w}, "
        f"sum(CASE WHEN Dt_Consumo <= date_add(d0, {w}) AND {LINHA[coorte]} THEN Vl_Produto ELSE 0 END) AS item_ate_{w}"
        for w in ws)
    return f"""
WITH coh AS (SELECT cpf, d0 FROM ({CO[coorte]}) WHERE cpf rlike '^[0-9]{{11}}$' AND cpf <> '00000000000' AND d0 >= DATE'2026-01-01'),
cons AS (SELECT lpad(regexp_replace(num_cpf_paciente_d, '[^0-9]', ''), 11, '0') AS cpf, Id_Hospital, Cd_Atendimento,
                Dt_Consumo, Vl_Produto, nme_tipo_produto, nme_produto
         FROM {CONS} WHERE trim(Cd_Destino) <> 'H' AND Dt_Consumo >= DATE'2025-12-01' AND Cd_Atendimento IS NOT NULL),
cs AS (SELECT x.*, coh.d0 FROM cons x JOIN coh ON coh.cpf = x.cpf),
atend AS (
  SELECT cpf, Id_Hospital, Cd_Atendimento, d0, min(Dt_Consumo) AS d_ini,
         CASE WHEN min(Dt_Consumo) > d0 THEN 1 ELSE 0 END AS novo,
         max(CASE WHEN {LINHA[coorte]} AND Dt_Consumo > d0 THEN 1 ELSE 0 END) AS tem_linha,
         count(DISTINCT CASE WHEN lower(nme_tipo_produto) rlike 'di[aá]ria' THEN Dt_Consumo END) AS diarias,
         {valores}
  FROM cs WHERE Dt_Consumo > d0 OR Dt_Consumo >= date_sub(d0, 60)
  GROUP BY 1, 2, 3, 4
)
{" UNION ALL ".join(blocos)}
"""


args = []
for k in CO:
    f = os.path.join(T, f"at_{k}.sql")
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
    print(o["state"], (o.get("error") or "")[:500])
    print(o.get("columns"))
    for row in o.get("rows") or []:
        print(row)
