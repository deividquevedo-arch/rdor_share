import json,subprocess,os
V="bronze_fabrica_ia_dev.navegacao.vw_central_navegacao_caso_analitico"
W=f"FROM {V} WHERE fl_ativo AND dt_referencia BETWEEN '2025-01-01' AND current_date()"
def pct(c): return f"count({c}) n_{c}, percentile({c},0.5) p50_{c}, percentile({c},0.75) p75_{c}, percentile({c},0.9) p90_{c}"
qs=[f"SELECT lower(nome_linha) linha, count(*) casos, min(dt_referencia), max(dt_referencia), sum(CASE WHEN fl_captado THEN 1 ELSE 0 END) captados, sum(CASE WHEN fl_infusao_realizada THEN 1 ELSE 0 END) infusoes, count(dt_primeira_consulta) com_consulta, count(vl_consumo) com_valor, round(avg(vl_consumo),0) vl_medio {W} GROUP BY 1",
    f"SELECT lower(nome_linha) linha, {pct('dias_liberacao_ate_caso')}, {pct('dias_caso_ate_contato_1')}, {pct('dias_liberacao_ate_contato_1')} {W} AND dias_liberacao_ate_caso >= 0 GROUP BY 1",
    f"SELECT lower(nome_linha) linha, {pct('int_lib_consulta')}, {pct('int_lib_infusao')}, {pct('int_lib_cirurgia')}, {pct('int_lib_admissao')} {W} GROUP BY 1",
    f"SELECT lower(nome_linha) linha, ds_etapa_funil, count(*) {W} GROUP BY 1,2 ORDER BY 1,3 DESC"]
args=[]
for i,q in enumerate(qs):
    f=f"j{i}.sql"; open(f,"w").write(q); args+=["--file",f]
r=subprocess.run(['databricks','experimental','aitools','tools','query','--profile','nlp-platform','--output','json',*args],capture_output=True,text=True,encoding='utf-8',env={**os.environ,'DATABRICKS_WAREHOUSE_ID':'5db9022c9b724e7a'})
for o in json.loads(r.stdout):
    print("\n",o['state'],(o.get('error') or '')[:300]); cols=o.get('columns') or []
    for row in o.get('rows') or []: print(dict(zip(cols,row)))
