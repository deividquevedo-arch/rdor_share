import json,subprocess,os,importlib.util,io,contextlib
spec=importlib.util.spec_from_file_location("pp","poder_params.py")
src=open("poder_params.py",encoding="utf-8").read()
ns={"__file__": "poder_params.py"}; exec(src.split("def sql(")[0], ns)   # pega COORTES, CONS, DEC sem rodar a consulta
CO=ns["COORTES"]; CONS=ns["CONS"]
def q(coorte, wmax):
    dias=[7,14,21,30,45,60,90,180]
    dias=[d for d in dias if d<=wmax]
    cols=", ".join(f"coalesce(sum(CASE WHEN cs.Dt_Consumo > coh.d0 AND cs.Dt_Consumo <= date_add(coh.d0,{d}) THEN cs.Vl_Produto END),0) v{d}, max(CASE WHEN cs.Dt_Consumo > coh.d0 AND cs.Dt_Consumo <= date_add(coh.d0,{d}) THEN 1 ELSE 0 END) r{d}" for d in dias)
    out=", ".join(f"round(sum(v{d})/sum(v{wmax}),3) frac_valor_{d}, round(avg(r{d}),3) retorno_{d}" for d in dias)
    return f"""WITH coh AS (SELECT cpf,d0 FROM ({CO[coorte]}) WHERE cpf rlike '^[0-9]{{11}}$' AND cpf<>'00000000000' AND d0 >= DATE'2026-01-01' AND date_add(d0,{wmax}) <= DATE'2026-09-29'),
cons AS (SELECT lpad(regexp_replace(num_cpf_paciente_d,'[^0-9]',''),11,'0') cpf, Dt_Consumo, Vl_Produto, nme_tipo_produto FROM {CONS} WHERE trim(Cd_Destino)<>'H' AND Dt_Consumo>=DATE'2025-12-01'),
cs AS (SELECT c.* FROM cons c LEFT SEMI JOIN coh ON coh.cpf=c.cpf),
pp AS (SELECT coh.cpf, {cols} FROM coh LEFT JOIN cs ON cs.cpf=coh.cpf GROUP BY 1)
SELECT '{coorte}' coorte, count(*) n, {out} FROM pp"""
def q2(coorte, wmax):
    return f"""WITH coh AS (SELECT cpf,d0 FROM ({CO[coorte]}) WHERE cpf rlike '^[0-9]{{11}}$' AND cpf<>'00000000000' AND d0 >= DATE'2026-01-01' AND date_add(d0,{wmax}) <= DATE'2026-09-29'),
cons AS (SELECT lpad(regexp_replace(num_cpf_paciente_d,'[^0-9]',''),11,'0') cpf, Dt_Consumo, Vl_Produto, nme_tipo_produto FROM {CONS} WHERE trim(Cd_Destino)<>'H' AND Dt_Consumo>=DATE'2025-12-01'),
x AS (SELECT c.nme_tipo_produto tipo, c.Vl_Produto v FROM cons c JOIN coh ON coh.cpf=c.cpf AND c.Dt_Consumo > coh.d0 AND c.Dt_Consumo <= date_add(coh.d0,{wmax}))
SELECT '{coorte}' coorte, tipo, round(sum(v)/(SELECT sum(v) FROM x),3) parcela FROM x GROUP BY tipo ORDER BY 3 DESC LIMIT 6"""
qs=[q("reumato_legado",180), q("tireoide_plataforma",60), q2("reumato_legado",180), q2("tireoide_plataforma",60)]
args=[]
for i,s in enumerate(qs):
    f=f"a{i}.sql"; open(f,"w",encoding="utf-8").write(s); args+=["--file",f]
r=subprocess.run(['databricks','experimental','aitools','tools','query','--profile','nlp-platform','--output','json',*args],capture_output=True,text=True,encoding='utf-8',env={**os.environ,'DATABRICKS_WAREHOUSE_ID':'5db9022c9b724e7a'})
for o in json.loads(r.stdout):
    print("\n",o['state'],(o.get('error') or '')[:300]); cols=o.get('columns') or []
    for row in o.get('rows') or []: print(dict(zip(cols,row)))
