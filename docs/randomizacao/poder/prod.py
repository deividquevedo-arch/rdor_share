import json,subprocess,os
ns={"__file__":"poder_params.py"}; exec(open("poder_params.py",encoding="utf-8").read().split("def sql(")[0], ns)
CO=ns["COORTES"]; CONS=ns["CONS"]
V="bronze_fabrica_ia_dev.navegacao.vw_central_navegacao_caso_analitico"
def top(coorte,w,filtro,lim=15):
    return f"""WITH coh AS (SELECT cpf,d0 FROM ({CO[coorte]}) WHERE cpf rlike '^[0-9]{{11}}$' AND d0>=DATE'2026-01-01' AND date_add(d0,{w})<=DATE'2026-09-29'),
cons AS (SELECT lpad(regexp_replace(num_cpf_paciente_d,'[^0-9]',''),11,'0') cpf, Dt_Consumo, Vl_Produto, nme_tipo_produto, nme_produto FROM {CONS} WHERE trim(Cd_Destino)<>'H' AND Dt_Consumo>=DATE'2025-12-01'),
x AS (SELECT c.* FROM cons c JOIN coh ON coh.cpf=c.cpf AND c.Dt_Consumo>coh.d0 AND c.Dt_Consumo<=date_add(coh.d0,{w}) WHERE {filtro})
SELECT '{coorte}' c, nme_tipo_produto, nme_produto, count(DISTINCT cpf) pacientes, round(sum(Vl_Produto)/(SELECT sum(Vl_Produto) FROM x),3) parcela FROM x GROUP BY 2,3 ORDER BY 5 DESC LIMIT {lim}"""
qs=[f"SELECT lower(nome_linha), ds_produtos_linha, ds_categorias_produto_linha, count(*) FROM {V} WHERE ds_produtos_linha IS NOT NULL GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 10",
    top("reumato_legado",90,"nme_tipo_produto = 'Medicamentos'"),
    top("tireoide_plataforma",60,"lower(nme_produto) rlike 'tireo|tiroi|paaf|pun[cç][aã]o|bi[oó]psia|iod'"),
    f"SELECT nme_tipo_produto, count(*) FROM {CONS} WHERE Dt_Consumo='2026-09-01' GROUP BY 1 ORDER BY 2 DESC LIMIT 25"]
args=[]
for i,q in enumerate(qs):
    f=f"p{i}.sql"; open(f,"w",encoding="utf-8").write(q); args+=["--file",f]
r=subprocess.run(['databricks','experimental','aitools','tools','query','--profile','nlp-platform','--output','json',*args],capture_output=True,text=True,encoding='utf-8',env={**os.environ,'DATABRICKS_WAREHOUSE_ID':'5db9022c9b724e7a'})
for o in json.loads(r.stdout):
    print("\n",o['state'],(o.get('error') or '')[:300])
    for row in o.get('rows') or []: print(row)
