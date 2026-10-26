import json,subprocess,os
ns={"__file__":"poder_params.py"}; exec(open("poder_params.py",encoding="utf-8").read().split("def sql(")[0], ns)
CO=ns["COORTES"]; CONS=ns["CONS"]
R="lower(nme_produto) rlike 'simponi|golimum|remicade|infliximab|remsima|actemra|tocilizum|orencia|abatacep|mabthera|rituxim|humira|adalimum|enbrel|etanercep|cimzia|certolizum|cosentyx|secukinum|stelara|ustekinum|benlysta|belimum|taltz|ixekizum|metotrex|ciclofosf|pulsoterap|infus'"
q=f"""WITH coh AS (SELECT cpf,d0 FROM ({CO['reumato_legado']}) WHERE cpf rlike '^[0-9]{{11}}$' AND d0>=DATE'2026-01-01' AND date_add(d0,90)<=DATE'2026-09-29'),
x AS (SELECT c.nme_tipo_produto t, c.nme_produto p, lpad(regexp_replace(c.num_cpf_paciente_d,'[^0-9]',''),11,'0') cpf, c.Vl_Produto v
 FROM {CONS} c JOIN coh ON coh.cpf=lpad(regexp_replace(c.num_cpf_paciente_d,'[^0-9]',''),11,'0') AND c.Dt_Consumo>coh.d0 AND c.Dt_Consumo<=date_add(coh.d0,90)
 WHERE trim(c.Cd_Destino)<>'H' AND c.Dt_Consumo>=DATE'2025-12-01' AND {R.replace('nme_produto','c.nme_produto')})
SELECT t, p, count(DISTINCT cpf) pacientes, round(sum(v),0) valor FROM x GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20"""
open("chk.sql","w",encoding="utf-8").write(q)
r=subprocess.run(['databricks','experimental','aitools','tools','query','--profile','nlp-platform','--output','json','--file','chk.sql','--file','chk.sql'],capture_output=True,text=True,encoding='utf-8',env={**os.environ,'DATABRICKS_WAREHOUSE_ID':'5db9022c9b724e7a'})
o=json.loads(r.stdout)[0]; print(o['state'],o.get('error'))
for row in o.get('rows') or []: print(row)
