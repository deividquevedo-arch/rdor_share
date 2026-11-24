"""Captados por unidade: pacientes enviados pelos algoritmos que entraram na navegação (base do BI de captação).

Uso: python extrair_captados_por_unidade.py <AAAA-MM-DD fim>  ->  captados-por-unidade-<fim>.json (só agregados)

Cadeia: envio OneDrive de PRD (exame, linha, dia) -> exame na gold (paciente, unidade) -> CPF do paciente (decriptado
DENTRO da consulta) -> base de captados do BI (CPF, linha do BI, data de entrada >= dia do envio).
O CPF nunca sai da consulta: o resultado é contagem por linha e unidade.
"""
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

AQUI = Path(__file__).parent
LINHA_BI = {  # linha do algoritmo -> linha da base de captados do BI
    "cancer_colon": "CA Colon", "doenca_inflamatoria_intestinal": "DII", "cancer_rim": "CA Rim",
    "doencas_biliares": "Calculo Biliar", "reumatologia": "Reumato", "tirads": "CA Tireoide", "ateromatose_coronariana": "Ateromatose",
    "neuroimunologia": "Neuro", "pirads": "CA Próstata", "cancer_estomago": "CA Estômago",
}


def main(fim: str) -> None:
    ini = (dt.date.fromisoformat(fim) - dt.timedelta(days=29)).isoformat()
    mapa = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in LINHA_BI.items())
    sql = f"""WITH env AS (
  SELECT specialty AS linha, CAST(exame_an AS STRING) AS id_exame, min(CAST(dt_execucao_modelo AS DATE)) AS dia_envio
  FROM mlops_fabrica_ia.operacao.tb_mov_onedrive_saida
  WHERE environment = 'prd' AND CAST(dt_execucao_modelo AS DATE) BETWEEN DATE'{ini}' AND DATE'{fim}'
  GROUP BY 1, 2),
ex AS (SELECT CAST(g.id_exame AS STRING) AS id_exame, max(g.id_patient) AS id_paciente, max(CAST(g.id_unidade AS STRING)) AS unidade
       FROM gold_corporativo_ia.corporativo.tb_gold_mov_exame g
       JOIN (SELECT DISTINCT id_exame FROM env) k ON CAST(g.id_exame AS STRING) = k.id_exame GROUP BY 1),  -- id_exame e string
pac AS (SELECT id_paciente, regexp_replace(security.prd.rdsl_decrypt(doc_cpf, 1), '[^0-9]', '') AS cpf
        FROM gold_corporativo_ia.corporativo.tb_gold_mov_paciente WHERE id_paciente IN (SELECT id_paciente FROM ex)),
cap AS (SELECT regexp_replace(regexp_replace(cpf, '[^0-9]', ''), '^0+', '') AS cpf, linha, min(data_entrada) AS entrada
        FROM gold_fabrica_ia_dev.navegacao.tb_pacientes_captados GROUP BY 1, 2),
j AS (SELECT e.linha, x.unidade, p.cpf, e.dia_envio, CASE e.linha {mapa} END AS linha_bi
      FROM env e JOIN ex x ON x.id_exame = e.id_exame LEFT JOIN pac p ON p.id_paciente = x.id_paciente)
SELECT j.linha, j.unidade,
  count(DISTINCT j.cpf) AS pacientes_enviados,
  count(DISTINCT CASE WHEN c.cpf IS NOT NULL AND c.entrada >= j.dia_envio THEN j.cpf END) AS captados
FROM j LEFT JOIN cap c ON c.cpf = regexp_replace(j.cpf, '^0+', '') AND c.linha = j.linha_bi
GROUP BY 1, 2"""
    r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", sql, "--profile", "nlp-platform", "-o", "json"],
                       capture_output=True, text=True, encoding="utf-8")
    if r.returncode:
        raise SystemExit(r.stderr[:800])
    linhas = json.loads(r.stdout)
    out = AQUI / f"captados-por-unidade-{fim}.json"
    out.write_text(json.dumps({"inicio": ini, "fim": fim, "linhas": linhas}, ensure_ascii=False, indent=1), encoding="utf-8")
    tot = {}
    for x in linhas:
        t = tot.setdefault(x["linha"], [0, 0])
        t[0] += int(x["pacientes_enviados"])
        t[1] += int(x["captados"])
    print(out.name, "|", len(linhas), "linhas")
    for k, (e, c) in sorted(tot.items(), key=lambda t: -t[1][1]):
        print(f"  {k:32} enviados {e:5}  captados {c:4}")


if __name__ == "__main__":
    main(sys.argv[1])
