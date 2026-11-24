"""Extrai os números do one page "Visão por linha": exames na entrada do motor, relevantes e enviados, por linha e hospital.

Uso: python extrair_visao_por_linha.py <AAAA-MM-DD fim>  ->  visao-por-linha-<fim>.json (só agregados)

- Janela: 30 dias de execução do modelo terminando na data informada (inclusive).
- Ca de Cólon e DII separados em "colonoscopia" e "outros exames" pelo tipo do exame.
- Hospitais: os da navegação de PRD (ids de cadastro agrupados por hospital), via gerar_painel_visao_por_linha.
- Enviados: exames registrados no envio OneDrive de PRD (tb_mov_onedrive_saida), cruzados com a saída da linha.
Nenhum dado de paciente sai deste script.
"""
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI))
from gerar_painel_visao_por_linha import LINHAS, hospitais_navegados  # noqa: E402

DIVIDIDAS = {"cancer_colon", "doenca_inflamatoria_intestinal"}
COLONO = "lower(exm_tipo) RLIKE 'colonosc|retossigmoidosc|sigmoidosc'"


def q(sql: str) -> list[dict]:
    r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", sql, "--profile", "nlp-platform",
                        "-o", "json"], capture_output=True, text=True, encoding="utf-8")
    if r.returncode:
        raise SystemExit(r.stderr[:800])
    return json.loads(r.stdout)


def main(fim: str) -> None:
    ini = (dt.date.fromisoformat(fim) - dt.timedelta(days=29)).isoformat()
    hosp = hospitais_navegados()
    vals = ", ".join(f"('{h[:2]}', '{h[5:].replace(chr(39), chr(92) + chr(39))}', '{i}')" for h, ids in hosp.items() for i in ids)
    partes = []
    for l in LINHAS:
        variante = f"CASE WHEN {COLONO} THEN 'colonoscopia' ELSE 'outros exames' END" if l in DIVIDIDAS else "''"
        partes.append(
            f"SELECT '{l}' AS linha, {variante} AS variante, CAST(id_unidade AS STRING) AS unidade, "
            f"CAST(id_exame AS STRING) AS id_exame, fl_relevante FROM diamond_fabrica_ia.{l}.tb_mod_diamond_{l}_saida_v0 "
            f"WHERE CAST(dt_execucao_modelo AS DATE) BETWEEN DATE'{ini}' AND DATE'{fim}'")
    sql = f"""WITH hm AS (SELECT * FROM VALUES {vals} AS t(uf, hospital, unidade)),
s AS ({" UNION ALL ".join(partes)}),
env AS (SELECT DISTINCT specialty AS linha, CAST(exame_an AS STRING) AS id_exame FROM mlops_fabrica_ia.operacao.tb_mov_onedrive_saida
        WHERE environment = 'prd' AND CAST(dt_execucao_modelo AS DATE) BETWEEN DATE'{ini}' AND DATE'{fim}'),
j AS (SELECT s.*, hm.uf, hm.hospital, e.id_exame IS NOT NULL AS enviado
      FROM s LEFT JOIN hm ON hm.unidade = s.unidade LEFT JOIN env e ON e.linha = s.linha AND e.id_exame = s.id_exame)
SELECT linha, variante, unidade, coalesce(uf, '—') AS uf, coalesce(hospital, '(não navegado)') AS hospital,
  count(DISTINCT id_exame) AS exames, count(DISTINCT CASE WHEN fl_relevante = 1 THEN id_exame END) AS relevantes,
  count(DISTINCT CASE WHEN enviado THEN id_exame END) AS enviados
FROM j GROUP BY 1, 2, 3, 4, 5"""
    linhas = q(sql)
    # controle: enviados no OneDrive que não casaram com a saída (tem de ser ~0)
    orfaos = q(f"""WITH s AS ({" UNION ALL ".join(f"SELECT '{l}' AS linha, CAST(id_exame AS STRING) AS id_exame FROM diamond_fabrica_ia.{l}.tb_mod_diamond_{l}_saida_v0" for l in LINHAS)})
SELECT count(DISTINCT e.exame_an) AS enviados, count(DISTINCT CASE WHEN s.id_exame IS NULL THEN e.exame_an END) AS sem_saida
FROM mlops_fabrica_ia.operacao.tb_mov_onedrive_saida e LEFT JOIN s ON s.linha = e.specialty AND s.id_exame = CAST(e.exame_an AS STRING)
WHERE e.environment = 'prd' AND CAST(e.dt_execucao_modelo AS DATE) BETWEEN DATE'{ini}' AND DATE'{fim}'""")[0]
    out = AQUI / f"visao-por-linha-{fim}.json"
    out.write_text(json.dumps({"inicio": ini, "fim": fim, "hospitais": list(hosp), "controle_envio": orfaos,
                               "linhas": linhas}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(linhas), "linhas agregadas | controle de envio:", orfaos)


if __name__ == "__main__":
    main(sys.argv[1])
