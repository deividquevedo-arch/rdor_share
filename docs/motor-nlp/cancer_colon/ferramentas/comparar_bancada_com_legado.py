"""Paridade da bancada cancer_colon_imagem (v0tcl = negação left, v0tcb = both) contra o legado de TC. Só agregados."""
import json
import pathlib
import subprocess
import sys

OUT = pathlib.Path(sys.argv[1])
VARS = sys.argv[2].split(",") if len(sys.argv) > 2 else ["v0tcl", "v0tcb"]
S = "diamond_fabrica_ia_dev.cancer_colon.tb_mod_diamond_cancer_colon_saida_"
LEG = "diamond_fabrica_ia_hml.legado.tb_diamond_mod_colon_saida"
JAN = "DATE'2026-09-01' AND DATE'2026-09-28'"
OUTROS = ("f[ií]gado|hep[aá]tic|ves[ií]cul|biliar|p[aâ]ncre|ba[cç]o|espl[eê]n|\\\\brim\\\\b|rins|renal|ureter|bexiga|"
          "vesical|adrenal|suprarrenal|[uú]ter|endometr|ov[aá]ri|anex|pr[oó]stat|pulm|pleur|aort|\\\\bveia|art[eé]ri|"
          "linfonod|peritone|retroperit|delgad|jejun|[ií]leo|duoden|est[oô]mag|g[aá]stric")


def q(sql):
    r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", sql, "--profile", "nlp-platform",
                        "--output", "json"], capture_output=True)
    if r.returncode:
        raise SystemExit("ERRO: " + r.stderr.decode("utf-8", "replace")[-800:])
    return json.loads(r.stdout.decode("utf-8") or "[]")


LEG_D = f"""SELECT CAST(exm_an AS STRING) an, max(CASE WHEN CAST(fl_relevante AS STRING) IN ('1','true') THEN 1 ELSE 0 END) rel,
  max(CAST(exm_laudo_achados AS STRING)) achados FROM {LEG} WHERE CAST(exm_data AS DATE) BETWEEN {JAN} GROUP BY 1"""
res = {}
for v in VARS:
    T = S + v
    M = f"""SELECT CAST(id_exame AS STRING) id_exame, max(CAST(fl_relevante AS INT)) rel, max(CAST(findings AS STRING)) findings,
      max(lower(CAST(findings_match AS STRING))) ev, max(lower(CAST(exm_laudo_texto_tratado AS STRING))) txt FROM {T} GROUP BY 1"""
    r = {}
    r["pre"] = q(f"""SELECT count(*) linhas, count(DISTINCT id_exame) exames, concat_ws(',', collect_set(engine_version)) engine,
      concat_ws(',', collect_set(config_version)) config, sum(CAST(fl_relevante AS INT)) relevantes FROM {T}""")
    r["matriz"] = q(f"""WITH m AS ({M}), l AS ({LEG_D}) SELECT count(*) pares,
      sum(CASE WHEN l.rel=1 AND m.rel=1 THEN 1 ELSE 0 END) ambos, sum(CASE WHEN l.rel=1 AND m.rel=0 THEN 1 ELSE 0 END) so_legado,
      sum(CASE WHEN l.rel=0 AND m.rel=1 THEN 1 ELSE 0 END) so_plataforma, sum(CASE WHEN l.rel=0 AND m.rel=0 THEN 1 ELSE 0 END) nenhum
      FROM m JOIN l ON l.an = m.id_exame""")
    r["sem_par"] = q(f"""WITH m AS ({M}), l AS ({LEG_D}) SELECT
      (SELECT count(*) FROM m WHERE id_exame NOT IN (SELECT an FROM l)) plat_sem_legado,
      (SELECT sum(rel) FROM m WHERE id_exame NOT IN (SELECT an FROM l)) plat_sem_legado_rel,
      (SELECT count(*) FROM l WHERE an NOT IN (SELECT id_exame FROM m)) legado_sem_plat,
      (SELECT sum(rel) FROM l WHERE an NOT IN (SELECT id_exame FROM m)) legado_sem_plat_rel""")
    r["so_plataforma_findings"] = q(f"""WITH m AS ({M}), l AS ({LEG_D}) SELECT m.findings, count(*) n
      FROM m JOIN l ON l.an=m.id_exame WHERE l.rel=0 AND m.rel=1 GROUP BY 1 ORDER BY 2 DESC LIMIT 12""")
    r["so_legado_achados"] = q(f"""WITH m AS ({M}), l AS ({LEG_D}) SELECT l.achados, count(*) n
      FROM m JOIN l ON l.an=m.id_exame WHERE l.rel=1 AND m.rel=0 GROUP BY 1 ORDER BY 2 DESC LIMIT 12""")
    # Proxy do desempate "órgão mais próximo vence": algum termo do achado e menção a outro órgão a <= 60 caracteres.
    r["so_plataforma_outro_orgao_perto"] = q(f"""WITH m AS ({M}), l AS ({LEG_D}),
      d AS (SELECT m.* FROM m JOIN l ON l.an=m.id_exame WHERE l.rel=0 AND m.rel=1)
      SELECT count(*) n, sum(CASE WHEN txt rlike '({OUTROS}).{{0,60}}(les[aã]o|massa|tumor|neoplas|carcinom|p[oó]lip|[uú]lcer|eros)'
        OR txt rlike '(les[aã]o|massa|tumor|neoplas|carcinom|p[oó]lip|[uú]lcer|eros).{{0,60}}({OUTROS})' THEN 1 ELSE 0 END) com_outro_orgao
      FROM d""")
    res[v] = r

(OUT / "banc_resultado.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
for v, r in res.items():
    print("=====", v)
    for k, val in r.items():
        print("  --", k)
        for x in val:
            print("    ", x)
