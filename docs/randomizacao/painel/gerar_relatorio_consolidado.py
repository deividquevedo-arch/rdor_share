"""Relatório da randomização para o negócio — CONSOLIDADO em 5 abas: Onda 1 · Onda 2 · Onda 3 · Global HML · Global PRD.

Uso: python gerar_relatorio_consolidado.py  ->  painel-negocio-consolidado-v1.lvdash.json
Cada aba é a página do relatório APROVADO (Onda 1 v2, versão de 26/10/2026) com datasets próprios:
  - Onda 1: igual à aprovada (sorteio de produção, volume e prazo como aprovados);
  - Ondas 2 e 3: sorteio de HOMOLOGAÇÃO (prévia até a ativação), volume e prazo do volume elegível de produção;
  - Global HML: todas as linhas com sorteio em homologação; volume e prazo de homologação;
  - Global PRD: todas as linhas com sorteio em produção (cresce sozinha a cada onda ativada).
Gráfico dia a dia com eixo categórico (1 barra por dia) e legenda "Grupo" nas abas novas.
"""
import json
from pathlib import Path

import gerar_relatorio_onda_n as onda

AQUI = Path(__file__).parent
BASE = onda.BASE
ROTULO = {**onda.ROTULO, "tirads": "Tireoide", "reumatologia": "Reumatologia"}
Z2, P, D = onda.Z2, onda.P, onda.D
CASE = "CASE nm_especialidade " + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in ROTULO.items()) + " ELSE initcap(nm_especialidade) END"

ABAS = [
    {"id": "o1", "nome": "Onda 1", "aprovada": True},
    {"id": "o2", "nome": "Onda 2", "amb_sorteio": "hml", "amb_volume": "prd", "linhas": onda.ONDAS[2]["linhas"],
     "total": "Onda 2 (total)", "titulo": "Onda 2", "subtitulo": f"{onda.ONDAS[2]['nomes']} · PRD prevista para {onda.ONDAS[2]['prd']}",
     "aviso": "**🧪 PRÉVIA EM HOMOLOGAÇÃO.** A randomização da Onda 2 entra em produção em 17/11/2026 (data em ajuste). Até lá, "
              "as seções **1** e **5** mostram os sorteios de **homologação** — a janela de teste de 12 e 13/10, reprocessada em "
              "validação; poucos pacientes, para ver o formato, não para ler resultado. Volume e prazo usam os exames elegíveis "
              "de **produção**.", "sem_volume": onda.ONDAS[2]["sem_volume"]},
    {"id": "o3", "nome": "Onda 3", "amb_sorteio": "hml", "amb_volume": "prd", "linhas": onda.ONDAS[3]["linhas"],
     "total": "Onda 3 (total)", "titulo": "Onda 3", "subtitulo": f"{onda.ONDAS[3]['nomes']} · PRD prevista para {onda.ONDAS[3]['prd']}",
     "aviso": "**🧪 PRÉVIA EM HOMOLOGAÇÃO.** A randomização da Onda 3 entra em produção em 24/11/2026 (data em ajuste). Até lá, "
              "as seções **1** e **5** mostram os sorteios de **homologação** — a janela de teste de 12 e 13/10, reprocessada em "
              "validação; poucos pacientes, para ver o formato, não para ler resultado. Volume e prazo usam os exames elegíveis "
              "de **produção**.", "sem_volume": onda.ONDAS[3]["sem_volume"]},
    {"id": "gh", "nome": "Global HML", "amb_sorteio": "hml", "amb_volume": "hml", "linhas": None,
     "total": "Global HML (total)", "titulo": "visão global em homologação",
     "subtitulo": "Todas as linhas com sorteio em homologação · atualização a cada execução",
     "aviso": "**🧪 HOMOLOGAÇÃO.** Visão de todas as linhas com sorteio em homologação. Os jobs de homologação rodam sob demanda, "
              "em janelas de validação — os números mostram o processo funcionando, não o volume real. Volume e prazo desta aba "
              "usam os exames elegíveis de **homologação**.",
     "sem_volume": "Em homologação o volume depende das janelas que foram rodadas; não serve para projetar o mês de produção."},
    {"id": "gp", "nome": "Global PRD", "amb_sorteio": "prd", "amb_volume": "prd", "linhas": None,
     "total": "Global PRD (total)", "titulo": "visão global em produção", "vol_pacientes": True,
     "subtitulo": "Todas as linhas com sorteio em produção · atualização diária",
     "aviso": "**🟢 PRODUÇÃO.** Todas as linhas com sorteio ativo em produção. Hoje é a Onda 1 (Reumatologia e Tireoide); a aba "
              "cresce sozinha conforme as Ondas 2 e 3 forem ativadas.",
     "sem_volume": "O volume soma só as linhas com sorteio ativo em produção."},
]


def filtro_linhas(aba, col="nm_especialidade"):
    if aba["linhas"]:
        return f"{col} IN (" + ", ".join(f"'{l}'" for l in aba["linhas"]) + ")"
    return (f"{col} IN (SELECT DISTINCT nm_especialidade FROM tb_mov_randomizacao_log "
            f"WHERE tp_ambiente = '{aba['amb_sorteio']}')")


def sqls(aba):
    f = f"tp_ambiente = '{aba['amb_sorteio']}' AND {filtro_linhas(aba)}"
    vol = f"""
WITH r AS (
  SELECT nm_especialidade, to_date(dt_execucao_modelo) AS d, max_by(qt_exportados, dh_registro) AS elegiveis
  FROM tb_mov_randomizacao_relatorio
  WHERE tp_ambiente = '{aba['amb_volume']}' AND {filtro_linhas(aba)} AND to_date(dt_execucao_modelo) >= DATE'2026-10-14'
  GROUP BY 1, 2
),
por_linha AS (SELECT nm_especialidade, avg(elegiveis) * 21 AS mes FROM r GROUP BY 1),
mes AS (SELECT coalesce(sum(mes), 0) AS pacientes_mes FROM por_linha)"""
    if aba.get("vol_pacientes"):  # mesmo método da Onda 1 aprovada: pacientes sorteados por dia útil desde 14/10
        vol = f"""
WITH p AS (SELECT num_cpf, nm_especialidade, min(to_date(dt_execucao_modelo)) AS d FROM tb_mov_randomizacao_log WHERE {f} GROUP BY 1, 2),
dias AS (SELECT count(DISTINCT to_date(dt_execucao_modelo)) AS n FROM tb_mov_randomizacao_log WHERE {f} AND to_date(dt_execucao_modelo) >= DATE'2026-10-14'),
mes AS (SELECT count(*) / max(dias.n) * 21 AS pacientes_mes FROM p CROSS JOIN dias WHERE p.d >= DATE'2026-10-14')"""
    return {
        "pacientes": f"""
SELECT {CASE} AS linha, sha2(cast(num_cpf AS string), 256) AS id_paciente,
       CASE WHEN max(tp_grupo) = 'controle' THEN 'Grupo controle' ELSE 'Enviados à navegação' END AS grupo,
       CASE WHEN max(tp_grupo) = 'controle' THEN 1 ELSE 0 END AS fl_controle,
       CASE WHEN max(tp_grupo) = 'controle' THEN 0 ELSE 1 END AS fl_navegacao,
       min(to_date(dt_execucao_modelo)) AS dia,
       date_format(min(to_date(dt_execucao_modelo)), 'yyyy-MM-dd') AS dia_txt
FROM tb_mov_randomizacao_log WHERE {f}
GROUP BY 1, 2""",
        "garantias": f"""
WITH l AS (SELECT * FROM tb_mov_randomizacao_log WHERE {f})
SELECT
  (SELECT count(*) FROM l WHERE tp_grupo = 'controle' AND fl_enviado) AS controle_enviado,
  (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf HAVING count(DISTINCT tp_grupo) > 1)) AS trocou_de_grupo,
  (SELECT count(*) FROM l LEFT JOIN tb_mov_randomizacao_controle b
     ON lpad(cast(l.num_cpf AS string), 11, '0') = lpad(cast(b.num_cpf AS string), 11, '0')
   WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) AS diferente_do_sorteio""",
        "distribuicao": f"""
WITH p AS (
  SELECT {CASE} AS linha, num_cpf, max(CASE WHEN tp_grupo = 'controle' THEN 1 ELSE 0 END) AS c
  FROM tb_mov_randomizacao_log WHERE {f} GROUP BY 1, 2
)
SELECT linha, count(*) AS pacientes, sum(c) AS no_controle, sum(c) / count(*) AS pct_controle, 0.05 AS meta FROM p GROUP BY linha
UNION ALL
SELECT '{aba['total']}', count(*), sum(c), sum(c) / count(*), 0.05 FROM p""",
        "volume": vol + """,
c AS (SELECT * FROM VALUES ('Hoje: 5%', 0.05, 1), ('Com 10%', 0.10, 2) AS t(cenario, fracao, ordem))
SELECT c.cenario, c.ordem, 'Enviados à navegação' AS grupo, round(m.pacientes_mes * (1 - c.fracao)) AS pacientes_mes FROM c CROSS JOIN mes m
UNION ALL
SELECT c.cenario, c.ordem, 'Grupo controle', round(m.pacientes_mes * c.fracao) FROM c CROSS JOIN mes m
ORDER BY ordem, grupo""",
        "prazo": vol + f""",
c AS (SELECT * FROM VALUES ('Com 10%', 0.10, 1), ('Se ficasse em 5%', 0.05, 2) AS t(cenario, fracao, ordem)),
acum AS (
  SELECT c.cenario, c.ordem,
         round({Z2} * {P} * (1 - {P}) * (1 / c.fracao + 1 / (1 - c.fracao)) / ({D} * {D}) / nullif(m.pacientes_mes * 0.95, 0), 1) AS meses
  FROM c CROSS JOIN mes m
)
SELECT cenario, ordem, '1. Juntar pacientes no controle' AS etapa, meses FROM acum
UNION ALL SELECT cenario, ordem, '2. Acompanhar cada paciente 90 dias', 3.0 FROM acum
UNION ALL SELECT cenario, ordem, '3. Base de consumo atualizar', 0.5 FROM acum
ORDER BY ordem, etapa""",
    }


def textos(aba):
    t = onda.textos(2, {"nomes": "", "prd": "", "sem_volume": aba["sem_volume"]})
    t["titulo"] = [f"# Navegação da Captação — grupo controle: {aba['titulo']}\n", "\n", f"# {aba['subtitulo']}"]
    t["s2"] = [s.replace("a Onda 2 entra", "cada onda entra").replace("A Onda 2 usa", "Cada onda usa") for s in t["s2"]]
    t["leitura3"] = [s.replace("O volume mensal da Onda 2", "O volume mensal") for s in t["leitura3"]]
    t["s6"] = [s.replace("Onda 2", aba["nome"]) for s in t["s6"]]
    return t


def pagina(aba):
    page = json.loads(json.dumps(BASE["pages"][0]))
    sfx = aba["id"]
    page["name"] = f"pg_{sfx}"
    page["displayName"] = aba["nome"]
    if not aba.get("aprovada"):
        tx = textos(aba)
        for it in page["layout"]:
            if it["position"]["y"] >= 2:
                it["position"]["y"] += 2
            w = it["widget"]
            if "multilineTextboxSpec" in w and w["name"] in tx:
                w["multilineTextboxSpec"]["lines"] = tx[w["name"]]
            if w["name"] == "g_dia":
                w["queries"][0]["query"]["fields"][0] = {"name": "dia_txt", "expression": "`dia_txt`"}
                enc = w["spec"]["encodings"]
                enc["x"] = {"fieldName": "dia_txt", "scale": {"type": "categorical"}, "axis": {"title": ""}}
                enc["color"]["displayName"] = "Grupo"
                enc["label"] = {"show": True}
        page["layout"].insert(1, {"widget": {"name": "aviso", "multilineTextboxSpec": {"lines": [aba["aviso"]]}},
                                  "position": {"x": 0, "y": 2, "width": 12, "height": 2}})
    for it in page["layout"]:
        w = it["widget"]
        w["name"] = f"{w['name']}_{sfx}"
        for q in w.get("queries", []):
            q["query"]["datasetName"] = f"{q['query']['datasetName']}_{sfx}"
    txt = json.dumps(page, ensure_ascii=False)
    if not aba.get("aprovada"):
        txt = txt.replace("Onda 1 (total)", aba["total"])
        if aba.get("vol_pacientes"):
            txt = txt.replace("Volume real da Onda 1 desde 14/10 (103 unidades com navegação), projetado para um mês",
                              "Pacientes sorteados em produção desde 14/10, projetados para um mês")
        else:
            txt = txt.replace("Volume real da Onda 1 desde 14/10 (103 unidades com navegação), projetado para um mês",
                              f"Exames elegíveis ({aba['amb_volume'].upper()}) desde 14/10, projetados para um mês")
            txt = txt.replace("Pacientes por mês nos dois cenários", "Exames elegíveis por mês nos dois cenários")
        txt = txt.replace("Para enxergar um aumento de 8 pontos na taxa de retorno (de 58% para 66%), Onda 1 somada",
                          f"Para enxergar um aumento de 8 pontos na taxa de retorno (de 58% para 66%), {aba['nome']}")
        txt = txt.replace("Pacientes sorteados por dia — o controle é a fatia laranja",
                          f"Pacientes sorteados por dia ({aba['amb_sorteio'].upper()}) — o controle é a fatia laranja")
    return json.loads(txt)


def datasets(aba):
    sfx = aba["id"]
    out = []
    for ds in BASE["datasets"]:
        d = json.loads(json.dumps(ds))
        d["name"] = f"{ds['name']}_{sfx}"
        d["displayName"] = f"{aba['nome']} — {ds.get('displayName', ds['name'])}"
        if aba.get("aprovada"):
            q = "".join(ds["queryLines"]).replace("WHERE tp_ambiente = 'prd'",
                                                  "WHERE tp_ambiente = 'prd' AND nm_especialidade IN ('tirads', 'reumatologia')")
            d["queryLines"] = [l + "\n" for l in q.strip().splitlines()]
        else:
            d["queryLines"] = [l + "\n" for l in sqls(aba)[ds["name"]].strip().splitlines()]
        out.append(d)
    return out


def main():
    painel = {"datasets": [d for a in ABAS for d in datasets(a)], "pages": [pagina(a) for a in ABAS],
              "uiSettings": BASE.get("uiSettings", {})}
    out = AQUI / "painel-negocio-consolidado-v1.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(painel["datasets"]), "datasets", len(painel["pages"]), "abas")


if __name__ == "__main__":
    main()
