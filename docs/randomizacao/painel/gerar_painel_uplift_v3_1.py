"""Painel AI/BI do uplift — v3.1: correções da v3 (box sem scale no eixo x; medidas com parênteses e % não resolvidas na tabela).

v3: v2 redesenhada pelo guia "AI/BI Dashboards — Boas práticas (Out 2026)".

Uso: python gerar_painel_uplift_v3_1.py  ->  painel-uplift-v3.1.lvdash.json
Não altera a v1 nem a v2. Mesma base SIMULADA da v2 (sql_demo_base_v2.sql), então os números batem.

O que muda em relação à v2, prática a prática do guia:
  02 Consolidar datasets — 7 datasets (cada um repetindo a simulação inteira) viram 3, um por grão:
     pacientes (paciente × janela), trajetoria (grupo × linha × dia) e distribuicao (grupo × linha × faixa).
     Os KPIs saem de CALCULATIONS no dataset (`columns`), não de SQL por visual. Os agregados guardam
     numerador e denominador (SUM/SUM), então continuam certos com qualquer combinação de linhas filtrada.
  03 Primeiro carregamento — páginas por intenção (Resultado · Distribuição · Detalhe); um único filtro
     global (Linha); a janela é filtro de página com padrão restritivo de 90 dias.
  03 Filtro de campo — o dataset maior tem ~39 mil linhas (< 100 mil): filtro e cross-filter no navegador.
  04 Cache — consultas determinísticas (rand com semente). Publicação com permissões compartilhadas e
     schedule ficam para quando houver dado real (dashboard em rascunho não usa o cache compartilhado).
  01 Metric view — fica para o dado real: a fonte aqui é uma simulação em SQL, sem tabela. Quando a base
     de desfecho existir, os mesmos cálculos viram local metric view e depois metric view no UC.
"""
import json
from pathlib import Path

AQUI = Path(__file__).parent
B = "\n".join(l for l in (AQUI / "sql_demo_base_v2.sql").read_text(encoding="utf-8").splitlines()
              if not l.strip().startswith("--"))
JANELAS_ORIG = "FROM pac CROSS JOIN (SELECT explode(array(30, 90, 180)) AS janela) j"
assert JANELAS_ORIG in B, "padrao da janela nao encontrado na base v2"
B_SEMANAL = B.replace(JANELAS_ORIG, "FROM pac CROSS JOIN (SELECT explode(sequence(0, 182, 7)) AS janela) j")

NAV, CTRL, GAIN = "#0B7A80", "#C9811A", "#2A8A50"
GRUPO = {"Navegação": NAV, "Controle": CTRL}
BRL = {"type": "number-currency", "currencyCode": "BRL", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 0}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}

NAVC = "`grupo` = 'Navegação'"
CTLC = "`grupo` = 'Controle'"
EP = (f"SQRT(VAR_SAMP(CASE WHEN {NAVC} THEN `valor` END) / COUNT(CASE WHEN {NAVC} THEN 1 END) + "
      f"VAR_SAMP(CASE WHEN {CTLC} THEN `valor` END) / COUNT(CASE WHEN {CTLC} THEN 1 END))")
UPL = f"(AVG(CASE WHEN {NAVC} THEN `valor` END) - AVG(CASE WHEN {CTLC} THEN `valor` END))"

# Calculations do dataset de pacientes: cada KPI definido UMA vez e reutilizado pelos widgets.
MEDIDAS_PACIENTES = [
    ("Pacientes sorteados", "COUNT(`valor`)", "Navegação + controle"),
    ("Pacientes no controle", f"COUNT(CASE WHEN {CTLC} THEN 1 END)", ""),
    ("Retorno — navegação", f"AVG(CASE WHEN {NAVC} THEN `consumiu` END)", "% que voltou a consumir"),
    ("Retorno — controle", f"AVG(CASE WHEN {CTLC} THEN `consumiu` END)", "% que voltou a consumir"),
    ("Retorno a mais", f"AVG(CASE WHEN {NAVC} THEN `consumiu` END) - AVG(CASE WHEN {CTLC} THEN `consumiu` END)",
     "Diferença em pontos percentuais"),
    ("Consumo por paciente — navegação", f"AVG(CASE WHEN {NAVC} THEN `valor` END)", "Zero para quem não voltou"),
    ("Consumo por paciente — controle", f"AVG(CASE WHEN {CTLC} THEN `valor` END)", "Zero para quem não voltou"),
    ("Uplift por paciente", UPL, "Navegação − controle, por paciente sorteado"),
    ("Ganho sobre o controle", f"{UPL} / AVG(CASE WHEN {CTLC} THEN `valor` END)", ""),
    ("Uplift limite inferior 95", f"{UPL} - 1.96 * {EP}", ""),
    ("Uplift limite superior 95", f"{UPL} + 1.96 * {EP}", ""),
    ("Uplift total no período", f"{UPL} * COUNT(CASE WHEN {NAVC} THEN 1 END)", "Uplift por paciente × pacientes navegados"),
]

SQL = {
    "pacientes": f"""
WITH b AS ({B})
SELECT linha, grupo, janela,
       concat(cast(janela AS STRING), ' dias') AS janela_obs,
       round(valor, 2) AS valor,
       CASE WHEN valor > 0 THEN 1 ELSE 0 END AS consumiu,
       CASE WHEN valor > 0 AND valor < 6000 THEN round(valor, 2) END AS consumo_se_voltou
FROM b
""",
    "trajetoria": f"""
WITH b AS ({B_SEMANAL})
SELECT linha, grupo, janela AS dia,
       count(*) AS n_pacientes,
       sum(CASE WHEN valor > 0 THEN 1 ELSE 0 END) AS n_voltaram,
       sum(valor) AS soma_consumo,
       sum(CASE WHEN grupo = 'Navegação' THEN 1 ELSE 0 END) AS n_nav,
       sum(CASE WHEN grupo = 'Navegação' THEN valor ELSE 0 END) AS soma_nav,
       sum(CASE WHEN grupo = 'Controle' THEN 1 ELSE 0 END) AS n_ctrl,
       sum(CASE WHEN grupo = 'Controle' THEN valor ELSE 0 END) AS soma_ctrl
FROM b GROUP BY 1, 2, 3
""",
    # % do grupo por faixa de R$ 100, entre quem voltou em 90 dias. Contagem suavizada (média móvel de 5
    # faixas) e o total do grupo repetido em cada faixa: SUM(n)/SUM(total) por faixa soma cada linha uma vez.
    "distribuicao": f"""
WITH b AS ({B}),
v AS (SELECT linha, grupo, least(floor(valor / 100), 49) AS faixa FROM b WHERE janela = 90 AND valor > 0),
grade AS (SELECT l.linha, l.grupo, f.faixa FROM (SELECT DISTINCT linha, grupo FROM v) l
          CROSS JOIN (SELECT explode(sequence(0, 49)) AS faixa) f),
c AS (SELECT g.linha, g.grupo, g.faixa, count(v.faixa) AS n
      FROM grade g LEFT JOIN v ON v.linha = g.linha AND v.grupo = g.grupo AND v.faixa = g.faixa
      GROUP BY 1, 2, 3),
t AS (SELECT linha, grupo, count(*) AS n_total FROM v GROUP BY 1, 2)
SELECT c.linha, c.grupo, c.faixa * 100 + 50 AS consumo_rs,
       avg(c.n) OVER (PARTITION BY c.linha, c.grupo ORDER BY c.faixa ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS n_suave,
       t.n_total
FROM c JOIN t ON t.linha = c.linha AND t.grupo = c.grupo
""",
}
COLUNAS = {
    "pacientes": MEDIDAS_PACIENTES,
    "trajetoria": [
        ("% que já voltou", "SUM(`n_voltaram`) / SUM(`n_pacientes`)", ""),
        ("Consumo acumulado por paciente", "SUM(`soma_consumo`) / SUM(`n_pacientes`)", ""),
        ("Uplift acumulado por paciente", "SUM(`soma_nav`) / SUM(`n_nav`) - SUM(`soma_ctrl`) / SUM(`n_ctrl`)", ""),
    ],
    "distribuicao": [("% do grupo", "SUM(`n_suave`) / SUM(`n_total`)", "")],
}
NOMES = {"pacientes": "Pacientes sorteados (paciente × janela) — SIMULADO",
         "trajetoria": "Trajetória semanal (grupo × linha × dia) — SIMULADO",
         "distribuicao": "Distribuição do consumo, 90 dias (grupo × linha × faixa) — SIMULADO"}


def campo(n, e=None):
    return {"name": n, "expression": e or f"`{n}`"}


def m(nome):
    return campo(nome, f"MEASURE(`{nome}`)")


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def frame(titulo, desc=None):
    f = {"showTitle": True, "title": titulo}
    if desc:
        f.update(showDescription=True, description=desc)
    return f


def cores(c, mapa):
    return {"fieldName": c, "displayName": " ",
            "scale": {"type": "categorical", "mappings": [{"value": k, "color": v} for k, v in mapa.items()]}}


def w(nome, ds, campos, spec, p, disag=False):
    return {"widget": {"name": nome, "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": campos, "disaggregated": disag}}], "spec": spec}, "position": p}


def texto(nome, linhas, p):
    return {"widget": {"name": nome, "multilineTextboxSpec": {"lines": linhas}}, "position": p}


def kpi(nome, medida, titulo, desc, fmt, p):
    return w(nome, "pacientes", [m(medida)], {"version": 2, "widgetType": "counter", "frame": frame(titulo, desc),
             "encodings": {"value": {"fieldName": medida, "format": fmt}}}, p)


def linha_m(nome, ds, medida, titulo, desc, x_tit, fmt, p, cor=True):
    campos = [campo("dia"), m(medida)] + ([campo("grupo")] if cor else [])
    enc = {"x": {"fieldName": "dia", "scale": {"type": "quantitative"}, "axis": {"title": x_tit}},
           "y": {"fieldName": medida, "scale": {"type": "quantitative"}, "format": fmt, "axis": {"title": ""}}}
    if cor:
        enc["color"] = cores("grupo", GRUPO)
    return w(nome, ds, campos, {"version": 3, "widgetType": "line", "frame": frame(titulo, desc), "encodings": enc}, p)


def filtro(nome, titulo, tipo, campo_nome, dss, p, padrao=None):
    qs = [{"name": f"f_{campo_nome}_{ds}", "query": {"datasetName": ds, "fields": [campo(campo_nome)],
                                                       "disaggregated": False}} for ds in dss]
    spec = {"version": 2, "widgetType": tipo, "frame": frame(titulo),
            "encodings": {"fields": [{"fieldName": campo_nome, "displayName": titulo, "queryName": q["name"]} for q in qs]}}
    if padrao:
        spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": padrao}]}}}
    return {"widget": {"name": nome, "queries": qs, "spec": spec}, "position": p}


AVISO = ("**⚠️ DADOS SIMULADOS** — demonstração do formato do painel de uplift; nenhum número é resultado real. "
         "Onda 1 (Reumatologia + Tireoide) · cenário de ~12 meses com 10% de controle.")


def pagina_resultado():
    return [
        texto("aviso1", [AVISO], pos(0, 0, 9, 1)),
        filtro("f_janela1", "Janela de observação", "filter-single-select", "janela_obs", ["pacientes"], pos(9, 0, 3, 1), "90 dias"),
        kpi("k_upl", "Uplift por paciente", "Consumo a mais por paciente", "Navegação − controle, por paciente sorteado", BRL, pos(0, 1, 3, 3)),
        kpi("k_ret", "Retorno a mais", "Retorno a mais", "Diferença no % que voltou a consumir na Rede", PCT, pos(3, 1, 3, 3)),
        kpi("k_pct", "Ganho sobre o controle", "Ganho sobre o controle", "Quanto a navegação consome a mais, em %", PCT, pos(6, 1, 3, 3)),
        kpi("k_tot", "Uplift total no período", "Uplift total no período", "Por paciente × pacientes navegados", BRL, pos(9, 1, 3, 3)),
        w("g_ret", "pacientes", [campo("grupo"), campo("y", "AVG(`consumiu`)")],
          {"version": 3, "widgetType": "bar", "frame": frame("Mais pacientes voltam à Rede", "% do grupo que voltou a consumir"),
           "encodings": {"x": {"fieldName": "grupo", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "y": {"fieldName": "y", "scale": {"type": "quantitative"}, "format": PCT, "axis": {"title": ""}},
                         "color": cores("grupo", GRUPO), "label": {"show": True}}}, pos(0, 4, 4, 6)),
        w("g_linha", "pacientes", [campo("linha"), m("Uplift por paciente")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Consumo a mais por paciente, por linha", "A decisão é pela onda; por linha é descritivo"),
           "encodings": {"x": {"fieldName": "Uplift por paciente", "scale": {"type": "quantitative"}, "format": BRL, "axis": {"title": ""}},
                         "y": {"fieldName": "linha", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "label": {"show": True}}}, pos(4, 4, 4, 6)),
        texto("leitura1", [
            "### Como ler",
            "**Consumo a mais por paciente** é a diferença entre os grupos, contando zero para quem não voltou.",
            "",
            "**Retorno a mais** é o indicador principal: responde mais rápido e varia menos que o valor em R$.",
            "",
            "Os detalhes de intervalo de confiança estão na página **Detalhe**."], pos(8, 4, 4, 6)),
        linha_m("g_acum", "trajetoria", "Consumo acumulado por paciente", "O ganho se abre semana a semana",
                "Consumo acumulado por paciente sorteado (zero para quem não voltou)", "Dias desde o sorteio", BRL, pos(0, 10, 6, 6)),
        linha_m("g_volta", "trajetoria", "% que já voltou", "E voltam antes", "% do grupo que já teve algum consumo",
                "Dias desde o sorteio", PCT, pos(6, 10, 6, 6)),
    ]


def pagina_distribuicao():
    return [
        texto("aviso2", [AVISO], pos(0, 0, 9, 1)),
        filtro("f_janela2", "Janela de observação", "filter-single-select", "janela_obs", ["pacientes"], pos(9, 0, 3, 1), "90 dias"),
        w("g_box", "pacientes", [campo("grupo"), campo("consumo_se_voltou")],
          {"version": 1, "widgetType": "box",
           "frame": frame("Consumo de quem voltou — navegação × controle", "Caixa = 50% centrais · linha = mediana"),
           "encodings": {"x": {"fieldName": "grupo", "displayName": "Grupo", "scale": {"type": "categorical"}},
                         "y": {"fieldName": "consumo_se_voltou", "displayName": "Consumo (R$)", "scale": {"type": "quantitative"}}}},
          pos(0, 1, 6, 7), disag=True),
        w("g_dens", "distribuicao", [campo("consumo_rs"), m("% do grupo"), campo("grupo")],
          {"version": 3, "widgetType": "line",
           "frame": frame("A navegação desloca a curva para a direita",
                          "% de cada grupo por faixa de R$ 100, entre quem voltou em 90 dias (curva suavizada)"),
           "encodings": {"x": {"fieldName": "consumo_rs", "scale": {"type": "quantitative"}, "axis": {"title": "Consumo em 90 dias (R$)"}},
                         "y": {"fieldName": "% do grupo", "scale": {"type": "quantitative"}, "format": PCT, "axis": {"title": ""}},
                         "color": cores("grupo", GRUPO)}}, pos(6, 1, 6, 7)),
        linha_m("g_upl_sem", "trajetoria", "Uplift acumulado por paciente", "Diferença acumulada entre os grupos",
                "Navegação − controle, por paciente sorteado", "Dias desde o sorteio", BRL, pos(0, 8, 8, 6), cor=False),
        texto("leitura2", [
            "### Por que olhar a distribuição",
            "O consumo tem cauda longa: poucos pacientes concentram valores muito altos.",
            "",
            "Por isso a **mediana** (linha da caixa) diz mais que a média, e o **retorno** é o indicador principal."],
            pos(8, 8, 4, 6)),
    ]


def pagina_detalhe():
    cols = ["linha", "janela_obs", "Pacientes sorteados", "Pacientes no controle", "Retorno — navegação", "Retorno — controle",
            "Consumo por paciente — navegação", "Consumo por paciente — controle", "Uplift por paciente",
            "Uplift limite inferior 95", "Uplift limite superior 95"]
    campos = [campo("linha"), campo("janela_obs")] + [m(c) for c in cols[2:]]
    return [
        texto("aviso3", [AVISO, "",
                         "Ganho **claro** quando o limite inferior é maior que zero. Por linha isolada a leitura é descritiva: "
                         "a decisão é pela onda somada."], pos(0, 0, 12, 2)),
        w("t_det", "pacientes", campos, {"version": 2, "widgetType": "table", "frame": frame("Uplift por linha e janela, com intervalo de 95%"),
                                         "encodings": {"columns": [{"fieldName": c} for c in cols]}}, pos(0, 2, 12, 8)),
    ]


def pagina_filtros():
    return [filtro("f_linha", "Linha de cuidado", "filter-multi-select", "linha",
                   ["pacientes", "trajetoria", "distribuicao"], pos(0, 0, 4, 1))]


def main():
    datasets = []
    for k, v in SQL.items():
        datasets.append({"name": k, "displayName": NOMES[k],
                         "queryLines": [l + "\n" for l in v.strip().splitlines()],
                         "columns": [{"displayName": n, "description": d, "expression": e} for n, e, d in COLUNAS[k]]})
    pg = lambda name, disp, lay, t="PAGE_TYPE_CANVAS": {"name": name, "displayName": disp, "pageType": t,
                                                        "layoutVersion": "GRID_V1", "layout": lay}
    painel = {
        "datasets": datasets,
        "pages": [pg("resultado", "Resultado", pagina_resultado()),
                  pg("distribuicao", "Distribuição", pagina_distribuicao()),
                  pg("detalhe", "Detalhe", pagina_detalhe()),
                  pg("filtros", "Filtros", pagina_filtros(), "PAGE_TYPE_GLOBAL_FILTERS")],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#F7F8F9", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#1D2B36", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
            "visualizationColors": [NAV, CTRL, GAIN, "#7FB8BB", "#4E5185"],
            "widgetHeaderAlignment": "LEFT", "widgetCornerRadius": 10}},
    }
    out = AQUI / "painel-uplift-v3.1.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(datasets), "datasets", sum(len(p["layout"]) for p in painel["pages"]), "widgets")


if __name__ == "__main__":
    main()
