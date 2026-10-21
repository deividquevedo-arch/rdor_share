"""Painel AI/BI do uplift — v5: cenário PORTFÓLIO Q4 (+15% no consumo por paciente, metade no retorno e metade no
ticket), base calibrada com o medido, e gráficos revistos contra objetivo e tipo de dado.

Revisão dos gráficos (v4 -> v5):
  - Efeito por linha: barra sem incerteza -> GRÁFICO DE INTERVALO (estimativa + IC 95% + linha do zero), para o
    retorno e para o consumo. O objetivo é decidir se há efeito; sem o intervalo a barra engana.
  - Veredito em texto (tabela de 1 linha) no lugar de dois contadores de p-valor, que mostram 0,000 quando p é pequeno.
  - Consumo tem cauda longa (CV ~3,5): boxplot e curva de distribuição em ESCALA LOG; a curva usa faixas logarítmicas.
  - Equilíbrio antes do sorteio: "% que consumiu antes" no lugar da média em R$ (média com cauda longa oscila por acaso).
  - Consumo por paciente MÊS A MÊS removido: com ~100 pacientes de controle por mês e cauda longa, a linha só mostra ruído.
  - Captados por mês fica, como VOLUME (o controle é 10% por desenho; não é comparação entre grupos).

v4: a v3.2 completada com o que faltava para o negócio confiar no número.

Uso: python gerar_painel_uplift_v5.py  ->  painel-uplift-v5.lvdash.json
Não altera as versões anteriores. Base SIMULADA própria (sql_demo_base_v4.sql), com campos novos.

O que entra sobre a v3.2 (avaliação de 20/10):
  Bloco 1 — retorno como KPI principal, com p-valor e veredito na 1ª página; n por grupo; ticket de quem
            voltou; dias até o 1º consumo; tabela de estatísticas (média, mediana, desvio, mín., máx.,
            outliers, faixa mais frequente); linha do tempo MENSAL (calendário); "total no período" sai da
            1ª página.
  Bloco 2 — página "Os grupos são comparáveis?" (idade, sexo, consumo antes do sorteio, mistura de linhas,
            casamento do CPF, janela completa); corp × onco e produto da linha; progresso até o resultado.
  Bloco 3 — o que depende de definição ou de dado real fica visível como "a definir".
Mantém as práticas do guia AI/BI (Out 2026) aplicadas na v3: poucos datasets por grão, calculations para os
KPIs, páginas por intenção, filtros de página. Estatística com VAR/SQRT/PERCENTILE vai em dataset SQL
(não resolve em calculations — v3.1); boxplot em Vega-Lite (o `box` nativo exige quartis prontos).
A decisão é pela ONDA SOMADA (CA16): a 1ª página não tem filtro de linha.
"""
import json
from pathlib import Path

AQUI = Path(__file__).parent
P = "\n".join(l for l in (AQUI / "sql_demo_base_v5.sql").read_text(encoding="utf-8").splitlines()
              if not l.strip().startswith("--"))

NAV, CTRL, GAIN = "#0B7A80", "#C9811A", "#2A8A50"
GRUPO = {"Navegação": NAV, "Controle": CTRL}
LINHAS = {"Tireoide": "#4E5185", "Reumatologia": "#7FB8BB"}
BRL = {"type": "number-currency", "currencyCode": "BRL", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 0}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
NUM0 = {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}
NUM3 = {"type": "number", "decimalPlaces": {"type": "max", "places": 3}}
N_CTRL_NECESSARIO = 332  # +8 p.p. sobre 58%, 95%/80%, razão 9:1 (10% de controle) — ver horizonte-uplift-por-linha-v0.md

VALOR = "CASE WHEN t > janela THEN 0.0 ELSE base * least(1.0, (janela - t) / 90.0 * ritmo + 0.35) END"
PACIENTES_SQL = f"""
WITH pac AS ({P}),
x AS (
  SELECT pac.*, j.janela,
         CASE WHEN date_add(dt_sorteio, j.janela + 14) <= dt_corte THEN 1 ELSE 0 END AS janela_completa
  FROM pac CROSS JOIN (SELECT explode(array(30, 90, 180)) AS janela) j
),
y AS (SELECT x.*, CASE WHEN casou_cpf = 1 AND janela_completa = 1 THEN 1 ELSE 0 END AS analisavel FROM x)
SELECT linha, grupo, janela, concat(cast(janela AS STRING), ' dias') AS janela_obs,
       date_trunc('MONTH', dt_sorteio) AS mes_sorteio, idade, feminino, consumo_pre, casou_cpf,
       CASE WHEN consumo_pre > 0 THEN 1 ELSE 0 END AS consumiu_antes,
       janela_completa, analisavel,
       CASE WHEN analisavel = 1 THEN round({VALOR}, 2) END AS valor,
       CASE WHEN analisavel = 1 THEN CASE WHEN t <= janela THEN 1 ELSE 0 END END AS consumiu,
       CASE WHEN analisavel = 1 AND t <= janela THEN round(t, 1) END AS dias_1o_consumo,
       CASE WHEN analisavel = 1 THEN round({VALOR} * sh_onco, 2) END AS valor_onco,
       CASE WHEN analisavel = 1 THEN round({VALOR} * (1 - sh_onco), 2) END AS valor_corp,
       CASE WHEN analisavel = 1 THEN round({VALOR} * sh_produto, 2) END AS valor_produto,
       CASE WHEN analisavel = 1 AND t <= janela THEN round({VALOR}, 2) END AS consumo_se_voltou
FROM y
"""

# Cauda superior da normal padrão Q(|z|) (aprox. de Abramowitz-Stegun, a mesma da v2); p bilateral = 2·Q.
def p_bilateral(z):
    t = f"(1 / (1 + 0.2316419 * abs({z})))"
    return (f"least(1.0, 2 * 0.3989423 * exp(-({z}) * ({z}) / 2) * {t} * (0.3193815 + {t} * (-0.3565638 + {t} * "
            f"(1.781478 + {t} * (-1.821256 + {t} * 1.330274)))))")


SQL = {
    "pacientes": PACIENTES_SQL,
    "teste": f"""
WITH a AS (SELECT * FROM ({PACIENTES_SQL}) WHERE analisavel = 1),
g AS (
  SELECT coalesce(linha, 'Onda 1 (somada)') AS linha, janela, concat(cast(janela AS STRING), ' dias') AS janela_obs,
         count_if(grupo = 'Navegação') AS n_nav, count_if(grupo = 'Controle') AS n_ctrl,
         avg(CASE WHEN grupo = 'Navegação' THEN consumiu END) AS ret_nav,
         avg(CASE WHEN grupo = 'Controle' THEN consumiu END) AS ret_ctrl,
         avg(consumiu) AS ret_pool,
         avg(CASE WHEN grupo = 'Navegação' THEN valor END) AS cons_nav,
         avg(CASE WHEN grupo = 'Controle' THEN valor END) AS cons_ctrl,
         var_samp(CASE WHEN grupo = 'Navegação' THEN valor END) AS var_nav,
         var_samp(CASE WHEN grupo = 'Controle' THEN valor END) AS var_ctrl,
         percentile(CASE WHEN grupo = 'Navegação' THEN dias_1o_consumo END, 0.5) AS dias_nav,
         percentile(CASE WHEN grupo = 'Controle' THEN dias_1o_consumo END, 0.5) AS dias_ctrl,
         avg(CASE WHEN grupo = 'Navegação' AND consumiu = 1 THEN valor END) - avg(CASE WHEN grupo = 'Controle' AND consumiu = 1 THEN valor END) AS ticket_a_mais,
         avg(CASE WHEN grupo = 'Navegação' THEN valor_corp END) - avg(CASE WHEN grupo = 'Controle' THEN valor_corp END) AS corp_a_mais,
         avg(CASE WHEN grupo = 'Navegação' THEN valor_onco END) - avg(CASE WHEN grupo = 'Controle' THEN valor_onco END) AS onco_a_mais,
         avg(CASE WHEN grupo = 'Navegação' THEN valor_produto END) - avg(CASE WHEN grupo = 'Controle' THEN valor_produto END) AS produto_a_mais
  FROM a GROUP BY GROUPING SETS ((linha, janela), (janela))
),
z AS (
  SELECT *, ret_nav - ret_ctrl AS retorno_a_mais, cons_nav - cons_ctrl AS uplift,
         sqrt(var_nav / n_nav + var_ctrl / n_ctrl) AS ep,
         sqrt(ret_nav * (1 - ret_nav) / n_nav + ret_ctrl * (1 - ret_ctrl) / n_ctrl) AS ep_ret,
         (ret_nav - ret_ctrl) / sqrt(ret_pool * (1 - ret_pool) * (1.0 / n_nav + 1.0 / n_ctrl)) AS z_ret
  FROM g
)
SELECT linha, janela, janela_obs, n_nav, n_ctrl,
       CASE WHEN linha = 'Onda 1 (somada)' THEN 0 ELSE 1 END AS ordem,
       ret_nav, ret_ctrl, retorno_a_mais, {p_bilateral('z_ret')} AS p_valor_retorno,
       retorno_a_mais - 1.96 * ep_ret AS ret_li, retorno_a_mais + 1.96 * ep_ret AS ret_ls,
       round(cons_nav, 2) AS consumo_nav, round(cons_ctrl, 2) AS consumo_ctrl, round(uplift, 2) AS uplift,
       round(uplift - 1.96 * ep, 2) AS limite_inferior, round(uplift + 1.96 * ep, 2) AS limite_superior,
       {p_bilateral('uplift / ep')} AS p_valor_consumo,
       round(ticket_a_mais, 2) AS ticket_a_mais, round(dias_ctrl - dias_nav, 1) AS dias_a_menos,
       round(corp_a_mais, 2) AS corp_a_mais, round(onco_a_mais, 2) AS onco_a_mais, round(produto_a_mais, 2) AS produto_a_mais,
       round(uplift * n_nav, 0) AS uplift_total,
       CASE WHEN {p_bilateral('z_ret')} < 0.05 AND retorno_a_mais > 0 THEN 'retorno maior, diferença significativa'
            ELSE 'ainda não dá para afirmar' END AS leitura,
       CASE WHEN {p_bilateral('uplift / ep')} < 0.05 AND uplift > 0 THEN 'consumo maior, diferença significativa'
            ELSE 'ainda não dá para afirmar' END AS leitura_consumo
FROM z
""",
    "estatisticas": f"""
WITH v AS (SELECT * FROM ({PACIENTES_SQL}) WHERE analisavel = 1),
q AS (
  SELECT coalesce(linha, 'Onda 1 (somada)') AS linha, janela, concat(cast(janela AS STRING), ' dias') AS janela_obs, grupo,
         count(*) AS analisados, sum(consumiu) AS captados, avg(consumiu) AS retorno,
         avg(CASE WHEN consumiu = 1 THEN valor END) AS media,
         percentile(CASE WHEN consumiu = 1 THEN valor END, 0.5) AS mediana,
         stddev(CASE WHEN consumiu = 1 THEN valor END) AS desvio,
         min(CASE WHEN consumiu = 1 THEN valor END) AS minimo,
         max(CASE WHEN consumiu = 1 THEN valor END) AS maximo,
         percentile(CASE WHEN consumiu = 1 THEN valor END, 0.25) AS q1,
         percentile(CASE WHEN consumiu = 1 THEN valor END, 0.75) AS q3,
         percentile(dias_1o_consumo, 0.5) AS mediana_dias
  FROM v GROUP BY GROUPING SETS ((linha, janela, grupo), (janela, grupo))
),
f AS (
  SELECT coalesce(linha, 'Onda 1 (somada)') AS linha, janela, grupo, floor(valor / 100) AS faixa, count(*) AS n
  FROM v WHERE consumiu = 1 GROUP BY GROUPING SETS ((linha, janela, grupo, floor(valor / 100)), (janela, grupo, floor(valor / 100)))
),
moda AS (SELECT linha, janela, grupo, max_by(faixa, n) AS faixa FROM f GROUP BY 1, 2, 3),
o AS (
  SELECT q.linha, q.janela, q.grupo, count(v.valor) AS outliers
  FROM q LEFT JOIN v ON v.janela = q.janela AND v.grupo = q.grupo AND v.consumiu = 1
                    AND (q.linha = 'Onda 1 (somada)' OR v.linha = q.linha)
                    AND v.valor > q.q3 + 1.5 * (q.q3 - q.q1)
  GROUP BY 1, 2, 3
)
SELECT q.linha, q.janela, q.janela_obs, q.grupo, q.analisados, q.captados, q.retorno,
       round(q.media, 0) AS media, round(q.mediana, 0) AS mediana, round(q.desvio, 0) AS desvio,
       round(q.minimo, 0) AS minimo, round(q.maximo, 0) AS maximo, o.outliers, round(q.mediana_dias, 0) AS mediana_dias_1o_consumo,
       concat('R$ ', cast(cast(moda.faixa * 100 AS INT) AS STRING), ' a ', cast(cast(moda.faixa * 100 + 100 AS INT) AS STRING)) AS faixa_mais_frequente
FROM q JOIN moda ON moda.linha = q.linha AND moda.janela = q.janela AND moda.grupo = q.grupo
       JOIN o ON o.linha = q.linha AND o.janela = q.janela AND o.grupo = q.grupo
""",
    # Só quem tem 182 + 14 dias observados entra na trajetória, para todas as semanas usarem os mesmos pacientes.
    "trajetoria": f"""
WITH pac AS ({P}),
b AS (
  SELECT pac.*, d.janela FROM pac CROSS JOIN (SELECT explode(sequence(0, 182, 7)) AS janela) d
  WHERE casou_cpf = 1 AND date_add(dt_sorteio, 196) <= dt_corte
),
v AS (SELECT linha, grupo, janela AS dia, {VALOR} AS valor FROM b)
SELECT linha, grupo, dia, count(*) AS n_pacientes, sum(CASE WHEN valor > 0 THEN 1 ELSE 0 END) AS n_voltaram,
       sum(valor) AS soma_consumo,
       sum(CASE WHEN grupo = 'Navegação' THEN 1 ELSE 0 END) AS n_nav, sum(CASE WHEN grupo = 'Navegação' THEN valor ELSE 0 END) AS soma_nav,
       sum(CASE WHEN grupo = 'Controle' THEN 1 ELSE 0 END) AS n_ctrl, sum(CASE WHEN grupo = 'Controle' THEN valor ELSE 0 END) AS soma_ctrl
FROM v GROUP BY 1, 2, 3
""",
    "distribuicao": f"""
WITH v0 AS (SELECT * FROM ({PACIENTES_SQL}) WHERE janela = 90 AND consumiu = 1 AND valor > 0),
v AS (SELECT linha, grupo, floor(log10(valor) * 8) AS faixa FROM v0),
lim AS (SELECT min(faixa) AS f0, max(faixa) AS f1 FROM v),
grade AS (SELECT l.linha, l.grupo, f.faixa FROM (SELECT DISTINCT linha, grupo FROM v) l
          CROSS JOIN (SELECT explode(sequence(cast(f0 AS INT), cast(f1 AS INT))) AS faixa FROM lim) f),
c AS (SELECT g.linha, g.grupo, g.faixa, count(v.faixa) AS n
      FROM grade g LEFT JOIN v ON v.linha = g.linha AND v.grupo = g.grupo AND v.faixa = g.faixa GROUP BY 1, 2, 3),
t AS (SELECT linha, grupo, count(*) AS n_total FROM v GROUP BY 1, 2)
SELECT c.linha, c.grupo, round(pow(10, (c.faixa + 0.5) / 8), 0) AS consumo_rs,
       avg(c.n) OVER (PARTITION BY c.linha, c.grupo ORDER BY c.faixa ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n_suave,
       t.n_total
FROM c JOIN t ON t.linha = c.linha AND t.grupo = c.grupo
""",
}

SQL["veredito"] = f"""
SELECT janela_obs, retorno_a_mais, p_valor_retorno, leitura AS leitura_retorno, uplift, p_valor_consumo, leitura_consumo
FROM ({SQL['teste']}) WHERE linha = 'Onda 1 (somada)'
"""

NAVC, CTLC = "`grupo` = 'Navegação'", "`grupo` = 'Controle'"
UPL = f"(AVG(CASE WHEN {NAVC} THEN `valor` END) - AVG(CASE WHEN {CTLC} THEN `valor` END))"
COLUNAS = {
    "pacientes": [
        ("Analisados navegação", f"COUNT(CASE WHEN {NAVC} THEN `valor` END)", "CPF casado e janela completa"),
        ("Analisados controle", f"COUNT(CASE WHEN {CTLC} THEN `valor` END)", "CPF casado e janela completa"),
        ("Retorno", "AVG(`consumiu`)", "% que voltou a consumir na Rede"),
        ("Retorno navegação", f"AVG(CASE WHEN {NAVC} THEN `consumiu` END)", ""),
        ("Retorno controle", f"AVG(CASE WHEN {CTLC} THEN `consumiu` END)", ""),
        ("Retorno a mais", f"AVG(CASE WHEN {NAVC} THEN `consumiu` END) - AVG(CASE WHEN {CTLC} THEN `consumiu` END)", "Pontos percentuais"),
        ("Captados", "SUM(`consumiu`)", "Pacientes que voltaram a consumir"),
        ("Consumo por paciente", "AVG(`valor`)", "Zero para quem não voltou"),
        ("Uplift por paciente", UPL, "Navegação − controle, por paciente analisado"),
        ("Ganho sobre o controle", f"{UPL} / AVG(CASE WHEN {CTLC} THEN `valor` END)", ""),
        ("Ticket de quem voltou", "AVG(CASE WHEN `consumiu` = 1 THEN `valor` END)", ""),
        ("Idade média", "AVG(CASE WHEN `janela` = 90 THEN `idade` END)", "Todos os sorteados"),
        ("Mulheres", "AVG(CASE WHEN `janela` = 90 THEN `feminino` END)", "Todos os sorteados"),
        ("Consumiu antes do sorteio", "AVG(CASE WHEN `janela` = 90 THEN `consumiu_antes` END)", "% com algum consumo nos 12 meses anteriores"),
        ("Sorteados", "COUNT(CASE WHEN `janela` = 90 THEN 1 END)", ""),
        ("Tireoide no grupo", "AVG(CASE WHEN `janela` = 90 THEN CASE WHEN `linha` = 'Tireoide' THEN 1 ELSE 0 END END)", ""),
        ("CPF casado com a base", "AVG(CASE WHEN `janela` = 90 THEN `casou_cpf` END)", ""),
        ("Janela completa", "AVG(`janela_completa`)", "% dos sorteados com a janela inteira observada"),
        ("Controle analisado em 90 dias", f"COUNT(CASE WHEN {CTLC} AND `janela` = 90 THEN `valor` END)", ""),
        ("Progresso ate o resultado", f"COUNT(CASE WHEN {CTLC} AND `janela` = 90 THEN `valor` END) / {N_CTRL_NECESSARIO}", ""),
    ],
    "trajetoria": [
        ("Ja voltou", "SUM(`n_voltaram`) / SUM(`n_pacientes`)", "% que já teve algum consumo"),
        ("Consumo acumulado por paciente", "SUM(`soma_consumo`) / SUM(`n_pacientes`)", ""),
        ("Uplift acumulado por paciente", "SUM(`soma_nav`) / SUM(`n_nav`) - SUM(`soma_ctrl`) / SUM(`n_ctrl`)", ""),
    ],
    "distribuicao": [("Parcela do grupo", "SUM(`n_suave`) / SUM(`n_total`)", "")],
    "teste": [], "estatisticas": [], "veredito": [],
}
NOMES = {"pacientes": "Pacientes sorteados (paciente × janela) — SIMULADO",
         "teste": "Teste por linha e janela (p-valor, intervalo) — SIMULADO",
         "estatisticas": "Estatísticas descritivas por grupo — SIMULADO",
         "trajetoria": "Trajetória semanal desde o sorteio — SIMULADO",
         "distribuicao": "Distribuição do consumo, 90 dias, faixas logarítmicas — SIMULADO",
         "veredito": "Veredito da Onda 1 somada — SIMULADO"}


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


def kpi(nome, ds, expr, titulo, desc, fmt, p):
    return w(nome, ds, [campo("v", expr)], {"version": 2, "widgetType": "counter", "frame": frame(titulo, desc),
             "encodings": {"value": {"fieldName": "v", "format": fmt}}}, p)


def onda(col):
    return f"SUM(CASE WHEN `linha` = 'Onda 1 (somada)' THEN `{col}` END)"


def barra_grupo(nome, ds, medida, titulo, desc, fmt, p):
    return w(nome, ds, [campo("grupo"), m(medida)],
             {"version": 3, "widgetType": "bar", "frame": frame(titulo, desc),
              "encodings": {"x": {"fieldName": "grupo", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                            "y": {"fieldName": medida, "scale": {"type": "quantitative"}, "format": fmt, "axis": {"title": ""}},
                            "color": cores("grupo", GRUPO), "label": {"show": True}}}, p)


def linha_g(nome, ds, x, medida, titulo, desc, x_tit, fmt, p, temporal=False, cor=True, tipo="line"):
    campos = [campo(x), m(medida)] + ([campo("grupo")] if cor else [])
    enc = {"x": {"fieldName": x, "scale": {"type": "temporal" if temporal else "quantitative"}, "axis": {"title": x_tit}},
           "y": {"fieldName": medida, "scale": {"type": "quantitative"}, "format": fmt, "axis": {"title": ""}}}
    if cor:
        enc["color"] = cores("grupo", GRUPO)
    return w(nome, ds, campos, {"version": 3, "widgetType": tipo, "frame": frame(titulo, desc), "encodings": enc}, p)


def filtro(nome, titulo, tipo, campo_nome, dss, p, padrao=None):
    qs = [{"name": f"f_{nome}_{ds}", "query": {"datasetName": ds, "fields": [campo(campo_nome)], "disaggregated": False}}
          for ds in dss]
    spec = {"version": 2, "widgetType": tipo, "frame": frame(titulo),
            "encodings": {"fields": [{"fieldName": campo_nome, "displayName": titulo, "queryName": q["name"]} for q in qs]}}
    if padrao:
        spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": padrao}]}}}
    return {"widget": {"name": nome, "queries": qs, "spec": spec}, "position": p}


VEGA_BOX = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "data": {"name": "databricks_query"},
    "transform": [{"filter": "isValid(datum.consumo_se_voltou)"}],
    "mark": {"type": "boxplot", "extent": 1.5, "outliers": False, "size": 60, "median": {"color": "white", "strokeWidth": 2}},
    "encoding": {
        "x": {"field": "grupo", "type": "nominal", "title": None, "sort": ["Navegação", "Controle"]},
        "y": {"field": "consumo_se_voltou", "type": "quantitative", "title": "Consumo de quem voltou (R$, escala log)",
              "scale": {"type": "log"}},
        "color": {"field": "grupo", "type": "nominal", "legend": None,
                  "scale": {"domain": ["Navegação", "Controle"], "range": [NAV, CTRL]}},
    },
    "width": "container", "height": "container",
    "config": {"autosize": {"type": "fit", "contains": "padding"}},
}
def vega_intervalo(est, li, ls, titulo_x, fmt):
    """Gráfico de intervalo: ponto = estimativa, haste = IC 95%, linha tracejada = zero (sem efeito)."""
    y = {"field": "linha", "type": "nominal", "title": None, "sort": {"field": "ordem"}}
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"name": "databricks_query"},
        "layer": [
            {"mark": {"type": "rule", "strokeDash": [4, 4], "color": "#8A949B"}, "encoding": {"x": {"datum": 0}}},
            {"mark": {"type": "rule", "strokeWidth": 3, "color": NAV},
             "encoding": {"y": y, "x": {"field": li, "type": "quantitative", "title": titulo_x, "axis": {"format": fmt}},
                          "x2": {"field": ls}}},
            {"mark": {"type": "point", "filled": True, "size": 120, "color": NAV},
             "encoding": {"y": y, "x": {"field": est, "type": "quantitative"},
                          "tooltip": [{"field": "linha"}, {"field": est, "format": fmt}, {"field": li, "format": fmt},
                                      {"field": ls, "format": fmt}]}},
        ],
        "width": "container", "height": "container",
        "config": {"autosize": {"type": "fit", "contains": "padding"}},
    }


VEGA_DENS = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "data": {"name": "databricks_query"},
    "transform": [
        {"aggregate": [{"op": "sum", "field": "n_suave", "as": "n"}, {"op": "sum", "field": "n_total", "as": "tot"}],
         "groupby": ["grupo", "consumo_rs"]},
        {"calculate": "datum.n / datum.tot", "as": "parcela"},
    ],
    "mark": {"type": "line", "strokeWidth": 2.5, "interpolate": "monotone"},
    "encoding": {
        "x": {"field": "consumo_rs", "type": "quantitative", "scale": {"type": "log"}, "title": "Consumo em 90 dias (R$, escala log)"},
        "y": {"field": "parcela", "type": "quantitative", "axis": {"format": ".0%"}, "title": None},
        "color": {"field": "grupo", "type": "nominal", "title": None,
                  "scale": {"domain": ["Navegação", "Controle"], "range": [NAV, CTRL]}},
    },
    "width": "container", "height": "container",
    "config": {"autosize": {"type": "fit", "contains": "padding"}},
}


def vega_widget(nome, ds, spec, campos_nomes, titulo, desc, p):
    return w(nome, ds, [campo(c) for c in campos_nomes],
             {"version": 1, "widgetType": "custom-vega-viz", "frame": frame(titulo, desc),
              "jsonSpec": {"type": "vega-lite", "spec": json.dumps(spec, ensure_ascii=False)},
              "encodings": {"fields": [{"fieldName": c} for c in campos_nomes]}, "data": {"queryName": "main_query"}},
             p, disag=True)


AVISO = ("**⚠️ DADOS SIMULADOS — cenário Portfólio Q4:** efeito verdadeiro de **+15% no consumo por paciente** "
         "(metade no retorno, metade no ticket; meta 'a confirmar'). Onda 1 · 12 meses com 10% de controle. "
         "Nenhum número é resultado real.")


def pag_resultado():
    cver = ("leitura_retorno", "p_valor_retorno", "leitura_consumo", "p_valor_consumo")
    return [
        texto("aviso1", [AVISO], pos(0, 0, 9, 2)),
        filtro("f_jan1", "Janela de observação", "filter-single-select", "janela_obs", ["pacientes", "teste", "veredito"], pos(9, 0, 3, 2), "90 dias"),
        texto("t_dec", ["### A navegação aumenta o retorno do paciente à Rede? — Onda 1 somada"], pos(0, 2, 12, 1)),
        kpi("k_ret", "pacientes", "MEASURE(`Retorno a mais`)", "Retorno a mais", "Indicador principal · navegação − controle", PCT, pos(0, 3, 3, 3)),
        kpi("k_upl", "pacientes", "MEASURE(`Uplift por paciente`)", "Consumo a mais por paciente", "Indicador secundário · zero para quem não voltou", BRL, pos(3, 3, 3, 3)),
        kpi("k_ganho", "pacientes", "MEASURE(`Ganho sobre o controle`)", "Ganho sobre o controle", "Consumo a mais, em % do controle", PCT, pos(6, 3, 3, 3)),
        kpi("k_tkt", "teste", onda("ticket_a_mais"), "Ticket a mais de quem voltou", "Média de consumo entre quem voltou", BRL, pos(9, 3, 3, 3)),
        w("t_ver", "veredito", [campo(c) for c in cver],
          {"version": 2, "widgetType": "table", "frame": frame("Veredito", "Diferença significativa quando o p-valor é menor que 0,05"),
           "encodings": {"columns": [{"fieldName": c} for c in cver]}}, pos(0, 6, 6, 3), disag=True),
        kpi("k_nnav", "pacientes", "MEASURE(`Analisados navegação`)", "Analisados — navegação", "CPF casado com a base e janela completa", NUM0, pos(6, 6, 3, 3)),
        kpi("k_nctl", "pacientes", "MEASURE(`Analisados controle`)", "Analisados — controle", "CPF casado com a base e janela completa", NUM0, pos(9, 6, 3, 3)),
        vega_widget("g_fret", "teste", vega_intervalo("retorno_a_mais", "ret_li", "ret_ls", "Retorno a mais (p.p.)", ".1%"),
                    ["linha", "ordem", "retorno_a_mais", "ret_li", "ret_ls"], "Retorno a mais, com intervalo de 95%",
                    "Haste inteira à direita do zero = efeito claro · por linha é descritivo", pos(0, 9, 6, 6)),
        vega_widget("g_fcons", "teste", vega_intervalo("uplift", "limite_inferior", "limite_superior", "Consumo a mais por paciente (R$)", ",.0f"),
                    ["linha", "ordem", "uplift", "limite_inferior", "limite_superior"], "Consumo a mais por paciente, com intervalo de 95%",
                    "Com consumo de cauda longa o intervalo é largo — por isso o retorno é o indicador principal", pos(6, 9, 6, 6)),
        barra_grupo("g_ret", "pacientes", "Retorno", "Retorno por grupo", "% do grupo que voltou a consumir", PCT, pos(0, 15, 4, 6)),
        kpi("k_prog", "pacientes", "MEASURE(`Progresso ate o resultado`)", "Progresso até o resultado",
            f"Controle analisado em 90 dias ÷ {N_CTRL_NECESSARIO} necessários para enxergar +8 p.p. no retorno", PCT, pos(4, 15, 4, 3)),
        texto("ler1b", ["Acima de 100%, a amostra de controle já basta para o efeito de referência no retorno."], pos(8, 15, 4, 3)),
        texto("ler1", ["**Como ler:** com o efeito do Portfólio (+15% no consumo), o **retorno** já mostra diferença clara em 12 meses; "
                       "o **consumo em R$** varia tanto de paciente para paciente que o intervalo fica largo. "
                       "Em 10 simulações do mesmo cenário, o consumo a mais saiu entre +7% e +35%; o retorno, entre +3,9 e +9,3 p.p."],
              pos(4, 18, 8, 3)),
    ]


def pag_comparaveis():
    return [
        texto("aviso2", [AVISO, "",
                         "### Os grupos são comparáveis?",
                         "O sorteio é ao acaso, então controle e navegação devem ser **parecidos em tudo antes do sorteio**. "
                         "Se não forem, a diferença depois pode ser do acaso, e o resultado é ajustado pelo consumo anterior."],
              pos(0, 0, 12, 3)),
        barra_grupo("g_idade", "pacientes", "Idade média", "Idade média", "Todos os sorteados", NUM0, pos(0, 3, 3, 5)),
        barra_grupo("g_fem", "pacientes", "Mulheres", "Mulheres", "% dos sorteados", PCT, pos(3, 3, 3, 5)),
        barra_grupo("g_pre", "pacientes", "Consumiu antes do sorteio", "Consumiu antes do sorteio", "% com algum consumo nos 12 meses anteriores", PCT, pos(6, 3, 3, 5)),
        barra_grupo("g_mix", "pacientes", "Tireoide no grupo", "Mistura de linhas", "% de Tireoide entre os sorteados (o resto é Reumatologia)", PCT, pos(9, 3, 3, 5)),
        texto("t_cob", ["### Quanto do dado já pode ser analisado"], pos(0, 8, 12, 1)),
        kpi("k_sort", "pacientes", "MEASURE(`Sorteados`)", "Sorteados", "Navegação + controle", NUM0, pos(0, 9, 3, 3)),
        kpi("k_cpf", "pacientes", "MEASURE(`CPF casado com a base`)", "CPF casado com a base de consumo", "Quem não casa fica fora do resultado", PCT, pos(3, 9, 3, 3)),
        w("g_jan", "pacientes", [campo("janela"), m("Janela completa")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Janela completa", "% dos sorteados com a janela inteira observada (inclui ~2 semanas de atraso da base)"),
           "encodings": {"x": {"fieldName": "janela", "scale": {"type": "categorical"}, "axis": {"title": "Janela (dias)"}},
                         "y": {"fieldName": "Janela completa", "scale": {"type": "quantitative"}, "format": PCT, "axis": {"title": ""}},
                         "label": {"show": True}}}, pos(6, 9, 6, 3)),
    ]


def pag_distribuicao():
    cols = ["linha", "grupo", "analisados", "captados", "retorno", "media", "mediana", "desvio", "minimo", "maximo",
            "outliers", "faixa_mais_frequente", "mediana_dias_1o_consumo"]
    return [
        texto("aviso3", [AVISO], pos(0, 0, 6, 1)),
        filtro("f_lin3", "Linha de cuidado", "filter-multi-select", "linha", ["pacientes", "distribuicao"], pos(6, 0, 3, 1)),
        filtro("f_jan3", "Janela de observação", "filter-single-select", "janela_obs", ["pacientes", "estatisticas"], pos(9, 0, 3, 1), "90 dias"),
        w("g_box", "pacientes", [campo("grupo"), campo("consumo_se_voltou")],
          {"version": 1, "widgetType": "custom-vega-viz",
           "frame": frame("Consumo de quem voltou — navegação × controle", "Caixa = 50% centrais · linha branca = mediana · escala log"),
           "jsonSpec": {"type": "vega-lite", "spec": json.dumps(VEGA_BOX, ensure_ascii=False)},
           "encodings": {"fields": [{"fieldName": "grupo"}, {"fieldName": "consumo_se_voltou"}]},
           "data": {"queryName": "main_query"}}, pos(0, 1, 6, 7), disag=True),
        vega_widget("g_dens", "distribuicao", VEGA_DENS, ["grupo", "consumo_rs", "n_suave", "n_total"],
                    "A navegação desloca a curva para a direita", "% de cada grupo por faixa de consumo, entre quem voltou em 90 dias (escala log)",
                    pos(6, 1, 6, 7)),
        w("t_est", "estatisticas", [campo(c) for c in cols],
          {"version": 2, "widgetType": "table",
           "frame": frame("Estatísticas de quem voltou", "Valores em R$ · outliers = acima de 1,5× a caixa · dias até o 1º consumo entre quem voltou na janela · 'Onda 1 (somada)' soma as linhas"),
           "encodings": {"columns": [{"fieldName": c} for c in cols]}}, pos(0, 8, 12, 6), disag=True),
        texto("ler3", ["**Por que olhar a distribuição:** o consumo tem cauda longa — poucos pacientes concentram valores muito altos. "
                       "Por isso a **mediana** diz mais que a média. A **moda** foi substituída pela **faixa mais frequente**, "
                       "porque valor em R$ quase nunca se repete exatamente."], pos(0, 14, 12, 2)),
    ]


def pag_tempo():
    return [
        texto("aviso4", [AVISO], pos(0, 0, 6, 2)),
        filtro("f_lin4", "Linha de cuidado", "filter-multi-select", "linha", ["pacientes", "trajetoria"], pos(6, 0, 3, 2)),
        filtro("f_jan4", "Janela de observação", "filter-single-select", "janela_obs", ["pacientes"], pos(9, 0, 3, 2), "90 dias"),
        texto("t_mes", ["### Mês a mês, pelo mês do sorteio"], pos(0, 2, 12, 1)),
        linha_g("g_mret", "pacientes", "mes_sorteio", "Retorno", "Retorno por mês do sorteio",
                "Cada mês tem ~100 pacientes de controle: a linha laranja oscila mais · meses recentes sem janela completa ficam de fora",
                "", PCT, pos(0, 3, 6, 6), temporal=True),
        linha_g("g_mcap", "pacientes", "mes_sorteio", "Captados", "Captados por mês do sorteio — volume",
                "Pacientes que voltaram · o controle é 10% por desenho, então a barra mostra volume, não comparação",
                "", NUM0, pos(6, 3, 6, 6), temporal=True, tipo="bar"),
        texto("t_dias", ["### Desde o sorteio — pacientes com 26 semanas observadas"], pos(0, 9, 12, 1)),
        linha_g("g_volta", "trajetoria", "dia", "Ja voltou", "Quanto do grupo já voltou", "% do grupo que já teve algum consumo",
                "Dias desde o sorteio", PCT, pos(0, 10, 4, 6)),
        linha_g("g_acum", "trajetoria", "dia", "Consumo acumulado por paciente", "Consumo acumulado por paciente",
                "Zero para quem não voltou", "Dias desde o sorteio", BRL, pos(4, 10, 4, 6)),
        linha_g("g_upl", "trajetoria", "dia", "Uplift acumulado por paciente", "Diferença acumulada entre os grupos",
                "Navegação − controle, por paciente", "Dias desde o sorteio", BRL, pos(8, 10, 4, 6), cor=False),
    ]


def pag_detalhe():
    cols = ["linha", "janela_obs", "n_nav", "n_ctrl", "ret_nav", "ret_ctrl", "retorno_a_mais", "p_valor_retorno",
            "uplift", "limite_inferior", "limite_superior", "p_valor_consumo", "corp_a_mais", "onco_a_mais",
            "produto_a_mais", "uplift_total", "leitura", "leitura_consumo"]
    return [
        texto("aviso5", [AVISO, "",
                         "Diferença **significativa** quando o p-valor é menor que 0,05. Por linha isolada a leitura é descritiva: a decisão "
                         "é pela onda somada. Corp, onco e produto da linha são o consumo a mais por paciente em cada parte."], pos(0, 0, 12, 2)),
        w("t_det", "teste", [campo(c) for c in cols],
          {"version": 2, "widgetType": "table", "frame": frame("Teste por linha e janela"),
           "encodings": {"columns": [{"fieldName": c} for c in cols]}}, pos(0, 2, 12, 7), disag=True),
        texto("adef", [
            "### A definir — previsto, depende de definição do time ou de dado real",
            "- **Tempo até cirurgia**, controle × navegação — depende de identificar cirurgia na base de consumo.",
            "- **Ticket por atendimento e por procedimento**; **valor do 1º atendimento** — exige a base no grão de atendimento.",
            "- **LTV** — horizonte maior que 180 dias.",
            "- **Por unidade com navegadora ativa e por regional** — lista de unidades do método (§7).",
            "- **Contaminação** — paciente do controle navegado por outra linha ou pelo legado (CA14).",
            "- **Ajuste pelo consumo anterior** (CUPED / diferença em diferenças) — reduz o ruído e encurta o prazo.",
            "- Definições pendentes: o que é **captado**, recorte **corp × onco**, tratamento de **outliers**."], pos(0, 9, 12, 5)),
    ]


def main():
    datasets = []
    for k, v in SQL.items():
        d = {"name": k, "displayName": NOMES[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]}
        if COLUNAS[k]:
            d["columns"] = [{"displayName": n, "description": ds, "expression": e} for n, e, ds in COLUNAS[k]]
        datasets.append(d)
    pg = lambda name, disp, lay: {"name": name, "displayName": disp, "pageType": "PAGE_TYPE_CANVAS",
                                  "layoutVersion": "GRID_V1", "layout": lay}
    painel = {
        "datasets": datasets,
        "pages": [pg("resultado", "Resultado", pag_resultado()),
                  pg("comparaveis", "Os grupos são comparáveis?", pag_comparaveis()),
                  pg("distribuicao", "Distribuição e estatísticas", pag_distribuicao()),
                  pg("tempo", "Ao longo do tempo", pag_tempo()),
                  pg("detalhe", "Detalhe", pag_detalhe())],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#F7F8F9", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#1D2B36", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
            "visualizationColors": [NAV, CTRL, GAIN, "#7FB8BB", "#4E5185"],
            "widgetHeaderAlignment": "LEFT", "widgetCornerRadius": 10}},
    }
    out = AQUI / "painel-uplift-v5.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(datasets), "datasets", sum(len(p["layout"]) for p in painel["pages"]), "widgets")


if __name__ == "__main__":
    main()
