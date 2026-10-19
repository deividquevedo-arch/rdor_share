"""Painel AI/BI do uplift da captação — v1 (linha visual do protótipo v1).

Duas páginas:
  1. Demonstração — dados SIMULADOS gerados no próprio SQL (nenhuma tabela criada), com aviso no topo.
  2. Uplift — dados reais (randomização em PRD + base de consumo); se preenche quando a janela fechar.

Uso: python gerar_painel_uplift.py [--textos textos.json]  ->  painel-uplift-v1.lvdash.json
Os títulos-mensagem da demonstração são fixos (o AI/BI não gera texto dinâmico) e vêm de textos.json,
calculado a partir dos próprios datasets da demonstração.
"""
import argparse
import json
from pathlib import Path

AQUI = Path(__file__).parent
OP = "mlops_fabrica_ia.operacao"


def sql_base():
    linhas = [l for l in (AQUI / "sql_demo_base.sql").read_text(encoding="utf-8").splitlines()
              if not l.strip().startswith("--")]
    return "\n".join(linhas)


B = sql_base()

# Linha "Onda 1" = as duas linhas juntas, repetida em cada dataset para os gráficos da onda.
UNIAO = "SELECT linha, grupo, janela, valor, dias_ate_1o_consumo FROM b UNION ALL " \
        "SELECT 'Onda 1', grupo, janela, valor, dias_ate_1o_consumo FROM b"

PHI = ("(CASE WHEN z >= 0 THEN 1 - pd ELSE pd END)")  # usado com t, pd calculados abaixo

SQL = {}

SQL["demo_resumo"] = f"""
WITH b AS ({B}), u AS ({UNIAO}),
g AS (
  SELECT linha, janela,
         count_if(grupo = 'Navegação') AS n_nav, count_if(grupo = 'Controle') AS n_ctrl,
         avg(CASE WHEN grupo = 'Navegação' THEN CASE WHEN valor > 0 THEN 1.0 ELSE 0.0 END END) AS conv_nav,
         avg(CASE WHEN grupo = 'Controle'  THEN CASE WHEN valor > 0 THEN 1.0 ELSE 0.0 END END) AS conv_ctrl,
         avg(CASE WHEN grupo = 'Navegação' THEN valor END) AS percap_nav,
         avg(CASE WHEN grupo = 'Controle'  THEN valor END) AS percap_ctrl,
         var_samp(CASE WHEN grupo = 'Navegação' THEN valor END) AS var_nav,
         var_samp(CASE WHEN grupo = 'Controle'  THEN valor END) AS var_ctrl
  FROM u GROUP BY 1, 2
),
e AS (
  SELECT *, percap_nav - percap_ctrl AS uplift,
         sqrt(var_nav / n_nav + var_ctrl / n_ctrl) AS ep
  FROM g
),
z AS (
  SELECT *, uplift / ep AS z, 1 / (1 + 0.2316419 * abs(uplift / ep)) AS t FROM e
),
p AS (
  SELECT *, 0.3989423 * exp(-z * z / 2) * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274)))) AS pd
  FROM z
)
SELECT linha, janela, n_nav, n_ctrl,
       round(conv_nav, 4) AS conv_nav, round(conv_ctrl, 4) AS conv_ctrl,
       round(conv_nav - conv_ctrl, 4) AS conv_dif,
       round(percap_nav, 2) AS percap_nav, round(percap_ctrl, 2) AS percap_ctrl,
       round(uplift, 2) AS uplift, round(uplift - 1.96 * ep, 2) AS ic_inf, round(uplift + 1.96 * ep, 2) AS ic_sup,
       round(uplift / percap_ctrl, 4) AS uplift_pct,
       round(uplift * n_nav, 0) AS uplift_total,
       round({PHI}, 4) AS prob_ganho,
       round(2 * (CASE WHEN z >= 0 THEN pd ELSE 1 - pd END), 4) AS p_valor,
       CASE WHEN uplift - 1.96 * ep > 0 THEN 'ganho claro' ELSE 'ainda não dá para afirmar' END AS leitura
FROM p
"""

SQL["demo_pacientes"] = f"""
WITH b AS ({B})
SELECT linha, grupo, valor FROM b WHERE janela = 90 AND valor > 0
"""

SQL["demo_quartis"] = f"""
WITH b AS ({B}), u AS ({UNIAO})
SELECT linha, grupo,
       count(*) AS pacientes_que_consumiram,
       round(min(valor), 0) AS minimo, round(percentile(valor, 0.25), 0) AS q1,
       round(percentile(valor, 0.5), 0) AS mediana, round(avg(valor), 0) AS media,
       round(percentile(valor, 0.75), 0) AS q3, round(max(valor), 0) AS maximo
FROM u WHERE janela = 90 AND valor > 0
GROUP BY 1, 2
"""

SQL["demo_faixas"] = f"""
WITH b AS ({B}), u AS ({UNIAO}),
f AS (
  SELECT linha, grupo,
         CASE WHEN valor < 250 THEN '1 · até R$ 250' WHEN valor < 500 THEN '2 · R$ 250–500'
              WHEN valor < 1000 THEN '3 · R$ 500–1 mil' WHEN valor < 2000 THEN '4 · R$ 1–2 mil'
              WHEN valor < 5000 THEN '5 · R$ 2–5 mil' ELSE '6 · acima de R$ 5 mil' END AS faixa
  FROM u WHERE janela = 90 AND valor > 0
)
SELECT linha, grupo, faixa, count(*) AS pacientes,
       round(count(*) * 1.0 / sum(count(*)) OVER (PARTITION BY linha, grupo), 4) AS pct_do_grupo
FROM f GROUP BY 1, 2, 3
"""

SQL["demo_conversao_dia"] = f"""
WITH b AS ({B}), u AS ({UNIAO}),
d AS (SELECT explode(sequence(0, 180, 5)) AS dia)
SELECT u.linha, u.grupo, d.dia,
       round(avg(CASE WHEN u.dias_ate_1o_consumo <= d.dia THEN 1.0 ELSE 0.0 END), 4) AS pct_convertidos
FROM u CROSS JOIN d WHERE u.janela = 180
GROUP BY 1, 2, 3
"""

SQL["demo_semana"] = f"""
WITH ids AS (
  SELECT id, CASE WHEN id < 2780 THEN 'Reumatologia' ELSE 'Tireoide' END AS linha,
         CASE WHEN (id < 2780 AND id < 280) OR (id >= 2780 AND id - 2780 < 1020) THEN 'Controle' ELSE 'Navegação' END AS grupo
  FROM range(0, 13000, 1, 1)
)
SELECT 'ver base' AS nota
"""  # substituído abaixo: a curva semanal reaproveita a base com janela por semana

SQL["real_populacao"] = f"""
SELECT nm_especialidade AS linha,
       CASE WHEN tp_grupo = 'controle' THEN 'Controle' ELSE 'Navegação' END AS grupo,
       count(DISTINCT num_cpf) AS pacientes, min(to_date(dt_execucao_modelo)) AS desde
FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'
GROUP BY 1, 2
"""

SQL["real_operacional"] = f"""
SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia,
       max_by(qt_intervencao, dh_registro) AS enviados, max_by(qt_controle, dh_registro) AS retidos
FROM {OP}.tb_mov_randomizacao_relatorio
WHERE tp_ambiente = 'prd' AND qt_controle IS NOT NULL
GROUP BY 1, 2
"""

SQL["real_uplift"] = f"""
WITH pac AS (
  SELECT nm_especialidade AS linha,
         CASE WHEN tp_grupo = 'controle' THEN 'Controle' ELSE 'Navegação' END AS grupo,
         lpad(cast(num_cpf AS string), 11, '0') AS cpf, min(to_date(dt_execucao_modelo)) AS d0
  FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' GROUP BY 1, 2, 3
),
lim AS (SELECT max(Dt_Consumo) AS dmax FROM gold_corporativo_ia.corporativo.vw_mov_fato_consumo),
cons AS (
  SELECT lpad(regexp_replace(num_cpf_paciente_d, '[^0-9]', ''), 11, '0') AS cpf, Dt_Consumo, Vl_Produto
  FROM gold_corporativo_ia.corporativo.vw_mov_fato_consumo
  WHERE trim(Cd_Destino) <> 'H' AND Dt_Consumo >= date_sub(current_date(), 400)
),
j AS (SELECT explode(array(30, 90, 180)) AS janela),
pp AS (
  SELECT p.linha, p.grupo, p.cpf, j.janela,
         CASE WHEN date_add(p.d0, j.janela) <= lim.dmax THEN 1 ELSE 0 END AS completa,
         max(CASE WHEN c.Dt_Consumo > p.d0 AND c.Dt_Consumo <= date_add(p.d0, j.janela) THEN 1 ELSE 0 END) AS converteu,
         coalesce(sum(CASE WHEN c.Dt_Consumo > p.d0 AND c.Dt_Consumo <= date_add(p.d0, j.janela) THEN c.Vl_Produto END), 0) AS valor
  FROM pac p CROSS JOIN j CROSS JOIN lim LEFT JOIN cons c ON c.cpf = p.cpf
  GROUP BY 1, 2, 3, 4, 5
)
SELECT linha, grupo, janela, count(*) AS pacientes, sum(completa) AS pacientes_janela_completa,
       round(avg(CASE WHEN completa = 1 THEN converteu END), 4) AS pct_convertidos,
       round(avg(CASE WHEN completa = 1 THEN valor END), 2) AS consumo_por_paciente
FROM pp GROUP BY 1, 2, 3
"""


def finalizar_semana():
    """Consumo acumulado por paciente, por semana desde o sorteio (0–26), a partir da base simulada."""
    b = B.replace("FROM pac CROSS JOIN (SELECT explode(array(30, 90, 180)) AS janela) j",
                  "FROM pac CROSS JOIN (SELECT explode(sequence(0, 182, 7)) AS janela) j")
    assert b != B, "padrao da janela nao encontrado na base"
    return f"""
WITH b AS ({b}),
u AS (SELECT linha, grupo, janela, valor FROM b UNION ALL SELECT 'Onda 1', grupo, janela, valor FROM b)
SELECT linha, grupo, cast(janela / 7 AS INT) AS semana, round(avg(valor), 2) AS consumo_acumulado_por_paciente
FROM u GROUP BY 1, 2, 3
"""


SQL["demo_semana"] = finalizar_semana()

TITULOS = {
    "demo_resumo": "Demonstração — resumo por linha e janela", "demo_pacientes": "Demonstração — pacientes (90 dias)",
    "demo_quartis": "Demonstração — quartis (90 dias)", "demo_faixas": "Demonstração — faixas de valor (90 dias)",
    "demo_conversao_dia": "Demonstração — conversão por dia", "demo_semana": "Demonstração — consumo por semana",
    "real_populacao": "Real — população sorteada", "real_operacional": "Real — envios e retidos por dia",
    "real_uplift": "Real — uplift por janela",
}

COR = {"Navegação": "#0B7A80", "Controle": "#9AA6A9"}


def campo(n, e=None):
    return {"name": n, "expression": e or f"`{n}`"}


def q(ds, campos, disag=False, nome="main_query"):
    return [{"name": nome, "query": {"datasetName": ds, "fields": campos, "disaggregated": disag}}]


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def texto(nome, linhas, p):
    return {"widget": {"name": nome, "multilineTextboxSpec": {"lines": linhas}}, "position": p}


def contador(nome, titulo, ds, col, p, filtro_sql=None, fmt=None):
    expr = f"SUM(`{col}`)" if not filtro_sql else f"SUM(CASE WHEN {filtro_sql} THEN `{col}` END)"
    enc = {"value": {"fieldName": "v", "rowNumber": 0}}
    if fmt:
        enc["value"]["format"] = fmt
    return {"widget": {"name": nome, "queries": q(ds, [campo("v", expr)]),
                       "spec": {"version": 2, "widgetType": "counter", "frame": {"showTitle": True, "title": titulo},
                                "encodings": enc}}, "position": p}


def cor_grupo():
    return {"fieldName": "grupo", "displayName": "grupo",
            "scale": {"type": "categorical", "mappings": [{"value": k, "color": v} for k, v in COR.items()]}}


def barras(nome, titulo, ds, x, y_expr, y_nome, p, cor=True, temporal=False, horizontal=False, fmt_y=None):
    campos = [campo(x), campo(y_nome, y_expr)] + ([campo("grupo")] if cor else [])
    ex = {"fieldName": x, "displayName": x, "scale": {"type": "temporal" if temporal else "categorical"}}
    ey = {"fieldName": y_nome, "displayName": y_nome, "scale": {"type": "quantitative"}}
    if fmt_y:
        ey["format"] = fmt_y
    enc = {"x": ey, "y": ex} if horizontal else {"x": ex, "y": ey}
    if cor:
        enc["color"] = cor_grupo()
    enc["label"] = {"show": True}
    return {"widget": {"name": nome, "queries": q(ds, campos),
                       "spec": {"version": 3, "widgetType": "bar", "frame": {"showTitle": True, "title": titulo},
                                "encodings": enc, "mark": {"layout": "group"}}}, "position": p}


def linhas_graf(nome, titulo, ds, x, y_expr, y_nome, p, fmt_y=None):
    ey = {"fieldName": y_nome, "displayName": y_nome, "scale": {"type": "quantitative"}}
    if fmt_y:
        ey["format"] = fmt_y
    return {"widget": {"name": nome, "queries": q(ds, [campo(x), campo(y_nome, y_expr), campo("grupo")]),
                       "spec": {"version": 3, "widgetType": "line", "frame": {"showTitle": True, "title": titulo},
                                "encodings": {"x": {"fieldName": x, "displayName": x, "scale": {"type": "quantitative"}},
                                              "y": ey, "color": cor_grupo()}}}, "position": p}


def boxplot(nome, titulo, ds, p):
    return {"widget": {"name": nome, "queries": q(ds, [campo("grupo"), campo("valor")], disag=True),
                       "spec": {"version": 3, "widgetType": "box", "frame": {"showTitle": True, "title": titulo},
                                "encodings": {"x": {"fieldName": "grupo", "displayName": "grupo", "scale": {"type": "categorical"}},
                                              "y": {"fieldName": "valor", "displayName": "consumo (R$)", "scale": {"type": "quantitative"}},
                                              "color": cor_grupo()}}}, "position": p}


def tabela(nome, titulo, ds, cols, p):
    return {"widget": {"name": nome, "queries": q(ds, [campo(c) for c in cols], disag=True),
                       "spec": {"version": 2, "widgetType": "table", "frame": {"showTitle": True, "title": titulo},
                                "encodings": {"columns": [{"fieldName": c} for c in cols]}}}, "position": p}


PCT = {"type": "number-percent", "decimalPlaces": {"type": "exact", "places": 1}}
BRL = {"type": "number-currency", "currencyCode": "BRL", "decimalPlaces": {"type": "exact", "places": 0}}


def pagina_demo(t):
    o = "linha = 'Onda 1' AND janela = 90"
    return [
        texto("d_aviso", ["### ⚠️ DADOS SIMULADOS — demonstração do formato\n",
                          "Nenhum número desta página é resultado real. Mostra como o painel ficará quando houver desfecho "
                          "de consumo. Premissas: ~12 meses da onda 1 com 10% de controle; controle converte e consome menos."],
              pos(0, 0, 12, 2)),
        texto("d_headline", [f"# {t['headline']}\n", t["dek"]], pos(0, 2, 12, 2)),
        contador("d_k_uplift", "Consumo a mais por paciente (90 dias)", "demo_resumo", "uplift", pos(0, 4, 3, 3), o, BRL),
        contador("d_k_total", "Consumo atribuível à navegação", "demo_resumo", "uplift_total", pos(3, 4, 3, 3), o, BRL),
        contador("d_k_conv", "Conversão a mais (p.p.)", "demo_resumo", "conv_dif", pos(6, 4, 3, 3), o, PCT),
        contador("d_k_prob", "Probabilidade de ganho", "demo_resumo", "prob_ganho", pos(9, 4, 3, 3), o, PCT),
        texto("d_t_conv", [f"## {t['t_conv']}"], pos(0, 7, 12, 1)),
        linhas_graf("d_g_conv", "% do grupo que já consumiu, por dia após o sorteio — onda 1", "demo_conversao_dia",
                    "dia", "SUM(CASE WHEN `linha` = 'Onda 1' THEN `pct_convertidos` END)", "pct_convertidos",
                    pos(0, 8, 7, 6), PCT),
        barras("d_g_convbar", "% convertidos em 90 dias — por linha", "demo_resumo", "linha",
               "SUM(CASE WHEN `janela` = 90 THEN `conv_nav` END)", "navegação", pos(7, 8, 5, 6), cor=False, fmt_y=PCT),
        texto("d_t_valor", [f"## {t['t_valor']}"], pos(0, 14, 12, 1)),
        boxplot("d_g_box", "Consumo de quem consumiu — 90 dias (boxplot)", "demo_pacientes", pos(0, 15, 6, 7)),
        barras("d_g_faixas", "Distribuição sobreposta: % de cada grupo por faixa de valor — onda 1, 90 dias", "demo_faixas",
               "faixa", "SUM(CASE WHEN `linha` = 'Onda 1' THEN `pct_do_grupo` END)", "% do grupo", pos(6, 15, 6, 7), fmt_y=PCT),
        tabela("d_t_quartis", "Resumo estatístico — quem consumiu em 90 dias", "demo_quartis",
               ["linha", "grupo", "pacientes_que_consumiram", "minimo", "q1", "mediana", "media", "q3", "maximo"],
               pos(0, 22, 12, 4)),
        texto("d_t_tempo", [f"## {t['t_tempo']}"], pos(0, 26, 12, 1)),
        linhas_graf("d_g_semana", "Consumo acumulado por paciente sorteado — semanas desde o sorteio, onda 1", "demo_semana",
                    "semana", "SUM(CASE WHEN `linha` = 'Onda 1' THEN `consumo_acumulado_por_paciente` END)",
                    "consumo acumulado por paciente", pos(0, 27, 12, 6), BRL),
        texto("d_t_onde", [f"## {t['t_onde']}\n",
                           "A decisão é pela onda. Por linha, o intervalo é largo; cruzar o zero quer dizer que ainda não "
                           "dá para ver, não que não há efeito."], pos(0, 33, 12, 2)),
        barras("d_g_linha", "Consumo a mais por paciente — 90 dias", "demo_resumo", "linha",
               "SUM(CASE WHEN `janela` = 90 THEN `uplift` END)", "uplift por paciente", pos(0, 35, 5, 6),
               cor=False, horizontal=True, fmt_y=BRL),
        tabela("d_t_linha", "Uplift com intervalo de 95% — por linha e janela", "demo_resumo",
               ["linha", "janela", "percap_nav", "percap_ctrl", "uplift", "ic_inf", "ic_sup", "prob_ganho", "leitura"],
               pos(5, 35, 7, 6)),
        texto("d_next", ["## Próximo passo\n", t["next"]], pos(0, 41, 12, 2)),
    ]


def pagina_real():
    return [
        texto("r_cab", ["## Uplift da captação — dados reais\n",
                        "Randomização em produção desde 12/10 (reumatologia e tireoide). Os indicadores de consumo se "
                        "preenchem quando a janela de 30 dias fechar na base de consumo (meados de novembro); até lá, "
                        "**pacientes com janela completa = 0**."], pos(0, 0, 12, 2)),
        contador("r_k_nav", "Pacientes na navegação", "real_populacao", "pacientes", pos(0, 2, 3, 3), "grupo = 'Navegação'"),
        contador("r_k_ctrl", "Pacientes no controle", "real_populacao", "pacientes", pos(3, 2, 3, 3), "grupo = 'Controle'"),
        contador("r_k_comp", "Com janela de 30 dias completa", "real_uplift", "pacientes_janela_completa", pos(6, 2, 3, 3),
                 "janela = 30"),
        contador("r_k_ret", "Exames retidos no controle", "real_operacional", "retidos", pos(9, 2, 3, 3)),
        barras("r_g_pop", "Pacientes por linha e grupo", "real_populacao", "linha", "SUM(`pacientes`)", "pacientes",
               pos(0, 5, 5, 6)),
        {"widget": {"name": "r_g_oper", "queries": q("real_operacional", [campo("dia"), campo("enviados", "SUM(`enviados`)"),
                                                                           campo("retidos", "SUM(`retidos`)")]),
                    "spec": {"version": 3, "widgetType": "bar", "frame": {"showTitle": True, "title": "Exames enviados e retidos por dia"},
                             "encodings": {"x": {"fieldName": "dia", "scale": {"type": "temporal"}},
                                           "y": {"scale": {"type": "quantitative"}, "fields": [
                                               {"fieldName": "enviados", "displayName": "enviados"},
                                               {"fieldName": "retidos", "displayName": "retidos"}]}}}},
         "position": pos(5, 5, 7, 6)},
        tabela("r_t_uplift", "Uplift por janela — preenche com o desfecho", "real_uplift",
               ["linha", "grupo", "janela", "pacientes", "pacientes_janela_completa", "pct_convertidos", "consumo_por_paciente"],
               pos(0, 11, 12, 6)),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--textos", default=str(AQUI / "textos-demo.json"))
    a = ap.parse_args()
    t = json.loads(Path(a.textos).read_text(encoding="utf-8"))
    datasets = [{"name": k, "displayName": TITULOS[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]}
                for k, v in SQL.items()]
    painel = {"datasets": datasets, "pages": [
        {"name": "demonstracao", "displayName": "Demonstração (simulado)", "layout": pagina_demo(t)},
        {"name": "uplift_real", "displayName": "Uplift — dados reais", "layout": pagina_real()},
    ]}
    out = AQUI / "painel-uplift-v1.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(datasets), "datasets")


if __name__ == "__main__":
    main()
