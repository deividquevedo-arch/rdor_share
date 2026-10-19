"""Painel AI/BI do uplift — v2: estilo do protótipo v1 com dados mockados de efeito visível.

Uso: python gerar_painel_uplift_v2.py  ->  painel-uplift-v2.lvdash.json
Não altera a v1. Página única de demonstração (dados SIMULADOS no próprio SQL, nenhuma tabela criada).
"""
import json
from pathlib import Path

AQUI = Path(__file__).parent
B = "\n".join(l for l in (AQUI / "sql_demo_base_v2.sql").read_text(encoding="utf-8").splitlines()
              if not l.strip().startswith("--"))
U = ("SELECT linha, grupo, janela, valor, dias_ate_1o_consumo FROM b UNION ALL "
     "SELECT 'Onda 1', grupo, janela, valor, dias_ate_1o_consumo FROM b")

NAV, CTRL, GAIN = "#0B7A80", "#C9811A", "#2A8A50"

SQL = {
    "resumo": f"""
WITH b AS ({B}), u AS ({U}),
g AS (
  SELECT linha, janela,
         count_if(grupo = 'Navegação') AS n_nav, count_if(grupo = 'Controle') AS n_ctrl,
         avg(CASE WHEN grupo = 'Navegação' THEN CASE WHEN valor > 0 THEN 1.0 ELSE 0.0 END END) AS conv_nav,
         avg(CASE WHEN grupo = 'Controle'  THEN CASE WHEN valor > 0 THEN 1.0 ELSE 0.0 END END) AS conv_ctrl,
         avg(CASE WHEN grupo = 'Navegação' THEN valor END) AS percap_nav,
         avg(CASE WHEN grupo = 'Controle'  THEN valor END) AS percap_ctrl,
         var_samp(CASE WHEN grupo = 'Navegação' THEN valor END) AS var_nav,
         var_samp(CASE WHEN grupo = 'Controle'  THEN valor END) AS var_ctrl
  FROM u GROUP BY 1, 2),
e AS (SELECT *, percap_nav - percap_ctrl AS uplift, sqrt(var_nav / n_nav + var_ctrl / n_ctrl) AS ep FROM g),
z AS (SELECT *, uplift / ep AS z, 1 / (1 + 0.2316419 * abs(uplift / ep)) AS t FROM e),
p AS (SELECT *, 0.3989423 * exp(-z * z / 2) * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274)))) AS pd FROM z)
SELECT linha, janela, n_nav, n_ctrl, conv_nav, conv_ctrl, conv_nav - conv_ctrl AS conv_dif,
       round(percap_nav, 2) AS percap_nav, round(percap_ctrl, 2) AS percap_ctrl,
       round(uplift, 2) AS uplift, round(uplift - 1.96 * ep, 2) AS ic_inf, round(uplift + 1.96 * ep, 2) AS ic_sup,
       uplift / percap_ctrl AS uplift_pct, round(uplift * n_nav, 0) AS uplift_total,
       CASE WHEN z >= 0 THEN 1 - pd ELSE pd END AS prob_ganho,
       CASE WHEN uplift - 1.96 * ep > 0 THEN 'ganho claro' ELSE 'ainda não dá para afirmar' END AS leitura
FROM p""",
    # boxplot: pacientes que consumiram, onda 1, 90 dias
    "box": f"""
WITH b AS ({B})
SELECT grupo, round(valor, 2) AS consumo FROM b WHERE janela = 90 AND valor > 0 AND valor < 6000""",
    "quartis": f"""
WITH b AS ({B}), u AS ({U})
SELECT linha, grupo, count(*) AS consumiram,
       round(percentile(valor, 0.25), 0) AS q1, round(percentile(valor, 0.5), 0) AS mediana,
       round(avg(valor), 0) AS media, round(percentile(valor, 0.75), 0) AS q3,
       round(stddev(valor), 0) AS desvio
FROM u WHERE janela = 90 AND valor > 0 GROUP BY 1, 2""",
    # distribuição sobreposta: % do grupo por faixa de R$ 100, suavizada (média móvel de 5 faixas)
    "densidade": f"""
WITH b AS ({B}),
f AS (SELECT grupo, least(floor(valor / 100), 49) AS faixa FROM b WHERE janela = 90 AND valor > 0),
c AS (SELECT grupo, faixa, count(*) AS n FROM f GROUP BY 1, 2),
t AS (SELECT grupo, sum(n) AS tot FROM c GROUP BY 1),
s AS (SELECT c.grupo, c.faixa, c.n / t.tot AS pct FROM c JOIN t USING (grupo))
SELECT grupo, (faixa * 100 + 50) AS consumo_rs,
       avg(pct) OVER (PARTITION BY grupo ORDER BY faixa ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS pct_do_grupo
FROM s""",
    "conversao": f"""
WITH b AS ({B}), d AS (SELECT explode(sequence(0, 180, 5)) AS dia)
SELECT b.grupo, d.dia, avg(CASE WHEN b.dias_ate_1o_consumo <= d.dia THEN 1.0 ELSE 0.0 END) AS pct_convertidos
FROM b CROSS JOIN d WHERE b.janela = 180 GROUP BY 1, 2""",
    "semana": "",  # preenchido abaixo
    "conv_bar": f"""
WITH b AS ({B})
SELECT grupo, avg(CASE WHEN valor > 0 THEN 1.0 ELSE 0.0 END) AS pct_convertidos FROM b WHERE janela = 90 GROUP BY 1""",
}
b_sem = B.replace("FROM pac CROSS JOIN (SELECT explode(array(30, 90, 180)) AS janela) j",
                  "FROM pac CROSS JOIN (SELECT explode(sequence(0, 182, 7)) AS janela) j")
assert b_sem != B, "padrao da janela nao encontrado"
SQL["semana"] = f"""
WITH b AS ({b_sem}),
g AS (SELECT cast(janela / 7 AS INT) AS semana,
             avg(CASE WHEN grupo = 'Navegação' THEN valor END) AS nav,
             avg(CASE WHEN grupo = 'Controle' THEN valor END) AS ctrl
      FROM b GROUP BY 1)
SELECT semana, 'Navegação' AS serie, round(nav, 2) AS consumo FROM g
UNION ALL SELECT semana, 'Controle', round(ctrl, 2) FROM g
UNION ALL SELECT semana, 'Uplift (diferença)', round(nav - ctrl, 2) FROM g"""

NOMES = {"resumo": "Resumo por linha e janela", "box": "Pacientes que consumiram (90 dias)",
         "quartis": "Quartis (90 dias)", "densidade": "Distribuição por faixa (90 dias)",
         "conversao": "Conversão por dia", "semana": "Consumo acumulado por semana", "conv_bar": "Conversão em 90 dias"}


def campo(n, e=None):
    return {"name": n, "expression": e or f"`{n}`"}


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def frame(titulo, desc=None):
    f = {"showTitle": True, "title": titulo}
    if desc:
        f["showDescription"] = True
        f["description"] = desc
    return f


def cores(campo_cor, mapa):
    return {"fieldName": campo_cor, "displayName": campo_cor,
            "scale": {"type": "categorical", "mappings": [{"value": k, "color": v} for k, v in mapa.items()]}}


GRUPO = {"Navegação": NAV, "Controle": CTRL}
BRL = {"type": "number-currency", "currencyCode": "BRL", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 0}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}


def w(nome, ds, campos, spec, p, disag=False):
    return {"widget": {"name": nome, "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": campos, "disaggregated": disag}}], "spec": spec}, "position": p}


def contador(nome, titulo, col, p, fmt, desc=None, cond="linha = 'Onda 1' AND janela = 90"):
    return w(nome, "resumo", [campo("v", f"SUM(CASE WHEN {cond} THEN `{col}` END)")],
             {"version": 2, "widgetType": "counter", "frame": frame(titulo, desc),
              "encodings": {"value": {"fieldName": "v", "rowNumber": 0, "format": fmt}}}, p)


def linha_graf(nome, titulo, desc, ds, x, y, cor, mapa, p, fmt_y, x_tit, y_tit):
    return w(nome, ds, [campo(x), campo("y", f"SUM(`{y}`)"), campo(cor)],
             {"version": 3, "widgetType": "line", "frame": frame(titulo, desc),
              "encodings": {"x": {"fieldName": x, "scale": {"type": "quantitative"}, "axis": {"title": x_tit}},
                            "y": {"fieldName": "y", "scale": {"type": "quantitative"}, "format": fmt_y, "axis": {"title": y_tit}},
                            "color": cores(cor, mapa)}}, p)


def layout():
    L = []
    L.append({"widget": {"name": "aviso", "multilineTextboxSpec": {"lines": [
        "**⚠️ DADOS SIMULADOS** — demonstração do formato do painel de uplift. Nenhum número é resultado real. "
        "Onda 1 (Reumatologia + Tireoide) · janela de 90 dias · ~12 meses com 10% de controle."]}}, "position": pos(0, 0, 12, 1)})
    L.append(contador("k_uplift", "Consumo a mais por paciente", "uplift", pos(0, 1, 3, 3), BRL,
                      "Navegação − controle, por paciente sorteado, em 90 dias"))
    L.append(contador("k_pct", "Ganho sobre o controle", "uplift_pct", pos(3, 1, 3, 3), PCT,
                      "Quanto a navegação consome a mais, em %"))
    L.append(contador("k_conv", "Conversão a mais", "conv_dif", pos(6, 1, 3, 3), PCT,
                      "Diferença no % de pacientes que voltam a consumir"))
    L.append(contador("k_prob", "Probabilidade de ganho", "prob_ganho", pos(9, 1, 3, 3), PCT,
                      "Chance de o ganho ser maior que zero"))
    # Boxplot + quartis
    L.append(w("g_box", "box", [campo("grupo"), campo("consumo")],
               {"version": 3, "widgetType": "box", "frame": frame("Consumo de quem voltou — navegação × controle",
                                                                 "Caixa = 50% centrais · linha = mediana · pontos = valores fora da caixa"),
                "encodings": {"x": {"fieldName": "grupo", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                              "y": {"fieldName": "consumo", "scale": {"type": "quantitative"}, "format": BRL,
                                    "axis": {"title": "Consumo em 90 dias"}},
                              "color": cores("grupo", GRUPO)}}, pos(0, 4, 7, 7), disag=True))
    L.append(w("t_quartis", "quartis", [campo(c) for c in ("linha", "grupo", "consumiram", "q1", "mediana", "media", "q3", "desvio")],
               {"version": 2, "widgetType": "table", "frame": frame("Resumo estatístico — quem voltou em 90 dias"),
                "encodings": {"columns": [{"fieldName": c} for c in ("linha", "grupo", "consumiram", "q1", "mediana", "media", "q3", "desvio")]}},
               pos(7, 4, 5, 7), disag=True))
    # Distribuições sobrepostas + conversão
    L.append(linha_graf("g_dens", "Distribuições sobrepostas — a navegação se desloca para a direita",
                        "% de cada grupo por faixa de consumo (R$ 100), curva suavizada", "densidade",
                        "consumo_rs", "pct_do_grupo", "grupo", GRUPO, pos(0, 11, 6, 7), PCT, "Consumo em 90 dias (R$)", "% do grupo"))
    L.append(linha_graf("g_conv", "Mais pacientes voltam — e voltam antes",
                        "% do grupo que já teve algum consumo, por dia após o sorteio", "conversao",
                        "dia", "pct_convertidos", "grupo", GRUPO, pos(6, 11, 6, 7), PCT, "Dias após o sorteio", "% que já consumiu"))
    # Linha do tempo + barras de conversão
    L.append(linha_graf("g_semana", "O ganho se abre cedo e cresce semana a semana",
                        "Consumo acumulado por paciente sorteado (zero para quem não consumiu); verde = diferença", "semana",
                        "semana", "consumo", "serie", {"Navegação": NAV, "Controle": CTRL, "Uplift (diferença)": GAIN},
                        pos(0, 18, 8, 7), BRL, "Semanas desde o sorteio", "Consumo acumulado por paciente"))
    L.append(w("g_convbar", "conv_bar", [campo("grupo"), campo("y", "SUM(`pct_convertidos`)")],
               {"version": 3, "widgetType": "bar", "frame": frame("% que voltou em 90 dias"),
                "encodings": {"x": {"fieldName": "grupo", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                              "y": {"fieldName": "y", "scale": {"type": "quantitative"}, "format": PCT, "axis": {"title": ""}},
                              "color": cores("grupo", GRUPO), "label": {"show": True}}}, pos(8, 18, 4, 7)))
    # Uplift por linha
    L.append(w("g_linha", "resumo", [campo("linha"), campo("y", "SUM(CASE WHEN janela = 90 THEN `uplift` END)")],
               {"version": 3, "widgetType": "bar",
                "frame": frame("Consumo a mais por paciente — onda e linhas", "A decisão é pela onda; por linha é descritivo"),
                "encodings": {"x": {"fieldName": "y", "scale": {"type": "quantitative"}, "format": BRL, "axis": {"title": ""}},
                              "y": {"fieldName": "linha", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                              "label": {"show": True}}}, pos(0, 25, 5, 6)))
    L.append(w("t_linha", "resumo", [campo(c) for c in ("linha", "janela", "uplift", "ic_inf", "ic_sup", "prob_ganho", "leitura")],
               {"version": 2, "widgetType": "table", "frame": frame("Uplift com intervalo de 95% — por linha e janela"),
                "encodings": {"columns": [{"fieldName": c} for c in ("linha", "janela", "uplift", "ic_inf", "ic_sup", "prob_ganho", "leitura")]}},
               pos(5, 25, 7, 6), disag=True))
    return L


def main():
    datasets = [{"name": k, "displayName": NOMES[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]}
                for k, v in SQL.items()]
    painel = {"datasets": datasets, "pages": [{"name": "uplift_demo", "displayName": "Uplift — demonstração", "layout": layout()}]}
    out = AQUI / "painel-uplift-v2.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(datasets), "datasets", len(painel["pages"][0]["layout"]), "widgets")


if __name__ == "__main__":
    main()
