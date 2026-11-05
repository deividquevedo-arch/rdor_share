"""Painel de MONITORAMENTO da randomização em PRD — v3: o fluxo (relevantes → enviados × retidos) no topo.

Uso: python gerar_painel_monitoramento_prd_v3.py  ->  painel-monitoramento-randomizacao-prd-v3.lvdash.json
Reaproveita a v1 (datas de segregação e consultas) e a v2 (três camadas de garantia).

Relação verificada nos dados (04/11): nas linhas segregadas, relevantes exportados = intervenção + controle, e o
envio real do OneDrive = intervenção. Antes da segregação o envio levava a view completa (controle incluído).
Unidade: registros exportados (exames relevantes), não pacientes distintos.
"""
import json
from pathlib import Path

import gerar_painel_monitoramento_prd as v1
import gerar_painel_monitoramento_prd_v2 as v2

AQUI = Path(__file__).parent

FLUXO = v1.BASE + f""",
inicio AS (SELECT esp, min(dia) AS dt_inicio FROM d WHERE controle + intervencao > 0 GROUP BY 1)
SELECT {v1.CASE.replace('CASE esp', 'CASE s.esp')} AS linha,
  CASE WHEN s.dt_segregacao IS NULL THEN '⏳ aguardando' ELSE concat('✅ desde ', date_format(s.dt_segregacao, 'dd/MM')) END AS segregacao,
  sum(CASE WHEN d.fase = 'segregado' THEN d.exportados ELSE 0 END) AS relevantes,
  sum(CASE WHEN d.fase = 'segregado' THEN d.intervencao + d.sem_chave ELSE 0 END) AS enviados_navegacao,
  sum(CASE WHEN d.fase = 'segregado' THEN d.controle ELSE 0 END) AS retidos_controle,
  round(100 * sum(CASE WHEN d.fase = 'segregado' THEN d.controle ELSE 0 END)
        / nullif(sum(CASE WHEN d.fase = 'segregado' THEN d.exportados ELSE 0 END), 0), 1) AS pct_retido,
  sum(CASE WHEN d.fase = 'segregado' THEN d.sem_chave ELSE 0 END) AS sem_cpf_navegados,
  sum(CASE WHEN d.fase = 'segregado' THEN d.enviados ELSE 0 END) AS enviados_onedrive,
  sum(d.exportados) AS relevantes_total,
  sum(d.controle) AS controle_sorteado_total,
  sum(CASE WHEN d.fase <> 'segregado' THEN d.controle ELSE 0 END) AS controle_navegado_antes,
  date_format(min(i.dt_inicio), 'dd/MM') AS sorteio_desde
FROM seg s LEFT JOIN d ON d.esp = s.esp LEFT JOIN inicio i ON i.esp = s.esp
GROUP BY s.esp, s.dt_segregacao
ORDER BY s.dt_segregacao NULLS LAST, linha"""

FLUXO_DIA = v1.BASE + f"""
SELECT {v1.CASE} AS linha, date_format(dia, 'yyyy-MM-dd') AS dia, 'Enviados à navegação' AS destino, intervencao + sem_chave AS n
FROM d WHERE fase = 'segregado'
UNION ALL
SELECT {v1.CASE} AS linha, date_format(dia, 'yyyy-MM-dd') AS dia, 'Retidos (controle)' AS destino, controle AS n
FROM d WHERE fase = 'segregado'"""


def barras_fluxo(name, x, y, w, h):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": "fluxo_dia", "fields": [
                {"name": "dia", "expression": "`dia`"}, {"name": "n", "expression": "SUM(`n`)"}, {"name": "destino", "expression": "`destino`"}],
                "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "bar", "frame": {"showTitle": True, "title": "Por dia, desde a segregação: enviados × retidos",
                                                                  "showDescription": True, "description": "Somando as linhas segregadas · registros exportados"},
                     "encodings": {"x": {"fieldName": "dia", "scale": {"type": "categorical"}, "displayName": "Dia"},
                                   "y": {"fieldName": "n", "scale": {"type": "quantitative"}, "displayName": "Registros"},
                                   "color": {"fieldName": "destino", "scale": {"type": "categorical", "mappings": [
                                       {"value": "Enviados à navegação", "color": "#0B7A80"}, {"value": "Retidos (controle)", "color": "#C9811A"}]},
                                       "displayName": "Destino"},
                                   "label": {"show": True}}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def main():
    sql = dict(v1.SQL, camadas=v2.CAMADAS, fluxo=FLUXO, fluxo_dia=FLUXO_DIA)
    descr = dict(v1.DESCR, camadas="Três camadas de garantia por linha", fluxo="Fluxo por linha: relevantes, enviados, retidos",
                 fluxo_dia="Enviados e retidos por dia")
    datasets = [{"name": k, "displayName": descr[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]} for k, v in sql.items()]
    topo = [
        v1.txt("titulo", ["# Randomização em PRD — monitoramento (v3)\n", "\n",
                          "Quantos casos relevantes saíram para a navegação e quantos ficaram **retidos no grupo controle**, e as garantias "
                          "de que o controle não é navegado · Tireoide e Reumatologia segregam desde **30/10/2026** · DII e Ateromatose desde **05/11/2026**"],
               0, 0, 12, 2),
        v1.counter("k_rel", "fluxo", "SUM(`relevantes`)", "Relevantes", "Registros exportados pelo motor desde a segregação, linhas segregadas", 0, 2),
        v1.counter("k_env", "fluxo", "SUM(`enviados_navegacao`)", "Enviados à navegação", "Grupo intervenção (+ sem CPF, que segue para a navegação)", 3, 2),
        v1.counter("k_ret", "fluxo", "SUM(`retidos_controle`)", "Retidos (controle)", "Não enviados por sorteio — esperado ~5% dos relevantes", 6, 2),
        v1.counter("k_od", "fluxo", "SUM(`enviados_onedrive`)", "Envio confirmado (OneDrive)", "Exames no log do envio; deve igualar os enviados à navegação", 9, 2),
        v1.table("t_fluxo", "fluxo", ["linha", "segregacao", "relevantes", "enviados_navegacao", "retidos_controle", "pct_retido", "sem_cpf_navegados",
                                      "enviados_onedrive", "sorteio_desde", "relevantes_total", "controle_sorteado_total", "controle_navegado_antes"],
                 "Fluxo por linha",
                 "Colunas até enviados_onedrive: desde a segregação · *_total e controle_navegado_antes: desde o início do sorteio "
                 "(controle navegado antes da segregação fica fora da análise de uplift)", 0, 5, 12, 4),
        barras_fluxo("g_fluxo", 0, 9, 12, 5),
    ]
    resto = []
    for w in v2_layout():
        if w["widget"]["name"] == "titulo":
            continue
        w["position"]["y"] += 12
        resto.append(w)
    L = topo + resto
    painel = {"datasets": datasets, "pages": [{"name": "monitoramento", "displayName": "Monitoramento PRD", "layout": L}]}
    out = AQUI / "painel-monitoramento-randomizacao-prd-v3.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(datasets), "datasets |", len(L), "widgets")


def v2_layout():
    """Layout da v2, lido do JSON que ela gera (mantém uma fonte só para as três camadas)."""
    v2.main()
    return json.loads((AQUI / "painel-monitoramento-randomizacao-prd-v2.lvdash.json").read_text(encoding="utf-8"))["pages"][0]["layout"]


if __name__ == "__main__":
    main()
