"""Painel de MONITORAMENTO da randomização em PRD — v4.

Uso: python gerar_painel_monitoramento_prd_v4.py  ->  painel-monitoramento-randomizacao-prd-v4.lvdash.json

Página 1 "Visão geral": a v3, todas as linhas (Doenças biliares e Neuroimunologia entram como 'aguardando').
Página 2 "Por linha de cuidado": filtro de seleção ÚNICA; mostra só a linha escolhida e o acompanhamento diário.
As linhas e as datas de segregação vivem em SEGREGACAO (gerar_painel_monitoramento_prd.py).
"""
import json
from pathlib import Path

import gerar_painel_monitoramento_prd as v1
import gerar_painel_monitoramento_prd_v3 as v3

AQUI = Path(__file__).parent
TITULO = ["# Randomização em PRD — monitoramento (v4)\n", "\n",
          "Quantos casos relevantes saíram para a navegação e quantos ficaram **retidos no grupo controle** · Tireoide e "
          "Reumatologia desde **30/10/2026** · DII e Ateromatose desde **05/11/2026** · Doenças biliares e Neuroimunologia após "
          "o deploy do PR 7874 · **Página 2: uma linha por vez**"]

# Por dia e por linha: antes da segregação todo relevante era enviado (retidos = 0); depois, controle fica retido.
LINHA_DIA = v1.BASE + f"""
SELECT {v1.CASE} AS linha, dia, 'Enviados à navegação' AS destino,
  CASE WHEN fase = 'segregado' THEN intervencao + sem_chave ELSE exportados END AS n
FROM d
UNION ALL
SELECT {v1.CASE} AS linha, dia, 'Retidos (controle)' AS destino,
  CASE WHEN fase = 'segregado' THEN controle ELSE 0 END AS n
FROM d"""

FILTRADOS = ["fluxo", "linha_dia", "diario", "camadas"]


def filtro_linha(padrao):
    qs = [{"name": f"f_{ds}", "query": {"datasetName": ds, "fields": [{"name": "linha", "expression": "`linha`"}], "disaggregated": False}}
          for ds in FILTRADOS]
    return {"widget": {"name": "f_linha", "queries": qs,
                       "spec": {"version": 2, "widgetType": "filter-single-select",
                                "encodings": {"fields": [{"fieldName": "linha", "displayName": "Linha de cuidado", "queryName": q["name"]} for q in qs]},
                                "selection": {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": padrao}]}}},
                                "frame": {"showTitle": True, "title": "Linha de cuidado"}}},
            "position": {"x": 0, "y": 0, "width": 12, "height": 2}}


def barras_dia(x, y, w, h):
    return {"widget": {"name": "g_linha_dia", "queries": [{"name": "main_query", "query": {"datasetName": "linha_dia", "fields": [
                {"name": "daily(dia)", "expression": 'DATE_TRUNC("DAY", `dia`)'}, {"name": "sum(n)", "expression": "SUM(`n`)"},
                {"name": "destino", "expression": "`destino`"}], "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "bar",
                     "frame": {"showTitle": True, "title": "Acompanhamento diário",
                               "showDescription": True, "description": "Registros relevantes por dia · antes da segregação todos eram enviados"},
                     "encodings": {"x": {"fieldName": "daily(dia)", "scale": {"type": "temporal"}, "displayName": "Dia"},
                                   "y": {"fieldName": "sum(n)", "scale": {"type": "quantitative"}, "displayName": "Registros"},
                                   "color": {"fieldName": "destino", "scale": {"type": "categorical", "mappings": [
                                       {"value": "Enviados à navegação", "color": "#0B7A80"}, {"value": "Retidos (controle)", "color": "#C9811A"}]},
                                       "displayName": "Destino"},
                                   "label": {"show": True}}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def pagina_linha():
    return [
        filtro_linha("Tireoide"),
        v1.counter("p2_rel", "fluxo", "SUM(`relevantes`)", "Relevantes", "Desde a segregação da linha", 0, 2),
        v1.counter("p2_env", "fluxo", "SUM(`enviados_navegacao`)", "Enviados à navegação", "Desde a segregação da linha", 3, 2),
        v1.counter("p2_ret", "fluxo", "SUM(`retidos_controle`)", "Retidos (controle)", "Desde a segregação · esperado ~5%", 6, 2),
        v1.counter("p2_od", "fluxo", "SUM(`enviados_onedrive`)", "Envio confirmado (OneDrive)", "Deve igualar os enviados", 9, 2),
        barras_dia(0, 5, 12, 6),
        v1.table("p2_camadas", "camadas", ["linha", "segregacao", "situacao", "c1_controle_na_intervencao", "c2_envio_fora_intervencao",
                                          "c3_controle_com_caso"], "Garantias da linha", "Os três números têm de ser 0 depois da segregação", 0, 11, 12, 3),
        v1.table("p2_diario", "diario", ["dia", "fase", "execucao", "alerta", "exportados", "controle", "intervencao", "enviados",
                                         "enviados_fora_intervencao", "intervencao_nao_enviada"], "Dia a dia da linha",
                 "fase: antes = sorteio sem segregação · segregado = envio só da intervenção", 0, 14, 12, 7),
    ]


def main():
    v3.main()
    d = json.loads((AQUI / "painel-monitoramento-randomizacao-prd-v3.lvdash.json").read_text(encoding="utf-8"))
    for w in d["pages"][0]["layout"]:
        if w["widget"]["name"] == "titulo":
            w["widget"]["multilineTextboxSpec"]["lines"] = TITULO
    d["pages"][0].update({"name": "monitoramento", "displayName": "Visão geral", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1"})
    d["datasets"].append({"name": "linha_dia", "displayName": "Enviados e retidos por dia e linha",
                          "queryLines": [l + "\n" for l in LINHA_DIA.strip().splitlines()]})
    d["pages"].append({"name": "por_linha", "displayName": "Por linha de cuidado", "pageType": "PAGE_TYPE_CANVAS",
                       "layoutVersion": "GRID_V1", "layout": pagina_linha()})
    out = AQUI / "painel-monitoramento-randomizacao-prd-v4.lvdash.json"
    out.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(d["datasets"]), "datasets |", [len(p["layout"]) for p in d["pages"]], "widgets por página")


if __name__ == "__main__":
    main()
