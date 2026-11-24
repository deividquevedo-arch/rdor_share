"""Painel "Visão por linha" (pedido do head da Fábrica) — v1, rascunho AI/BI.

Uso: python gerar_painel_visao_por_linha.py  ->  painel-visao-por-linha-v1.lvdash.json

Página 1 "Laudos por hospital": laudos que chegaram ao motor em PRD nos últimos 30 dias, por linha e hospital navegado
           (ids de cadastro agrupados por hospital, como na navegação). Filtro de linha (seleção única).
Página 2 "Randomização": % e nº no grupo controle por linha (mesma consulta do painel de monitoramento PRD v4).
Página 3 "Pendências": o que ainda não tem dado (lista oficial de hospitais, consumo, uplift).

A lista de hospitais vem dos comentários da navegação de PRD do Ca de Cólon (a mesma nas linhas de captação).
Só agrega: nenhum dado de paciente.
"""
import json
import re
import sys
from pathlib import Path

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI.parent / "randomizacao" / "painel"))
import gerar_painel_monitoramento_prd as mon  # noqa: E402

NAV = AQUI.parents[1] / "_worktrees" / "motor-regras-estruturadas" / "plataform" / "config" / "exchange" / "prd" / \
    "ntb_ia_cancer_colon_navegacao.py"
LINHAS = {"ateromatose_coronariana": "Ateromatose", "cancer_colon": "Ca de Cólon", "cancer_estomago": "Ca de Estômago",
          "cancer_rim": "Ca de Rim", "doenca_inflamatoria_intestinal": "DII", "doencas_biliares": "Doenças biliares",
          "hepatologia": "Hepatologia", "neuroimunologia": "Neuroimunologia", "pirads": "Próstata (PI-RADS)",
          "reumatologia": "Reumatologia", "tirads": "Tireoide (TI-RADS)", "transplante_pulmao": "Transplante de pulmão",
          "tumor_osseo": "Tumor ósseo"}
JANELA = "CAST(dt_execucao_modelo AS DATE) BETWEEN date_sub(current_date(), 30) AND date_sub(current_date(), 1)"


def hospitais_navegados() -> dict[str, list[str]]:
    """Hospital ('UF — NOME') -> ids de cadastro que estão no IN da navegação (pausados ficam de fora)."""
    texto = NAV.read_text(encoding="utf-8")
    no_in = set(re.findall(r"'(\d+)'", re.search(r'"e\.id_unidade":\s*\((.*?)\),\n', texto, re.S).group(1)))
    out = {}
    for m in re.finditer(r"#\s+([A-Z]{2}) — ([^:]+):\s*([0-9 ,]+)", texto):
        ids = [i.strip() for i in m.group(3).split(",") if i.strip() in no_in]
        if ids:
            out[f"{m.group(1)} — {m.group(2).strip()}"] = ids
    assert sorted(i for v in out.values() for i in v) == sorted(no_in), "id do IN sem hospital nos comentarios"
    return out


def sql_laudos(hosp: dict[str, list[str]]) -> tuple[str, str]:
    vals = ", ".join(f"('{h[:2]}', '{h[5:].replace(chr(39), chr(92) + chr(39))}', '{i}')" for h, ids in hosp.items() for i in ids)
    rot = "CASE linha " + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in LINHAS.items()) + " END"
    uniao = "\nUNION ALL\n".join(
        f"SELECT '{l}' AS linha, CAST(id_unidade AS STRING) AS unidade, id_exame, fl_relevante, "
        f"CAST(dt_execucao_modelo AS DATE) AS dia FROM diamond_fabrica_ia.{l}.tb_mod_diamond_{l}_saida_v0 WHERE {JANELA}"
        for l in LINHAS)
    base = f"""WITH hm AS (SELECT * FROM VALUES {vals} AS t(uf, hospital, unidade)),
s AS ({uniao}),
lh AS (SELECT l.linha, h.uf, h.hospital,
         count(DISTINCT s.id_exame) AS laudos_30d,
         count(DISTINCT CASE WHEN s.fl_relevante = 1 THEN s.id_exame END) AS relevantes_30d
       FROM (SELECT * FROM VALUES {", ".join(f"('{l}')" for l in LINHAS)} AS t(linha)) l
       CROSS JOIN (SELECT DISTINCT uf, hospital FROM hm) h
       JOIN hm ON hm.hospital = h.hospital
       LEFT JOIN s ON s.linha = l.linha AND s.unidade = hm.unidade
       GROUP BY 1, 2, 3)"""
    por_hospital = base + f"""
SELECT {rot} AS linha, uf, hospital, laudos_30d, relevantes_30d,
  CASE WHEN laudos_30d = 0 THEN '🔴 sem laudo' WHEN laudos_30d <= 30 THEN '🟡 até 30 no mês' ELSE '🟢 ok' END AS situacao
FROM lh ORDER BY linha, laudos_30d"""
    por_linha = base + f""",
ult AS ({" UNION ALL ".join(f"SELECT '{l}' AS linha, max(CAST(dt_execucao_modelo AS DATE)) AS ultima FROM diamond_fabrica_ia.{l}.tb_mod_diamond_{l}_saida_v0" for l in LINHAS)})
SELECT {rot.replace("CASE linha", "CASE lh.linha")} AS linha, count(*) AS hospitais,
  sum(CASE WHEN laudos_30d = 0 THEN 1 ELSE 0 END) AS sem_laudo,
  sum(CASE WHEN laudos_30d BETWEEN 1 AND 30 THEN 1 ELSE 0 END) AS ate_30,
  percentile_approx(laudos_30d, 0.5) AS mediana_laudos,
  sum(laudos_30d) AS laudos_30d, sum(relevantes_30d) AS relevantes_30d,
  date_format(max(u.ultima), 'dd/MM/yyyy') AS ultima_execucao,
  CASE WHEN max(u.ultima) < date_sub(current_date(), 3) THEN '🔴 sem execução recente' ELSE '🟢 rodando' END AS execucao
FROM lh JOIN ult u ON u.linha = lh.linha
GROUP BY lh.linha ORDER BY laudos_30d DESC"""
    return por_hospital, por_linha


def filtro_linha(datasets: list[str], padrao: str) -> dict:
    qs = [{"name": f"f_{d}", "query": {"datasetName": d, "fields": [{"name": "linha", "expression": "`linha`"}],
                                         "disaggregated": False}} for d in datasets]
    return {"widget": {"name": "f_linha", "queries": qs,
                       "spec": {"version": 2, "widgetType": "filter-single-select",
                                "encodings": {"fields": [{"fieldName": "linha", "displayName": "Linha de cuidado",
                                                          "queryName": q["name"]} for q in qs]},
                                "selection": {"defaultSelection": {"values": {"dataType": "STRING",
                                                                              "values": [{"value": padrao}]}}},
                                "frame": {"showTitle": True, "title": "Linha de cuidado"}}},
            "position": {"x": 0, "y": 2, "width": 12, "height": 2}}


def ds(name: str, display: str, sql: str) -> dict:
    return {"name": name, "displayName": display, "queryLines": [l + "\n" for l in sql.strip().splitlines()]}


def main():
    hosp = hospitais_navegados()
    por_hospital, por_linha = sql_laudos(hosp)
    datasets = [
        ds("por_linha", "Cobertura por linha (30 dias)", por_linha),
        ds("por_hospital", "Laudos por hospital e linha (30 dias)", por_hospital),
        ds("randomizacao", "Randomização por linha (PRD)", mon.SQL["resumo"]),
    ]
    p1 = [
        mon.txt("t1", ["# Visão por linha — laudos por hospital\n", "\n",
                       f"Laudos que **chegaram ao motor** em PRD nos últimos 30 dias, nos **{len(hosp)} hospitais navegados** "
                       "(lista da navegação, **a validar com Natan e Rúbia**). É a entrada do motor depois do filtro de cada "
                       "linha, não o volume de exames do hospital. 🔴 sem laudo · 🟡 até 30 no mês · 🟢 ok"], 0, 0, 12, 2),
        mon.table("tb_linha", "por_linha", ["linha", "execucao", "ultima_execucao", "hospitais", "sem_laudo", "ate_30",
                                            "mediana_laudos", "laudos_30d", "relevantes_30d"],
                  "Cobertura por linha", "Hospitais navegados sem laudo ou com volume baixo são a oportunidade de investigação",
                  0, 4, 12, 6),
        {**filtro_linha(["por_hospital"], "Ca de Estômago"), "position": {"x": 0, "y": 10, "width": 12, "height": 2}},
        mon.table("tb_hosp", "por_hospital", ["situacao", "uf", "hospital", "laudos_30d", "relevantes_30d"],
                  "Hospitais da linha escolhida", "Ordenado do menor para o maior volume", 0, 12, 12, 10),
    ]
    p2 = [
        mon.txt("t2", ["# Randomização por linha\n", "\n",
                       "% e número de pacientes no **grupo controle** (não navegados) nas linhas segregadas em PRD, desde a "
                       "data de segregação de cada uma. Mesma consulta do painel de monitoramento PRD v4."], 0, 0, 12, 2),
        mon.table("tb_rand", "randomizacao", ["linha", "segregacao", "sorteados", "controle", "pct_controle",
                                              "enviados_fora_intervencao", "situacao"],
                  "Grupo controle por linha", "enviados_fora_intervencao tem de ser 0", 0, 2, 12, 6),
    ]
    p3 = [mon.txt("t3", [
        "# O que ainda não está neste painel\n", "\n",
        "| item | situação |\n", "|---|---|\n",
        "| Hospitais com dados/laudos | lista atual = navegação das linhas de captação; **oficial a definir por Natan e Rúbia** |\n",
        "| Pacientes captados por unidade | no BI da Central |\n",
        "| Consumo por linha | no BI; base nova `gold_fabrica_ia.finops.tb_mov_consumo` **aguardando acesso** |\n",
        "| Uplift / margem por linha | depende do consumo (base nova) e de a janela de desfecho do controle fechar |\n",
        "| Hepatologia | sem execução em PRD desde 14/10/2026 — a confirmar se é intencional |\n"], 0, 0, 12, 8)]
    d = {"datasets": datasets, "pages": [
        {"name": "laudos", "displayName": "Laudos por hospital", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p1},
        {"name": "randomizacao", "displayName": "Randomização", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p2},
        {"name": "pendencias", "displayName": "Pendências", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p3}]}
    out = AQUI / "painel-visao-por-linha-v1.lvdash.json"
    out.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(hosp), "hospitais |", len(datasets), "datasets |", [len(p["layout"]) for p in d["pages"]])


if __name__ == "__main__":
    main()
