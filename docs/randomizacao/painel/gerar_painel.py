"""Gera o painel AI/BI da primeira linha de atenção da randomização.

Uso: python gerar_painel.py  ->  escreve painel-randomizacao.lvdash.json ao lado deste arquivo.
As consultas são as de ../relatorio-sql, sem parâmetros: o período e a linha são filtros de campo.
"""
import json
from pathlib import Path

OP = "mlops_fabrica_ia.operacao"
LINHAS = [
    "ateromatose_coronariana", "cancer_colon", "cancer_estomago", "cancer_rim",
    "doenca_inflamatoria_intestinal", "doencas_biliares", "hepatologia", "neuroimunologia",
    "pirads", "transplante_pulmao", "tumor_osseo", "reumatologia", "tirads",
]

SQL = {}

SQL["atencao"] = f"""
WITH log AS (SELECT * FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'),
ativas AS (SELECT nm_especialidade AS linha, min(to_date(dt_execucao_modelo)) AS ini FROM log GROUP BY 1),
dias AS (
  SELECT linha, dia FROM (SELECT linha, explode(sequence(ini, current_date())) AS dia FROM ativas)
  WHERE dayofweek(dia) BETWEEN 2 AND 6
),
rel AS (
  SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia,
         max_by(st_execucao, dh_registro) AS st,
         max_by(coalesce(ds_etapas_falha, ''), dh_registro) AS falhas,
         max_by(qt_exportados, dh_registro) AS exportados,
         max_by(qt_controle, dh_registro) AS controle,
         max_by(qt_intervencao, dh_registro) AS navegacao,
         max_by(qt_sem_chave, dh_registro) AS sem_cpf
  FROM {OP}.tb_mov_randomizacao_relatorio WHERE tp_ambiente = 'prd' GROUP BY 1, 2
),
lg AS (
  SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia, count(*) AS registros,
         sum(CASE WHEN tp_grupo = 'controle' AND fl_enviado THEN 1 ELSE 0 END) AS controle_enviado
  FROM log GROUP BY 1, 2
),
base AS (
  SELECT d.linha, d.dia, r.st, r.falhas, r.exportados, r.controle, r.navegacao, r.sem_cpf,
         l.registros, coalesce(l.controle_enviado, 0) AS controle_enviado,
         CASE WHEN r.linha IS NULL THEN 1 ELSE 0 END AS chk_sem_execucao,
         CASE WHEN r.st <> 'SUCESSO' OR r.falhas <> '' THEN 1 ELSE 0 END AS chk_falha,
         CASE WHEN r.exportados <> coalesce(r.controle,0) + coalesce(r.navegacao,0) + coalesce(r.sem_cpf,0) THEN 1 ELSE 0 END AS chk_conservacao,
         CASE WHEN coalesce(l.registros,0) <> coalesce(r.controle,0) + coalesce(r.navegacao,0) THEN 1 ELSE 0 END AS chk_log_x_relatorio,
         CASE WHEN coalesce(l.controle_enviado,0) > 0 THEN 1 ELSE 0 END AS chk_controle_enviado
  FROM dias d LEFT JOIN rel r ON r.linha = d.linha AND r.dia = d.dia
              LEFT JOIN lg l ON l.linha = d.linha AND l.dia = d.dia
)
SELECT *,
  CASE WHEN chk_sem_execucao = 1 AND dia = current_date() THEN 'PENDENTE'
       WHEN chk_sem_execucao + chk_falha + chk_conservacao + chk_log_x_relatorio + chk_controle_enviado > 0 THEN 'ALERTA'
       ELSE 'OK' END AS status,
  CASE WHEN NOT (chk_sem_execucao = 1 AND dia = current_date())
        AND chk_sem_execucao + chk_falha + chk_conservacao + chk_log_x_relatorio + chk_controle_enviado > 0
       THEN 1 ELSE 0 END AS fl_alerta
FROM base
"""

SQL["proporcao"] = f"""
WITH b AS (SELECT avg(CASE WHEN tp_grupo = 'controle' THEN 1.0 ELSE 0.0 END) AS p FROM {OP}.tb_mov_randomizacao_controle),
pac AS (
  SELECT nm_especialidade AS linha, num_cpf, max(tp_grupo) AS grupo
  FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' GROUP BY 1, 2
),
agg AS (
  SELECT linha, count(*) AS n, sum(CASE WHEN grupo = 'controle' THEN 1 ELSE 0 END) AS k FROM pac GROUP BY linha
  UNION ALL
  SELECT 'TOTAL', count(*), sum(CASE WHEN grupo = 'controle' THEN 1 ELSE 0 END) FROM pac
)
SELECT a.linha, a.n AS pacientes, a.k AS pacientes_controle,
       round(a.k / a.n, 4) AS fracao_observada, round(b.p, 4) AS fracao_base,
       round((a.k - a.n * b.p) / sqrt(a.n * b.p * (1 - b.p)), 2) AS z,
       CASE WHEN abs((a.k - a.n * b.p) / sqrt(a.n * b.p * (1 - b.p))) > 3 THEN 'ALERTA' ELSE 'OK' END AS status
FROM agg a CROSS JOIN b
"""

SQL["integridade"] = f"""
WITH l AS (SELECT * FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd')
SELECT
  (SELECT count(*) FROM l) AS registros_total,
  (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf HAVING count(DISTINCT tp_grupo) > 1)) AS cpf_com_dois_grupos,
  (SELECT count(*) FROM l WHERE tp_grupo = 'controle' AND fl_enviado) AS controle_enviado,
  (SELECT count(*) FROM l LEFT JOIN {OP}.tb_mov_randomizacao_controle b
     ON lpad(cast(l.num_cpf AS string), 11, '0') = lpad(cast(b.num_cpf AS string), 11, '0')
   WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) AS divergentes_da_base
"""

SQL["grupos"] = f"""
SELECT nm_especialidade AS linha, tp_grupo AS grupo, count(*) AS exames, count(DISTINCT num_cpf) AS pacientes,
       sum(CASE WHEN fl_enviado THEN 1 ELSE 0 END) AS exames_enviados,
       min(to_date(dt_execucao_modelo)) AS primeiro_dia, max(to_date(dt_execucao_modelo)) AS ultimo_dia
FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'
GROUP BY 1, 2
"""

SQL["funil"] = f"""
SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia,
       max_by(qt_entrada, dh_registro) AS exames_entrada,
       max_by(qt_saida, dh_registro) AS exames_processados,
       max_by(qt_relevantes, dh_registro) AS exames_relevantes,
       max_by(qt_exportados, dh_registro) AS exames_elegiveis_envio,
       max_by(qt_controle, dh_registro) AS exames_controle,
       max_by(qt_intervencao, dh_registro) AS exames_navegacao,
       max_by(qt_sem_chave, dh_registro) AS exames_sem_cpf
FROM {OP}.tb_mov_randomizacao_relatorio WHERE tp_ambiente = 'prd'
GROUP BY 1, 2
"""

_rel = "\n  UNION ALL ".join(
    f"SELECT '{l}' AS linha, cast(id_paciente AS string) AS idp, to_date(dt_execucao_modelo) AS dia "
    f"FROM diamond_fabrica_ia.{l}.tb_mod_diamond_{l}_saida_v0 WHERE fl_relevante = 1"
    for l in LINHAS
)
SQL["contaminacao"] = f"""
WITH g AS (
  SELECT nm_especialidade AS linha, tp_grupo AS grupo, cast(id_paciente AS string) AS idp,
         min(to_date(dt_execucao_modelo)) AS dt_rand
  FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' GROUP BY 1, 2, 3
),
rel AS (
  {_rel}
)
SELECT g.linha, g.grupo, count(DISTINCT g.idp) AS pacientes_randomizados,
       count(DISTINCT CASE WHEN r.linha IS NOT NULL THEN g.idp END) AS relevantes_em_outra_linha
FROM g LEFT JOIN rel r ON r.idp = g.idp AND r.linha <> g.linha AND r.dia >= g.dt_rand
GROUP BY 1, 2
"""

_saida = "\n  UNION ALL ".join(
    f"SELECT '{l}' AS linha, cast(id_paciente AS string) AS idp, fl_relevante, to_date(dt_execucao_modelo) AS dia "
    f"FROM diamond_fabrica_ia.{l}.tb_mod_diamond_{l}_saida_v0"
    for l in LINHAS
)
# Visão geral, em PACIENTES, só das linhas randomizadas, desde o primeiro dia randomizado de cada uma.
SQL["vg_resumo"] = f"""
WITH ini AS (
  SELECT nm_especialidade AS linha, min(to_date(dt_execucao_modelo)) AS d0
  FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' GROUP BY 1
),
s AS (
  {_saida}
),
proc AS (
  SELECT s.linha, count(DISTINCT s.idp) AS analisados,
         count(DISTINCT CASE WHEN s.fl_relevante = 1 THEN s.idp END) AS relevantes
  FROM s JOIN ini i ON i.linha = s.linha AND s.dia >= i.d0 GROUP BY 1
),
g AS (
  SELECT nm_especialidade AS linha, count(DISTINCT num_cpf) AS randomizados,
         count(DISTINCT CASE WHEN tp_grupo = 'controle' THEN num_cpf END) AS controle,
         count(DISTINCT CASE WHEN tp_grupo = 'intervencao' THEN num_cpf END) AS navegacao
  FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' GROUP BY 1
)
SELECT p.linha, i.d0 AS inicio, p.analisados, p.relevantes, g.randomizados, g.controle, g.navegacao,
       round(100.0 * g.controle / g.randomizados, 1) AS pct_controle
FROM proc p JOIN g ON g.linha = p.linha JOIN ini i ON i.linha = p.linha
"""

SQL["vg_grupos"] = f"""
SELECT nm_especialidade AS linha,
       CASE WHEN tp_grupo = 'controle' THEN 'Controle' ELSE 'Navegação' END AS grupo,
       count(DISTINCT num_cpf) AS pacientes
FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'
GROUP BY 1, 2
"""

SQL["vg_dia"] = f"""
SELECT nm_especialidade AS linha, to_date(dt_execucao_modelo) AS dia,
       CASE WHEN tp_grupo = 'controle' THEN 'Controle' ELSE 'Navegação' END AS grupo,
       count(DISTINCT num_cpf) AS pacientes
FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd'
GROUP BY 1, 2, 3
"""

TITULOS = {
    "atencao": "Atenção diária", "proporcao": "Proporção acumulada", "integridade": "Integridade",
    "grupos": "Grupos por linha", "funil": "Funil por linha e dia", "contaminacao": "Contaminação",
    "vg_resumo": "Visão geral — resumo por linha", "vg_grupos": "Visão geral — grupos por linha",
    "vg_dia": "Visão geral — grupos por dia",
}
datasets = [
    {"name": k, "displayName": TITULOS[k], "queryLines": [ln + "\n" for ln in v.strip().splitlines()]}
    for k, v in SQL.items()
]


def campo(n, expr=None):
    return {"name": n, "expression": expr or f"`{n}`"}


def tabela(nome, titulo, ds, colunas, pos):
    return {
        "widget": {
            "name": nome,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": [campo(c) for c in colunas], "disaggregated": True}}],
            "spec": {"version": 2, "widgetType": "table",
                     "frame": {"showTitle": True, "title": titulo},
                     "encodings": {"columns": [{"fieldName": c} for c in colunas]}},
        },
        "position": pos,
    }


def contador(nome, titulo, ds, coluna, pos):
    f = f"sum({coluna})"
    return {
        "widget": {
            "name": nome,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": [campo(f, f"SUM(`{coluna}`)")], "disaggregated": False}}],
            "spec": {"version": 2, "widgetType": "counter",
                     "frame": {"showTitle": True, "title": titulo},
                     "encodings": {"value": {"fieldName": f, "rowNumber": 0}}},
        },
        "position": pos,
    }


def filtro(nome, titulo, tipo, campo_nome, dss, pos):
    qs = [{"name": f"filtro_{campo_nome}_{ds}", "query": {
        "datasetName": ds,
        "fields": [campo(campo_nome),
                   campo(f"{campo_nome}_associativity", "COUNT_IF(`associative_filter_predicate_group`)")],
        "disaggregated": False}} for ds in dss]
    return {
        "widget": {
            "name": nome, "queries": qs,
            "spec": {"version": 2, "widgetType": tipo, "frame": {"showTitle": True, "title": titulo},
                     "encodings": {"fields": [{"fieldName": campo_nome, "queryName": q["name"]} for q in qs]}},
        },
        "position": pos,
    }


def contador_expr(nome, titulo, ds, campo_nome, expr, pos):
    return {
        "widget": {
            "name": nome,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": [campo(campo_nome, expr)], "disaggregated": False}}],
            "spec": {"version": 2, "widgetType": "counter",
                     "frame": {"showTitle": True, "title": titulo},
                     "encodings": {"value": {"fieldName": campo_nome, "rowNumber": 0}}},
        },
        "position": pos,
    }


def barras(nome, titulo, ds, x, y_soma, cor, temporal, pos):
    fy = f"sum({y_soma})"
    return {
        "widget": {
            "name": nome,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds,
                "fields": [campo(x), campo(fy, f"SUM(`{y_soma}`)"), campo(cor)],
                "disaggregated": False}}],
            "spec": {
                "version": 3, "widgetType": "bar",
                "frame": {"showTitle": True, "title": titulo},
                "encodings": {
                    "x": {"fieldName": x, "displayName": x,
                          "scale": {"type": "temporal" if temporal else "categorical"}},
                    "y": {"fieldName": fy, "displayName": "pacientes", "scale": {"type": "quantitative"}},
                    "color": {"fieldName": cor, "displayName": cor,
                              "scale": {"type": "categorical", "mappings": [
                                  {"value": "Navegação", "color": "#0E6E74"},
                                  {"value": "Controle", "color": "#E0A030"}]}},
                    "label": {"show": True},
                },
                "data": {"queryName": "main_query"},
            },
        },
        "position": pos,
    }


def texto(nome, linhas, pos):
    return {"widget": {"name": nome, "multilineTextboxSpec": {"lines": linhas}}, "position": pos}


layout = [
    texto("cabecalho", [
        "## Randomização — primeira linha de atenção\n",
        "Produção. Checagens diárias por linha randomizada (seg–sex, após 08:00). "
        "**ALERTA** = alguma checagem obrigatória falhou · **PENDENTE** = hoje, job ainda sem registro.",
    ], {"x": 0, "y": 0, "width": 8, "height": 2}),
    filtro("f_linha", "Linha de cuidado", "filter-multi-select", "linha",
           ["atencao", "proporcao", "grupos", "funil", "contaminacao"], {"x": 8, "y": 0, "width": 4, "height": 1}),
    filtro("f_dia", "Período", "filter-date-range-picker", "dia",
           ["atencao", "funil"], {"x": 8, "y": 1, "width": 4, "height": 1}),
    contador("k_alertas", "Dias-linha em ALERTA", "atencao", "fl_alerta", {"x": 0, "y": 2, "width": 3, "height": 2}),
    contador("k_dois_grupos", "CPF com dois grupos", "integridade", "cpf_com_dois_grupos", {"x": 3, "y": 2, "width": 3, "height": 2}),
    contador("k_ctrl_enviado", "Controle enviado", "integridade", "controle_enviado", {"x": 6, "y": 2, "width": 3, "height": 2}),
    contador("k_divergentes", "Divergentes da base", "integridade", "divergentes_da_base", {"x": 9, "y": 2, "width": 3, "height": 2}),
    tabela("t_atencao", "Atenção diária — status por linha e dia útil", "atencao",
           ["linha", "dia", "status", "st", "exportados", "controle", "navegacao", "sem_cpf", "registros",
            "chk_sem_execucao", "chk_falha", "chk_conservacao", "chk_log_x_relatorio", "chk_controle_enviado"],
           {"x": 0, "y": 4, "width": 12, "height": 8}),
    tabela("t_proporcao", "Proporção de controle acumulada (por paciente) — ALERTA se |z| > 3", "proporcao",
           ["linha", "pacientes", "pacientes_controle", "fracao_observada", "fracao_base", "z", "status"],
           {"x": 0, "y": 12, "width": 6, "height": 5}),
    tabela("t_grupos", "Grupos por linha — desde o início", "grupos",
           ["linha", "grupo", "exames", "pacientes", "exames_enviados", "primeiro_dia", "ultimo_dia"],
           {"x": 6, "y": 12, "width": 6, "height": 5}),
    tabela("t_contaminacao", "Contaminação — randomizados relevantes em outra linha (informativo)", "contaminacao",
           ["linha", "grupo", "pacientes_randomizados", "relevantes_em_outra_linha"],
           {"x": 0, "y": 17, "width": 6, "height": 5}),
    tabela("t_integridade", "Integridade — totais (devem ser zero, exceto o total)", "integridade",
           ["registros_total", "cpf_com_dois_grupos", "controle_enviado", "divergentes_da_base"],
           {"x": 6, "y": 17, "width": 6, "height": 5}),
    tabela("t_funil", "Funil por linha e dia — todas as linhas em produção (informativo)", "funil",
           ["linha", "dia", "exames_entrada", "exames_processados", "exames_relevantes",
            "exames_elegiveis_envio", "exames_controle", "exames_navegacao", "exames_sem_cpf"],
           {"x": 0, "y": 22, "width": 12, "height": 9}),
]

visao_geral = [
    texto("vg_cabecalho", [
        "## Randomização — visão geral\n",
        "Pacientes das linhas randomizadas, em produção, desde o primeiro dia de randomização de cada linha. "
        "**Navegação** = enviado às enfermeiras navegadoras · **Controle** = identificado, registrado e "
        "não enviado, para comparação. A mesma pessoa conta uma vez por linha.",
    ], {"x": 0, "y": 0, "width": 9, "height": 2}),
    filtro("vg_f_linha", "Linha de cuidado", "filter-multi-select", "linha",
           ["vg_resumo", "vg_grupos", "vg_dia"], {"x": 9, "y": 0, "width": 3, "height": 1}),
    filtro("vg_f_dia", "Período (gráfico diário)", "filter-date-range-picker", "dia",
           ["vg_dia"], {"x": 9, "y": 1, "width": 3, "height": 1}),
    contador("vg_k_analisados", "Pacientes analisados pelo algoritmo", "vg_resumo", "analisados", {"x": 0, "y": 2, "width": 2, "height": 3}),
    contador("vg_k_relevantes", "Com achado relevante", "vg_resumo", "relevantes", {"x": 2, "y": 2, "width": 2, "height": 3}),
    contador("vg_k_randomizados", "Randomizados", "vg_resumo", "randomizados", {"x": 4, "y": 2, "width": 2, "height": 3}),
    contador("vg_k_navegacao", "Grupo navegação", "vg_resumo", "navegacao", {"x": 6, "y": 2, "width": 2, "height": 3}),
    contador("vg_k_controle", "Grupo controle", "vg_resumo", "controle", {"x": 8, "y": 2, "width": 2, "height": 3}),
    contador_expr("vg_k_pct", "% no controle", "vg_resumo", "pct_controle_total",
                  "ROUND(SUM(`controle`) * 100.0 / SUM(`randomizados`), 1)", {"x": 10, "y": 2, "width": 2, "height": 3}),
    barras("vg_b_linha", "Controle × navegação por linha (pacientes)", "vg_grupos", "linha", "pacientes", "grupo",
           False, {"x": 0, "y": 5, "width": 5, "height": 7}),
    barras("vg_b_dia", "Controle × navegação por dia (pacientes)", "vg_dia", "dia", "pacientes", "grupo",
           True, {"x": 5, "y": 5, "width": 7, "height": 7}),
    tabela("vg_t_resumo", "Resumo por linha", "vg_resumo",
           ["linha", "inicio", "analisados", "relevantes", "randomizados", "navegacao", "controle", "pct_controle"],
           {"x": 0, "y": 12, "width": 12, "height": 4}),
]

painel = {"datasets": datasets, "pages": [
    {"name": "visao_geral", "displayName": "Visão geral", "layout": visao_geral},
    {"name": "atencao", "displayName": "Primeira linha de atenção", "layout": layout},
]}
saida = Path(__file__).with_name("painel-randomizacao.lvdash.json")
saida.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"{saida.name}: {len(datasets)} datasets, {len(layout)} widgets")
