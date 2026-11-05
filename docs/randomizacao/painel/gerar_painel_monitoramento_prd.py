"""Painel de MONITORAMENTO da randomização em PRD — garantia diária da segregação.

Uso: python gerar_painel_monitoramento_prd.py  ->  painel-monitoramento-randomizacao-prd-v1.lvdash.json
Linhas: Tireoide e Reumatologia (segregação desde 30/10/2026), DII e Ateromatose (entram na semana de 03/11).
Ao ligar uma linha, preencher a data em SEGREGACAO abaixo (ou no dataset `segregacao` do painel) e regerar.

Medida central: exames ENVIADOS (log do OneDrive) que NÃO pertencem ao grupo intervenção do mesmo dia.
Depois da segregação, tem de ser zero. Não usa o `fl_enviado` do log de sorteio, que é a decisão, não o envio.
"""
import json
from pathlib import Path

AQUI = Path(__file__).parent
SEGREGACAO = {  # linha -> data em que o envio passou a sair só com a intervenção (None = ainda não ligada)
    "tirads": "2026-10-30",
    "reumatologia": "2026-10-30",
    "doenca_inflamatoria_intestinal": "2026-11-05",
    "ateromatose_coronariana": "2026-11-05",
}
ROTULO = {"tirads": "Tireoide", "reumatologia": "Reumatologia", "doenca_inflamatoria_intestinal": "DII",
          "ateromatose_coronariana": "Ateromatose"}
OP = "mlops_fabrica_ia.operacao"
CASE = "CASE esp " + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in ROTULO.items()) + " END"
SEG = "seg AS (SELECT * FROM VALUES " + ", ".join(
    f"('{k}', {'DATE' + repr(v) if v else 'CAST(NULL AS DATE)'})" for k, v in SEGREGACAO.items()) + " AS t(esp, dt_segregacao))"
LINHAS = ", ".join(f"'{k}'" for k in SEGREGACAO)

BASE = f"""WITH {SEG},
rel AS (SELECT nm_especialidade AS esp, to_date(dt_execucao_modelo) AS dia, max_by(qt_exportados, dh_registro) AS exportados,
          max_by(qt_controle, dh_registro) AS controle, max_by(qt_intervencao, dh_registro) AS intervencao,
          max_by(qt_sem_chave, dh_registro) AS sem_chave, max(CASE WHEN st_execucao = 'SUCESSO' THEN 1 ELSE 0 END) AS teve_sucesso,
          max_by(st_execucao, dh_registro) AS ultimo_status
        FROM {OP}.tb_mov_randomizacao_relatorio WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({LINHAS})
          AND to_date(dt_execucao_modelo) >= DATE'2026-10-12' GROUP BY 1, 2),
env AS (SELECT specialty AS esp, dt_execucao_modelo AS dia, exame_an FROM {OP}.tb_mov_onedrive_saida
        WHERE environment = 'prd' AND specialty IN ({LINHAS}) GROUP BY 1, 2, 3),
itv AS (SELECT nm_especialidade AS esp, to_date(dt_execucao_modelo) AS dia, exame_an FROM {OP}.tb_mov_randomizacao_intervencao
        WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({LINHAS}) GROUP BY 1, 2, 3),
cruz AS (SELECT coalesce(e.esp, i.esp) AS esp, coalesce(e.dia, i.dia) AS dia, count(DISTINCT e.exame_an) AS enviados,
           count(DISTINCT CASE WHEN e.exame_an IS NOT NULL AND i.exame_an IS NOT NULL THEN e.exame_an END) AS enviados_da_intervencao,
           count(DISTINCT i.exame_an) AS exames_intervencao
         FROM env e FULL OUTER JOIN itv i ON i.esp = e.esp AND i.dia = e.dia AND i.exame_an = e.exame_an GROUP BY 1, 2),
d AS (SELECT r.esp, r.dia, s.dt_segregacao,
        CASE WHEN s.dt_segregacao IS NULL THEN 'aguardando' WHEN r.dia >= s.dt_segregacao THEN 'segregado' ELSE 'antes' END AS fase,
        CASE WHEN r.teve_sucesso = 1 THEN 'SUCESSO' ELSE coalesce(r.ultimo_status, '—') END AS execucao,
        coalesce(r.exportados, 0) AS exportados, coalesce(r.controle, 0) AS controle, coalesce(r.intervencao, 0) AS intervencao,
        coalesce(c.enviados, 0) AS enviados, coalesce(c.enviados - c.enviados_da_intervencao, 0) AS enviados_fora_intervencao,
        coalesce(c.exames_intervencao - c.enviados_da_intervencao, 0) AS intervencao_nao_enviada, coalesce(r.sem_chave, 0) AS sem_chave
      FROM rel r JOIN seg s ON s.esp = r.esp LEFT JOIN cruz c ON c.esp = r.esp AND c.dia = r.dia)"""

SQL = {
    "diario": BASE + f"""
SELECT {CASE} AS linha, date_format(dia, 'yyyy-MM-dd') AS dia, fase, execucao, exportados, controle, intervencao, enviados,
  enviados_fora_intervencao, intervencao_nao_enviada, sem_chave,
  CASE WHEN fase = 'segregado' AND enviados_fora_intervencao > 0 THEN '🔴 envio fora da intervenção'
       WHEN fase = 'segregado' AND intervencao_nao_enviada > 0 THEN '🟡 intervenção não enviada'
       WHEN execucao <> 'SUCESSO' THEN '🟡 execução sem sucesso'
       ELSE '🟢 ok' END AS alerta
FROM d ORDER BY dia DESC, linha""",
    "resumo": BASE + f""",
ult AS (SELECT esp, max(CASE WHEN execucao = 'SUCESSO' THEN dia END) AS ultimo_sucesso, max(dia) AS ultimo_dia FROM d GROUP BY 1)
SELECT {CASE.replace('CASE esp', 'CASE s.esp')} AS linha,
  CASE WHEN s.dt_segregacao IS NULL THEN '⏳ aguardando' ELSE concat('✅ desde ', date_format(s.dt_segregacao, 'dd/MM')) END AS segregacao,
  sum(CASE WHEN d.fase = 'segregado' THEN d.controle + d.intervencao ELSE 0 END) AS sorteados,
  sum(CASE WHEN d.fase = 'segregado' THEN d.controle ELSE 0 END) AS controle,
  round(100 * sum(CASE WHEN d.fase = 'segregado' THEN d.controle ELSE 0 END) / nullif(sum(CASE WHEN d.fase = 'segregado' THEN d.controle + d.intervencao ELSE 0 END), 0), 1) AS pct_controle,
  sum(CASE WHEN d.fase = 'segregado' THEN d.enviados_fora_intervencao ELSE 0 END) AS enviados_fora_intervencao,
  sum(CASE WHEN d.fase = 'segregado' THEN d.intervencao_nao_enviada ELSE 0 END) AS intervencao_nao_enviada,
  date_format(u.ultimo_sucesso, 'dd/MM') AS ultima_execucao_ok,
  CASE WHEN s.dt_segregacao IS NULL THEN '⏳ ligar a segregação'
       WHEN sum(CASE WHEN d.fase = 'segregado' THEN d.enviados_fora_intervencao ELSE 0 END) > 0 THEN '🔴 controle pode ter sido enviado'
       WHEN u.ultimo_dia > u.ultimo_sucesso THEN '🟡 última execução sem sucesso'
       ELSE '🟢 ok' END AS situacao
FROM seg s JOIN d ON d.esp = s.esp JOIN ult u ON u.esp = s.esp
GROUP BY s.esp, s.dt_segregacao, u.ultimo_sucesso, u.ultimo_dia
ORDER BY s.dt_segregacao NULLS LAST, linha""",
    "garantias": f"""WITH {SEG},
l AS (SELECT g.* FROM {OP}.tb_mov_randomizacao_log g JOIN seg s ON s.esp = g.nm_especialidade
      WHERE g.tp_ambiente = 'prd' AND s.dt_segregacao IS NOT NULL AND to_date(g.dt_execucao_modelo) >= s.dt_segregacao)
SELECT
  (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf HAVING count(DISTINCT tp_grupo) > 1)) AS trocou_de_grupo,
  (SELECT count(*) FROM l LEFT JOIN {OP}.tb_mov_randomizacao_controle b
     ON lpad(cast(l.num_cpf AS string), 11, '0') = lpad(cast(b.num_cpf AS string), 11, '0')
   WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) AS diferente_do_sorteio,
  (SELECT count(*) FROM l WHERE num_cpf IS NULL) AS sem_cpf,
  (SELECT count(*) FROM l) AS registros_verificados""",
    "pacientes": f"""WITH {SEG}
SELECT {CASE.replace('esp', 'g.nm_especialidade')} AS linha, date_format(to_date(g.dt_execucao_modelo), 'yyyy-MM-dd') AS dia,
  CASE WHEN g.tp_grupo = 'controle' THEN 'Grupo controle' ELSE 'Intervenção (navegação)' END AS grupo,
  count(DISTINCT g.num_cpf) AS pacientes
FROM {OP}.tb_mov_randomizacao_log g JOIN seg s ON s.esp = g.nm_especialidade
WHERE g.tp_ambiente = 'prd' AND to_date(g.dt_execucao_modelo) >= DATE'2026-10-26'
GROUP BY 1, 2, 3""",
}
DESCR = {"diario": "Uma linha por linha de cuidado e dia de execução", "resumo": "Situação por linha desde a segregação",
         "garantias": "Garantias do sorteio desde a segregação", "pacientes": "Pacientes sorteados por dia e grupo (desde 26/10)"}


def txt(name, lines, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}}, "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(name, ds, expr, title, desc, x, y, w=3, h=3):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [{"name": "v", "expression": expr}], "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "counter", "frame": {"showTitle": True, "title": title, "showDescription": True, "description": desc},
                                "encodings": {"value": {"fieldName": "v", "rowNumber": 0}}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def table(name, ds, cols, title, desc, x, y, w, h):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [{"name": c, "expression": f"`{c}`"} for c in cols], "disaggregated": True}}],
                       "spec": {"version": 2, "widgetType": "table", "frame": {"showTitle": True, "title": title, "showDescription": True, "description": desc},
                                "encodings": {"columns": [{"fieldName": c} for c in cols]}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def barras(name, x, y, w, h):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": "pacientes", "fields": [
                {"name": "dia", "expression": "`dia`"}, {"name": "n", "expression": "SUM(`pacientes`)"}, {"name": "grupo", "expression": "`grupo`"}], "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "bar", "frame": {"showTitle": True, "title": "Pacientes sorteados por dia (as quatro linhas)", "showDescription": True,
                                                                  "description": "Desde 26/10 · o controle é a fatia laranja"},
                     "encodings": {"x": {"fieldName": "dia", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                                   "y": {"fieldName": "n", "scale": {"type": "quantitative"}, "axis": {"title": "pacientes"}},
                                   "color": {"fieldName": "grupo", "displayName": "Grupo", "scale": {"type": "categorical", "mappings": [
                                       {"value": "Intervenção (navegação)", "color": "#0B7A80"}, {"value": "Grupo controle", "color": "#C9811A"}]}},
                                   "label": {"show": True}}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def main():
    datasets = [{"name": k, "displayName": DESCR[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]} for k, v in SQL.items()]
    layout = [
        txt("titulo", ["# Randomização em PRD — monitoramento da segregação\n", "\n",
                       "Tireoide e Reumatologia segregam desde **30/10/2026** · DII e Ateromatose entram na semana de 03/11 · atualização a cada execução"], 0, 0, 12, 2),
        counter("k_fora", "resumo", "SUM(`enviados_fora_intervencao`)", "Envios fora da intervenção",
                "Exames enviados que não são do grupo intervenção, depois da segregação. Tem de ser 0", 0, 2),
        counter("k_nao_env", "resumo", "SUM(`intervencao_nao_enviada`)", "Intervenção não enviada",
                "Exames da intervenção que não aparecem no envio (falha de envio)", 3, 2),
        counter("k_ctrl", "resumo", "SUM(`controle`)", "Pacientes no controle", "Desde a segregação, somando as linhas segregadas", 6, 2),
        counter("k_troca", "garantias", "SUM(`trocou_de_grupo` + `diferente_do_sorteio`)", "Inconsistências do sorteio",
                "Paciente que trocou de grupo ou diverge da base de controle. Tem de ser 0", 9, 2),
        table("t_resumo", "resumo", ["linha", "segregacao", "situacao", "sorteados", "controle", "pct_controle", "enviados_fora_intervencao",
                                     "intervencao_nao_enviada", "ultima_execucao_ok"], "Situação por linha",
              "Contagens desde a data de segregação de cada linha · pct_controle esperado perto de 5%", 0, 5, 12, 4),
        barras("g_dia", 0, 9, 12, 5),
        table("t_diario", "diario", ["linha", "dia", "fase", "execucao", "alerta", "exportados", "controle", "intervencao", "enviados",
                                     "enviados_fora_intervencao", "intervencao_nao_enviada", "sem_chave"], "Dia a dia",
              "fase: aguardando = segregação ainda não ligada · antes = sorteio sem segregação · segregado = envio só da intervenção", 0, 14, 12, 8),
        txt("leitura", ["**Como ler**\n", "\n",
                        "- **Envios fora da intervenção** cruza o log real do envio (OneDrive) com o grupo intervenção do mesmo dia. Depois da segregação, qualquer valor acima de zero indica que paciente do controle pode ter sido navegado.\n",
                        "- **Intervenção não enviada** aponta falha de envio: o paciente foi sorteado para a navegação e o arquivo não o levou.\n",
                        "- O log de envio existe a partir de 26/10; antes disso a coluna enviados fica em zero.\n",
                        "- Dia com execução sem sucesso não tem exportação nem envio (ex.: gold sem carga).\n",
                        "- **Ao ligar DII ou Ateromatose:** preencher a data de segregação no dataset `resumo`/`diario`/`garantias` (bloco `seg`)."],
            0, 22, 12, 4),
    ]
    painel = {"datasets": datasets, "pages": [{"name": "monitoramento", "displayName": "Monitoramento PRD", "layout": layout}]}
    out = AQUI / "painel-monitoramento-randomizacao-prd-v1.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(datasets), "datasets |", len(layout), "widgets")


if __name__ == "__main__":
    main()
