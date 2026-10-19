"""Painel AI/BI do relatório da randomização para o negócio — v1.

Uso: python gerar_painel_negocio.py  ->  painel-negocio-v1.lvdash.json
Página única, linguagem simples: o que mudou, por que o sorteio é confiável, quando sai o resultado
e o que se pede. Dados reais de PRD (log da randomização), em PACIENTES, desde o primeiro dia.
Não altera os painéis anteriores.
"""
import json
from pathlib import Path

AQUI = Path(__file__).parent
NAV, CTRL = "#0B7A80", "#C9811A"
LOG = "tb_mov_randomizacao_log"
BASE = "tb_mov_randomizacao_controle"

# Prazo até o primeiro resultado confiável, em meses, para detectar +8 p.p. na taxa de retorno
# (58% -> 66%). Fonte: ../horizonte-uplift-por-linha-v0.md (onda 1, 1.066 sorteados/mês, CV medido).
PRAZO = [
    ("Controle de 10%", "1. Juntar pacientes", 3.0),
    ("Controle de 10%", "2. Acompanhar 90 dias", 3.0),
    ("Controle de 10%", "3. Base de consumo atualizar", 0.5),
    ("Controle de 5%", "1. Juntar pacientes", 6.0),
    ("Controle de 5%", "2. Acompanhar 90 dias", 3.0),
    ("Controle de 5%", "3. Base de consumo atualizar", 0.5),
]
ETAPA = {"1. Juntar pacientes": NAV, "2. Acompanhar 90 dias": "#7FB8BB", "3. Base de consumo atualizar": "#C9CED3"}

SQL = {
    "pacientes": f"""
SELECT CASE nm_especialidade WHEN 'tirads' THEN 'Tireoide' WHEN 'reumatologia' THEN 'Reumatologia'
            ELSE initcap(nm_especialidade) END AS linha,
       sha2(cast(num_cpf AS string), 256) AS id_paciente,
       CASE WHEN max(tp_grupo) = 'controle' THEN 'Grupo controle' ELSE 'Enviados à navegação' END AS grupo,
       CASE WHEN max(tp_grupo) = 'controle' THEN 1 ELSE 0 END AS fl_controle,
       CASE WHEN max(tp_grupo) = 'controle' THEN 0 ELSE 1 END AS fl_navegacao,
       min(to_date(dt_execucao_modelo)) AS dia
FROM {LOG} WHERE tp_ambiente = 'prd'
GROUP BY 1, 2
""",
    "garantias": f"""
WITH l AS (SELECT * FROM {LOG} WHERE tp_ambiente = 'prd')
SELECT
  (SELECT count(*) FROM l WHERE tp_grupo = 'controle' AND fl_enviado) AS controle_enviado,
  (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf HAVING count(DISTINCT tp_grupo) > 1)) AS trocou_de_grupo,
  (SELECT count(*) FROM l LEFT JOIN {BASE} b
     ON lpad(cast(l.num_cpf AS string), 11, '0') = lpad(cast(b.num_cpf AS string), 11, '0')
   WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) AS diferente_do_sorteio
""",
    "prazo": "SELECT * FROM VALUES\n" + ",\n".join(f"  ('{c}', '{e}', {m})" for c, e, m in PRAZO)
             + "\nAS t(cenario, etapa, meses)",
}
NOMES = {"pacientes": "Pacientes sorteados (um por linha)", "garantias": "Garantias do sorteio",
         "prazo": "Prazo até o resultado"}


def campo(n, e=None):
    return {"name": n, "expression": e or f"`{n}`"}


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


def contador(nome, ds, titulo, expr, p, desc=None, fmt=None):
    enc = {"fieldName": "v", "rowNumber": 0}
    if fmt:
        enc["format"] = fmt
    return w(nome, ds, [campo("v", expr)], {"version": 2, "widgetType": "counter",
             "frame": frame(titulo, desc), "encodings": {"value": enc}}, p)


GRUPO = {"Enviados à navegação": NAV, "Grupo controle": CTRL}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}


def layout():
    L = [
        texto("titulo", ["# Navegação da Captação — o grupo controle da Onda 1",
                         "Reumatologia e Tireoide · produção desde 12/10/2026 · dados atualizados todo dia"], pos(0, 0, 12, 2)),
        texto("mensagem", [
            "### Em uma frase",
            "De cada **20 pacientes** que o algoritmo identifica, **1 é sorteado para não ser enviado à navegação**. "
            "Esse pequeno grupo — o **grupo controle** — é a régua: comparando quem foi navegado com quem não foi, "
            "vamos saber **quanto a navegação aumenta o retorno do paciente à Rede**."], pos(0, 2, 12, 2)),
        texto("s1", ["## 1. O que mudou na operação"], pos(0, 4, 12, 1)),
        contador("k_total", "pacientes", "Pacientes identificados", "COUNT(`id_paciente`)", pos(0, 5, 4, 3),
                 "Com achado relevante e elegíveis para a navegação"),
        contador("k_nav", "pacientes", "Enviados à navegação", "SUM(`fl_navegacao`)", pos(4, 5, 4, 3),
                 "Seguem o fluxo normal — nada mudou para eles"),
        contador("k_ctrl", "pacientes", "Grupo controle (não enviados)", "SUM(`fl_controle`)", pos(8, 5, 4, 3),
                 "Recebem o atendimento habitual da Rede, sem a navegação ativa"),
        w("g_linha", "pacientes", [campo("linha"), campo("n", "COUNT(`id_paciente`)"), campo("grupo")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Por linha de cuidado", "A Tireoide responde por ~85% do volume"),
           "encodings": {"x": {"fieldName": "n", "scale": {"type": "quantitative"}, "axis": {"title": "pacientes"}},
                         "y": {"fieldName": "linha", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "color": cores("grupo", GRUPO), "label": {"show": True}}}, pos(0, 8, 6, 5)),
        w("g_dia", "pacientes", [campo("dia"), campo("n", "COUNT(`id_paciente`)"), campo("grupo")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Dia a dia", "Pacientes sorteados por dia de processamento"),
           "encodings": {"x": {"fieldName": "dia", "scale": {"type": "temporal"}, "axis": {"title": ""}},
                         "y": {"fieldName": "n", "scale": {"type": "quantitative"}, "axis": {"title": "pacientes"}},
                         "color": cores("grupo", GRUPO)}}, pos(6, 8, 6, 5)),
        texto("s2", ["## 2. O sorteio é confiável?",
                     "O sorteio é feito pelo **CPF**, uma única vez: o paciente fica **sempre no mesmo grupo**, "
                     "em qualquer linha e em qualquer dia. Ninguém escolhe quem entra no controle. "
                     "Três verificações rodam todos os dias — **o esperado é zero em todas**:"], pos(0, 13, 12, 2)),
        contador("k_g1", "garantias", "Pacientes do controle enviados por engano", "SUM(`controle_enviado`)", pos(0, 15, 4, 3)),
        contador("k_g2", "garantias", "Pacientes que trocaram de grupo", "SUM(`trocou_de_grupo`)", pos(4, 15, 4, 3)),
        contador("k_g3", "garantias", "Registros diferentes do sorteio original", "SUM(`diferente_do_sorteio`)", pos(8, 15, 4, 3)),
        contador("k_pct", "pacientes", "Parcela no controle até agora", "AVG(`fl_controle`)", pos(0, 18, 4, 3),
                 "Meta: 5%. Com poucos pacientes a parcela oscila e converge com o tempo", PCT),
        texto("leitura2", [
            "**Como ler:** a parcela observada varia em torno de 5% porque o sorteio é ao acaso — "
            "como uma moeda, que só mostra a proporção certa depois de muitos lançamentos. "
            "Se algum dos três números acima sair de zero, o time de dados é avisado no mesmo dia."], pos(4, 18, 8, 3)),
        texto("s3", ["## 3. Quando teremos o resultado?",
                     "O resultado depende de **juntar pacientes no grupo controle** e de **acompanhá-los por 90 dias**. "
                     "Quanto maior o grupo controle, mais rápido chega o resultado."], pos(0, 21, 12, 2)),
        w("g_prazo", "prazo", [campo("cenario"), campo("m", "SUM(`meses`)"), campo("etapa")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Meses até o primeiro resultado confiável",
                          "Para enxergar um aumento de 8 pontos na taxa de retorno (de 58% para 66%), Onda 1 somada"),
           "encodings": {"x": {"fieldName": "m", "scale": {"type": "quantitative"}, "axis": {"title": "meses"}},
                         "y": {"fieldName": "cenario", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "color": cores("etapa", ETAPA), "label": {"show": True}}}, pos(0, 23, 7, 5)),
        texto("leitura3", [
            "**Com 10%:** cerca de **6 meses e meio** — primeiro resultado por volta de **maio/2027**.",
            "",
            "**Com 5%:** cerca de **9 meses e meio** — por volta de **agosto/2027**.",
            "",
            "**O custo de 10%:** o dobro de pacientes sem navegação ativa (~100/mês em vez de ~50). "
            "Eles continuam com o atendimento habitual da Rede.",
            "",
            "A leitura é da **Onda 1 somada**: a Reumatologia sozinha tem volume pequeno para um resultado próprio. "
            "As próximas ondas somam pacientes e encurtam o prazo."], pos(7, 23, 5, 5)),
        texto("s4", ["## 4. O que precisamos do negócio",
                     "1. **De acordo** com a operação da Onda 1 como está.",
                     "2. **Decidir o tamanho do grupo controle:** manter 5% ou subir para 10%. Subir é possível a qualquer momento; descer, não.",
                     "3. **Confirmar o indicador principal:** a **taxa de retorno** — % de pacientes que voltam a consumir na Rede em até 90 dias. "
                     "O valor em R$ por paciente entra como indicador secundário, porque varia muito de paciente para paciente e demora mais para mostrar diferença."],
              pos(0, 28, 12, 3)),
    ]
    return L


def main():
    datasets = [{"name": k, "displayName": NOMES[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]}
                for k, v in SQL.items()]
    painel = {
        "datasets": datasets,
        "pages": [{"name": "relatorio", "displayName": "Relatório — Onda 1", "pageType": "PAGE_TYPE_CANVAS",
                   "layoutVersion": "GRID_V1", "layout": layout()}],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#F7F8F9", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#1D2B36", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
            "visualizationColors": [NAV, CTRL, "#7FB8BB", "#4E5185", "#C9CED3"],
            "widgetHeaderAlignment": "LEFT", "widgetCornerRadius": 10}},
    }
    out = AQUI / "painel-negocio-v1.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(datasets), "datasets", len(painel["pages"][0]["layout"]), "widgets")


if __name__ == "__main__":
    main()
