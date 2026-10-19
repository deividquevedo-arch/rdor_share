"""Painel AI/BI do relatório da randomização para o negócio — v2.

Uso: python gerar_painel_negocio_v2.py  ->  painel-negocio-v2.lvdash.json
Não altera a v1. Foco: impacto do grupo controle no volume da navegação (5% hoje, 10% com o de
acordo), distribuição observada e prazo até o primeiro resultado de uplift.
Mudanças sobre a v1, pelo cruzamento com o painel de Execuções & Qualidade:
  1. "Pacientes sorteados" no lugar de "identificados" (palavra já usada no funil da Central);
  2. indicador explicado como taxa de retorno, distinto da "taxa de captação" da Central;
  3. nota de reconciliação (pacientes, só produção, só linhas com sorteio);
  4. parcela sempre como total do controle / total de pacientes ("1 em cada 20").
"""
import json
from pathlib import Path

from gerar_painel_negocio import (CTRL, NAV, PCT, contador, cores, frame, pos, texto, w, campo,
                                  LOG, BASE, SQL as SQL_V1)

AQUI = Path(__file__).parent
ESCOPO_DESDE = "2026-10-14"  # 103 unidades com navegação, vigente em prd (PR 7578)
GRUPO = {"Enviados à navegação": NAV, "Grupo controle": CTRL}

# Meses até o primeiro resultado confiável para enxergar +8 p.p. na taxa de retorno (58% -> 66%),
# Onda 1 somada. Fonte: ../horizonte-uplift-por-linha-v0.md.
PRAZO = [
    ("Com 10% (após o de acordo)", "1. Juntar pacientes no controle", 3.0),
    ("Com 10% (após o de acordo)", "2. Acompanhar cada paciente 90 dias", 3.0),
    ("Com 10% (após o de acordo)", "3. Base de consumo atualizar", 0.5),
    ("Se ficasse em 5%", "1. Juntar pacientes no controle", 6.0),
    ("Se ficasse em 5%", "2. Acompanhar cada paciente 90 dias", 3.0),
    ("Se ficasse em 5%", "3. Base de consumo atualizar", 0.5),
]
ETAPA = {"1. Juntar pacientes no controle": NAV, "2. Acompanhar cada paciente 90 dias": "#7FB8BB",
         "3. Base de consumo atualizar": "#C9CED3"}

SQL = {
    "pacientes": SQL_V1["pacientes"],
    "garantias": SQL_V1["garantias"],
    "distribuicao": f"""
WITH p AS (
  SELECT CASE nm_especialidade WHEN 'tirads' THEN 'Tireoide' WHEN 'reumatologia' THEN 'Reumatologia'
              ELSE initcap(nm_especialidade) END AS linha,
         num_cpf, max(CASE WHEN tp_grupo = 'controle' THEN 1 ELSE 0 END) AS c
  FROM {LOG} WHERE tp_ambiente = 'prd' GROUP BY 1, 2
)
SELECT linha, count(*) AS pacientes, sum(c) AS no_controle, sum(c) / count(*) AS pct_controle, 0.05 AS meta
FROM p GROUP BY linha
UNION ALL
SELECT 'Onda 1 (total)', count(*), sum(c), sum(c) / count(*), 0.05 FROM p
""",
    # Volume mensal observado desde que o escopo de unidades passou a valer, projetado nos dois cenários.
    "volume": f"""
WITH p AS (
  SELECT num_cpf, nm_especialidade, min(to_date(dt_execucao_modelo)) AS d
  FROM {LOG} WHERE tp_ambiente = 'prd' GROUP BY 1, 2
),
dias AS (
  SELECT count(DISTINCT to_date(dt_execucao_modelo)) AS n FROM {LOG}
  WHERE tp_ambiente = 'prd' AND to_date(dt_execucao_modelo) >= DATE'{ESCOPO_DESDE}'
),
mes AS (SELECT count(*) / max(dias.n) * 21 AS pacientes_mes FROM p CROSS JOIN dias WHERE p.d >= DATE'{ESCOPO_DESDE}'),
c AS (SELECT * FROM VALUES ('Hoje: 5% (validação)', 0.05, 1), ('Com o de acordo: 10%', 0.10, 2) AS t(cenario, fracao, ordem))
SELECT c.cenario, c.ordem, 'Enviados à navegação' AS grupo, round(m.pacientes_mes * (1 - c.fracao)) AS pacientes_mes
FROM c CROSS JOIN mes m
UNION ALL
SELECT c.cenario, c.ordem, 'Grupo controle', round(m.pacientes_mes * c.fracao) FROM c CROSS JOIN mes m
ORDER BY ordem, grupo
""",
    "prazo": "SELECT * FROM VALUES\n" + ",\n".join(f"  ('{c}', '{e}', {m})" for c, e, m in PRAZO)
             + "\nAS t(cenario, etapa, meses)",
}
NOMES = {"pacientes": "Pacientes sorteados (um por linha)", "garantias": "Garantias do sorteio",
         "distribuicao": "Distribuição observada por linha", "volume": "Volume mensal nos dois cenários",
         "prazo": "Prazo até o resultado"}


def layout():
    return [
        texto("titulo", ["# Navegação da Captação — grupo controle da Onda 1",
                         "Reumatologia e Tireoide · produção desde 12/10/2026 · atualizado todo dia"], pos(0, 0, 12, 2)),
        texto("mensagem", [
            "### Em uma frase",
            "Hoje, de cada **20 pacientes** com indicação de navegação, **1 é sorteado** para o **grupo controle** e "
            "segue só com o atendimento habitual da Rede. Com o de acordo, passa a ser **1 em cada 10**. "
            "Comparar os dois grupos mostra **quanto a navegação aumenta o retorno do paciente à Rede**."], pos(0, 2, 12, 2)),

        texto("s1", ["## 1. Como está hoje — fase de validação com 5%"], pos(0, 4, 12, 1)),
        contador("k_total", "pacientes", "Pacientes sorteados", "COUNT(`id_paciente`)", pos(0, 5, 3, 3),
                 "Com achado relevante e elegíveis para a navegação"),
        contador("k_nav", "pacientes", "Enviados à navegação", "SUM(`fl_navegacao`)", pos(3, 5, 3, 3),
                 "Seguem o fluxo normal"),
        contador("k_ctrl", "pacientes", "Grupo controle", "SUM(`fl_controle`)", pos(6, 5, 3, 3),
                 "Atendimento habitual, sem navegação ativa"),
        contador("k_pct", "distribuicao", "Parcela no controle", "SUM(CASE WHEN `linha` = 'Onda 1 (total)' THEN `pct_controle` END)",
                 pos(9, 5, 3, 3), "Esperado: 1 em cada 20 (5%)", PCT),
        w("g_dia", "pacientes", [campo("dia"), campo("n", "COUNT(`id_paciente`)"), campo("grupo")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Distribuição dia a dia", "Pacientes sorteados por dia — o controle é a fatia laranja"),
           "encodings": {"x": {"fieldName": "dia", "scale": {"type": "temporal"}, "axis": {"title": ""}},
                         "y": {"fieldName": "n", "scale": {"type": "quantitative"}, "axis": {"title": "pacientes"}},
                         "color": cores("grupo", GRUPO)}}, pos(0, 8, 7, 5)),
        w("g_dist", "distribuicao", [campo("linha"), campo("y", "SUM(`pct_controle`)")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Parcela no controle, por linha", "Esperado: 5%. Com poucos pacientes, varia em torno disso"),
           "encodings": {"x": {"fieldName": "linha", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "y": {"fieldName": "y", "scale": {"type": "quantitative"}, "format": PCT, "axis": {"title": ""}},
                         "label": {"show": True}}}, pos(7, 8, 5, 5)),

        texto("s2", [
            "## 2. Por que começamos com 5% e vamos para 10%",
            "**5% foi a validação.** Antes de deixar mais pacientes sem navegação ativa, era preciso provar, em produção, "
            "que o sorteio funciona: que o grupo é sorteado ao acaso, que o paciente nunca troca de grupo e que "
            "**nenhum paciente do controle é enviado por engano**. Com 5% isso foi verificado mexendo o mínimo na operação.",
            "",
            "**10% é para ter o resultado na metade do tempo.** Com o dobro de pacientes no controle, a comparação fica "
            "confiável muito antes. Subir não muda ninguém de lugar: **quem já está no controle continua, quem foi navegado "
            "continua** — só entram mais pacientes novos no controle. (Subir é possível a qualquer momento; descer, não.)"],
            pos(0, 13, 12, 3)),

        texto("s3", ["## 3. O impacto no volume da navegação, por mês"], pos(0, 16, 12, 1)),
        w("g_vol", "volume", [campo("cenario"), campo("n", "SUM(`pacientes_mes`)"), campo("grupo")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Pacientes por mês nos dois cenários",
                          "Volume real da Onda 1 desde 14/10 (103 unidades com navegação), projetado para um mês"),
           "encodings": {"x": {"fieldName": "n", "scale": {"type": "quantitative"}, "axis": {"title": "pacientes por mês"}},
                         "y": {"fieldName": "cenario", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "color": cores("grupo", GRUPO), "label": {"show": True}}}, pos(0, 17, 7, 5)),
        texto("leitura3", [
            "A Onda 1 sorteia **cerca de 1.060 pacientes por mês** (≈ 900 da Tireoide e ≈ 160 da Reumatologia).",
            "",
            "**Hoje (5%):** ≈ 1.010 seguem para a navegação e **≈ 50 ficam no controle**.",
            "",
            "**Com 10%:** ≈ 955 seguem para a navegação e **≈ 105 ficam no controle**.",
            "",
            "Para as navegadoras, a mudança é de **≈ 50 pacientes a menos por mês** na fila — cerca de 2 por dia útil, "
            "somando as duas linhas."], pos(7, 17, 5, 5)),

        texto("s4", ["## 4. Quando teremos o primeiro resultado de uplift"], pos(0, 22, 12, 1)),
        w("g_prazo", "prazo", [campo("cenario"), campo("m", "SUM(`meses`)"), campo("etapa")],
          {"version": 3, "widgetType": "bar",
           "frame": frame("Meses até o primeiro resultado consistente, a partir do de acordo",
                          "Para enxergar um aumento de 8 pontos na taxa de retorno (de 58% para 66%), Onda 1 somada"),
           "encodings": {"x": {"fieldName": "m", "scale": {"type": "quantitative"}, "axis": {"title": "meses"}},
                         "y": {"fieldName": "cenario", "scale": {"type": "categorical"}, "axis": {"title": ""}},
                         "color": cores("etapa", ETAPA), "label": {"show": True}}}, pos(0, 23, 7, 5)),
        texto("leitura4", [
            "**Com 10%: cerca de 6 meses e meio** — primeiro resultado por volta de **maio/2027**.",
            "",
            "Se ficasse em 5%: cerca de 9 meses e meio — por volta de agosto/2027.",
            "",
            "**Por que demora:** além de juntar pacientes no controle, cada paciente precisa de **90 dias** de "
            "acompanhamento para sabermos se voltou à Rede, e a base de consumo chega com ~2 semanas de atraso.",
            "",
            "**O que será medido:** a **taxa de retorno** — % de pacientes que voltam a consumir na Rede em até 90 dias, "
            "medida **do mesmo jeito nos dois grupos**. Não é a \"taxa de captação\" da Central, que só existe para quem "
            "foi navegado. O valor em R$ por paciente vem como indicador secundário."], pos(7, 23, 5, 5)),

        texto("s5", ["## 5. Garantias — verificadas todos os dias (o esperado é zero)"], pos(0, 28, 12, 1)),
        contador("k_g1", "garantias", "Pacientes do controle enviados por engano", "SUM(`controle_enviado`)", pos(0, 29, 4, 3)),
        contador("k_g2", "garantias", "Pacientes que trocaram de grupo", "SUM(`trocou_de_grupo`)", pos(4, 29, 4, 3)),
        contador("k_g3", "garantias", "Registros diferentes do sorteio original", "SUM(`diferente_do_sorteio`)", pos(8, 29, 4, 3)),

        texto("s6", ["## 6. O que precisamos do negócio",
                     "1. **De acordo** com a operação da Onda 1 e com a **subida do grupo controle para 10%**.",
                     "2. **Confirmar o indicador principal:** taxa de retorno à Rede em até 90 dias."], pos(0, 32, 12, 2)),
        texto("nota", [
            "*Como ler os números: contamos **pacientes** (um paciente pode ter mais de um exame), **só de produção** e "
            "**só das linhas com sorteio**. Outros painéis podem mostrar exames, somar homologação ou incluir todas as "
            "linhas — por isso os totais podem ser diferentes. Prazos e datas são estimativas a partir do volume e da "
            "variação de consumo medidos até 19/10/2026.*"], pos(0, 34, 12, 2)),
    ]


def main():
    datasets = [{"name": k, "displayName": NOMES[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]}
                for k, v in SQL.items()]
    v1 = json.loads((AQUI / "painel-negocio-v1.lvdash.json").read_text(encoding="utf-8"))
    painel = {"datasets": datasets,
              "pages": [{"name": "relatorio", "displayName": "Relatório — Onda 1", "pageType": "PAGE_TYPE_CANVAS",
                         "layoutVersion": "GRID_V1", "layout": layout()}],
              "uiSettings": v1["uiSettings"]}
    out = AQUI / "painel-negocio-v2.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, len(datasets), "datasets", len(painel["pages"][0]["layout"]), "widgets")


if __name__ == "__main__":
    main()
