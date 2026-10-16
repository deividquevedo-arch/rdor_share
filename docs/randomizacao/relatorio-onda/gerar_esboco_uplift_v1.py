"""Esboço v1 do relatório de uplift — completo, com os indicadores do rascunho do time.

Uso: python gerar_esboco_uplift_v1.py [--inicio 2026-10-12 --fim 2026-10-16 --perfil nlp-platform]
     ->  ../relatorios/esboco-relatorio-uplift-v1.html
Dados reais só na população e na linha do tempo operacional; o que depende de consumo fica vazio.
"""
import argparse
import datetime as dt
from pathlib import Path

from gerar_esboco_uplift import EXTRA, svg_acumulo, svg_quadro_vazio, tabela
from gerar_relatorio_onda import CSS, LBL, OP, TXT, consulta, fmt, legenda, n, svg_diario

LINHAS = ["reumatologia", "tirads"]
NOMES = {"reumatologia": "Reumatologia", "tirads": "Tireoide (TI-RADS)"}
MES = 21
PH = '<span class="ph">aguardando desfecho</span>'


def svg_linhas_vazio(rotulos_x, titulo):
    """Moldura de linha do tempo: duas curvas tracejadas sem valores."""
    W, H, L, B, T, R = 760, 230, 40, 34, 16, 20
    X = lambda i: L + (W - L - R) * i / (len(rotulos_x) - 1)
    Y = lambda f: T + (H - T - B) * (1 - f)
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{titulo}, aguardando desfecho">']
    p.append(f'<line x1="{L}" y1="{H - B}" x2="{W - R}" y2="{H - B}" style="stroke:var(--line)"/>')
    for i, r in enumerate(rotulos_x):
        p.append(f'<text x="{X(i):.1f}" y="{H - 12}" text-anchor="middle" {TXT}>{r}</text>')
    for cor, alt in (("var(--nav)", 0.78), ("var(--ctrl)", 0.55)):
        pts = " ".join(f"{X(i):.1f},{Y(alt * (1 - 0.92 ** (i + 1)) / (1 - 0.92 ** len(rotulos_x))):.1f}"
                       for i in range(len(rotulos_x)))
        p.append(f'<polyline points="{pts}" style="fill:none;stroke:{cor};stroke-width:2;stroke-dasharray:5 4"/>')
    p += _aviso(W, H)
    p.append("</svg>")
    return "".join(p)


def svg_km_vazio():
    """Moldura de curva de sobrevivência (Kaplan-Meier): % que já consumiu, por dia desde o sorteio."""
    W, H, L, B, T, R = 760, 230, 40, 34, 16, 20
    dias = [0, 15, 30, 45, 60, 75, 90]
    X = lambda d: L + (W - L - R) * d / 90
    Y = lambda f: T + (H - T - B) * (1 - f)
    p = ['<svg viewBox="0 0 760 230" role="img" aria-label="Curva de tempo até o primeiro consumo, aguardando desfecho">']
    p.append(f'<line x1="{L}" y1="{H - B}" x2="{W - R}" y2="{H - B}" style="stroke:var(--line)"/>')
    for d in dias:
        p.append(f'<text x="{X(d):.1f}" y="{H - 12}" text-anchor="middle" {TXT}>{d} d</text>')
    for cor, taxa in (("var(--nav)", 0.012), ("var(--ctrl)", 0.007)):
        pts, f = [], 0.0
        for d in range(0, 91, 10):
            pts.append(f"{X(d):.1f},{Y(f):.1f}")
            f = 1 - (1 - f) * (1 - taxa * 10)
            pts.append(f"{X(d):.1f},{Y(f):.1f}")
        p.append(f'<polyline points="{" ".join(pts)}" style="fill:none;stroke:{cor};stroke-width:2;stroke-dasharray:5 4"/>')
    p += _aviso(W, H)
    p.append("</svg>")
    return "".join(p)


def svg_hist_vazio():
    """Moldura de histograma de consumo por paciente, cauda longa."""
    W, H, L, B, T = 760, 220, 40, 34, 16
    alt = [0.82, 0.6, 0.42, 0.3, 0.2, 0.14, 0.1, 0.07, 0.05, 0.04, 0.03, 0.02]
    larg = (W - L - 10) / len(alt)
    p = ['<svg viewBox="0 0 760 220" role="img" aria-label="Histograma de consumo por paciente, aguardando desfecho">']
    p.append(f'<line x1="{L}" y1="{H - B}" x2="{W - 10}" y2="{H - B}" style="stroke:var(--line)"/>')
    for i, a in enumerate(alt):
        h = (H - T - B) * a
        p.append(f'<rect x="{L + larg * i + 2:.1f}" y="{H - B - h:.1f}" width="{larg - 4:.1f}" height="{h:.1f}" '
                 f'style="fill:none;stroke:var(--muted);stroke-dasharray:4 3"/>')
    p.append(f'<text x="{L}" y="{H - 12}" {TXT}>R$ 0</text>')
    p.append(f'<text x="{W - 10}" y="{H - 12}" text-anchor="end" {TXT}>valor por paciente →</text>')
    p += _aviso(W, H)
    p.append("</svg>")
    return "".join(p)


def _aviso(W, H):
    return [f'<rect x="{W / 2 - 150}" y="{H / 2 - 30}" width="300" height="40" rx="8" '
            f'style="fill:var(--surface);stroke:var(--line)"/>',
            f'<text x="{W / 2}" y="{H / 2 - 5}" text-anchor="middle" {LBL} font-weight="600">'
            "aguardando desfecho — sem valores</text>"]


def carregar(a):
    lst = ", ".join(f"'{l}'" for l in LINHAS)
    per = f"to_date(dt_execucao_modelo) BETWEEN '{a.inicio}' AND '{a.fim}'"
    dia = consulta(f"""
      SELECT to_date(dt_execucao_modelo) dia, sum(nav) nav, sum(ctrl) ctrl FROM (
        SELECT nm_especialidade, dt_execucao_modelo, max_by(qt_intervencao, dh_registro) nav, max_by(qt_controle, dh_registro) ctrl
        FROM {OP}.tb_mov_randomizacao_relatorio WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({lst}) AND {per}
        GROUP BY 1, 2) GROUP BY 1 ORDER BY 1""", a.perfil)
    pac = consulta(f"""
      SELECT nm_especialidade linha, count(DISTINCT num_cpf) sorteados,
             count(DISTINCT CASE WHEN tp_grupo='controle' THEN num_cpf END) controle,
             count(DISTINCT CASE WHEN tp_grupo='intervencao' THEN num_cpf END) navegacao,
             count(DISTINCT to_date(dt_execucao_modelo)) dias
      FROM {OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({lst}) AND {per}
      GROUP BY 1""", a.perfil)
    return dia, {x["linha"]: {k: n(v) for k, v in x.items() if k != "linha"} for x in pac}


def card(titulo, corpo):
    return f'<div class="card"><h3>{titulo}</h3>{corpo}</div>'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inicio", default="2026-10-12")
    ap.add_argument("--fim", default="2026-10-16")
    ap.add_argument("--perfil", default="nlp-platform")
    a = ap.parse_args()
    dia, pac = carregar(a)
    tot = {k: sum(v[k] for v in pac.values()) for k in ("sorteados", "controle", "navegacao")}
    dias_exec = max((v["dias"] for v in pac.values()), default=1) or 1
    ctrl_mes_5 = tot["sorteados"] / dias_exec * MES * 0.05
    ctrl_mes_10 = ctrl_mes_5 * 2

    H = ["<title>Uplift da captação — esboço v1</title>",
         '<meta name="claude:viewport-fit-sides" content="cover">',
         '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Condensed:wght@600;700'
         '&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&display=swap">',
         f"<style>{CSS}{EXTRA}.nivel{{font:600 11px/1 var(--f-m);letter-spacing:.08em;text-transform:uppercase;"
         f"padding:4px 8px;border-radius:4px;background:var(--accent-soft);color:var(--accent);align-self:flex-start}}</style>",
         '<div class="wrap">', "<header>",
         '<span class="draft">Esboço v1 · indicadores do time · sem desfecho ainda</span>',
         '<span class="eyebrow">Fábrica de IA · Captação · Fase 3 — uplift</span>',
         "<h1>Uplift da captação — relatório mensal por onda</h1>",
         f'<span class="meta">Onda 1 (Reumatologia, Tireoide) · PRD · sorteio desde 12/10 · '
         f"dados operacionais até {a.fim[8:10]}/{a.fim[5:7]}. Os indicadores de consumo ficam vazios até a janela "
         "fechar na base de consumo.</span>", "</header>"]

    # 0. Como ler
    H += ["<section><h2>Como ler este relatório</h2>",
          tabela(["Nível", "Para que serve", "Indicadores"], [
              ['<span class="nivel">principal</span>', "decide se a navegação gerou consumo a mais",
               "consumo por paciente sorteado em 90 dias"],
              ['<span class="nivel">secundários</span>', "confirmam e explicam o principal",
               "% convertidos · tempo até o primeiro consumo · consumo específico (onco, corp)"],
              ['<span class="nivel">linha do tempo</span>', "mostra a evolução semana a semana",
               "% e quantidade de convertidos · consumo acumulado"],
              ['<span class="nivel">descritivos</span>', "descrevem a distribuição, sem decidir",
               "média · mediana · desvio · mínimo · máximo · outliers · tickets · primeiro atendimento · quantidades"],
              ['<span class="nivel">operacionais</span>', "contexto do envio",
               "envios e quantidade enviada · pacientes retidos no controle"],
              ['<span class="nivel">futuros</span>', "exigem horizonte ou definição", "LTV em 12 meses · tempo até cirurgia"],
          ], minw=760),
          '<p class="note">Todos os indicadores comparam os grupos <strong>sorteados</strong> (intenção de tratar): o '
          "controle inteiro contra a navegação inteira, contando como zero quem não consumiu. Só o indicador principal "
          "decide; com mais de 20 indicadores, alguns sairiam \"significativos\" por acaso.</p></section>"]

    # 1. População
    linhas_pop = [[NOMES[l], fmt(v["sorteados"]), fmt(v["navegacao"]), fmt(v["controle"])] for l, v in pac.items()]
    linhas_pop.append(["Onda 1", fmt(tot["sorteados"]), fmt(tot["navegacao"]), fmt(tot["controle"])])
    H += ["<section><h2>1. População sorteada</h2>",
          '<div class="kpis">',
          f'<div class="kpi"><span class="k">Sorteados</span><span class="v">{fmt(tot["sorteados"])}</span>'
          f'<span class="s">{a.inicio[8:10]}/{a.inicio[5:7]} a {a.fim[8:10]}/{a.fim[5:7]}</span></div>',
          f'<div class="kpi nav"><span class="k">Navegação</span><span class="v">{fmt(tot["navegacao"])}</span></div>',
          f'<div class="kpi ctrl"><span class="k">Controle</span><span class="v">{fmt(tot["controle"])}</span></div>',
          f'<div class="kpi"><span class="k">Controle/mês (estimado)</span><span class="v">{fmt(ctrl_mes_5)}</span>'
          '<span class="s">a 5%</span></div>', "</div>",
          '<div style="margin-top:12px">' + tabela(["Linha", "Sorteados", "Navegação", "Controle"], linhas_pop,
                                                   classe_num={1, 2, 3}, minw=480) + "</div>",
          '<p class="note">Análise principal restrita às unidades do escopo de navegação vigente (desde 14/10).</p></section>']

    # 2. Principal
    H += ["<section><h2>2. Indicador principal — consumo por paciente sorteado em 90 dias</h2>",
          '<span class="nivel">principal</span>',
          '<div class="formula" style="margin-top:10px"><strong>Uplift</strong> = consumo médio por paciente da '
          "<strong>navegação</strong> − consumo médio por paciente do <strong>controle</strong>, em 90 dias após o sorteio, "
          "ajustado pelo consumo dos 180 dias anteriores.</div>",
          legenda(("navegação", "var(--nav)"), ("controle", "var(--ctrl)")),
          '<div class="tbl" style="padding:12px 14px;margin:8px 0 12px">' +
          svg_quadro_vazio(["Reumatologia", "Tireoide", "Onda 1"]) + "</div>",
          tabela(["Linha", "Per capita — navegação", "Per capita — controle", "Uplift por paciente", "IC 95%",
                  "p-valor", "Uplift total (× navegados)"],
                 [["Reumatologia", PH, PH, PH, PH, PH, PH], ["Tireoide (TI-RADS)", PH, PH, PH, PH, PH, PH],
                  ["Onda 1", PH, PH, PH, PH, PH, PH]], minw=900),
          '<p class="note"><strong>Decisão:</strong> pela linha da onda. A abertura por linha é descritiva. '
          "<strong>Uplift total</strong> = uplift por paciente × pacientes navegados: é o consumo atribuível à navegação "
          "no período.</p></section>"]

    # 3. Secundários
    H += ["<section><h2>3. Indicadores secundários</h2>", '<span class="nivel">secundários</span>',
          '<div class="grid2" style="margin-top:10px">',
          card("% convertidos", '<p>Pacientes com algum consumo na Rede depois do sorteio ÷ pacientes do grupo. '
               "<strong>\"Captado\" = convertido</strong> (pendente de confirmação).</p>" +
               tabela(["Janela", "Navegação", "Controle", "Diferença (IC 95%)"],
                      [["30 dias", PH, PH, PH], ["90 dias", PH, PH, PH], ["180 dias", PH, PH, PH]], minw=300)),
          card("Consumo específico", "<p>Consumo por paciente nas categorias da linha.</p>" +
               tabela(["Categoria", "Navegação", "Controle", "Uplift"],
                      [["Total", PH, PH, PH], ["Corp", PH, PH, PH], ["Onco", PH, PH, PH]], minw=300)),
          "</div>",
          '<h3 style="font:600 16px var(--f-d);margin:16px 0 6px">Tempo até o primeiro consumo</h3>',
          legenda(("navegação", "var(--nav)"), ("controle", "var(--ctrl)")),
          '<div class="tbl" style="padding:12px 14px;margin-top:8px">' + svg_km_vazio() + "</div>",
          '<p class="note">Curva de sobrevivência (Kaplan-Meier): % do grupo que já consumiu em cada dia após o sorteio. '
          "Quem ainda não consumiu continua contando até o fim da janela, em vez de sair da média. Resume-se pela "
          "mediana de dias até o primeiro consumo.</p></section>"]

    # 4. Linha do tempo
    sem = ["sem 1", "sem 2", "sem 3", "sem 4", "sem 5", "sem 6", "sem 7", "sem 8"]
    H += ["<section><h2>4. Linha do tempo — semanas desde o sorteio</h2>", '<span class="nivel">linha do tempo</span>',
          '<div class="grid2" style="margin-top:10px">',
          card("% convertidos acumulado", svg_linhas_vazio(sem, "Percentual de convertidos acumulado por semana")),
          card("Consumo acumulado por paciente", svg_linhas_vazio(sem, "Consumo acumulado por paciente por semana")),
          "</div>",
          '<p class="note">Também por quantidade: convertidos e consumo total por semana, por grupo. O eixo é o tempo '
          "desde o sorteio de cada paciente, não a data do calendário — assim pacientes sorteados em semanas diferentes "
          "ficam comparáveis.</p></section>"]

    # 5. Descritivos
    desc = ["Média", "Mediana", "Desvio-padrão", "Mínimo", "Máximo", "Média sem os 1% maiores"]
    H += ["<section><h2>5. Distribuição do consumo e tickets</h2>", '<span class="nivel">descritivos</span>',
          '<div class="grid2" style="margin-top:10px">',
          card("Consumo por paciente em 90 dias",
               tabela(["Estatística", "Navegação", "Controle"], [[d, PH, PH] for d in desc], minw=300)),
          card("Tickets e quantidades (entre quem consumiu)",
               tabela(["Indicador", "Navegação", "Controle"],
                      [["Ticket médio por paciente", PH, PH], ["Ticket médio por atendimento", PH, PH],
                       ["Ticket médio por procedimento", PH, PH], ["Valor médio do primeiro atendimento", PH, PH],
                       ["Quantidade de consumo — total", PH, PH], ["Quantidade — corp", PH, PH],
                       ["Quantidade — onco", PH, PH]], minw=300)),
          "</div>",
          '<div class="tbl" style="padding:12px 14px;margin-top:12px">' + svg_hist_vazio() + "</div>",
          '<p class="note"><strong>Histograma no lugar da moda:</strong> valor monetário quase não se repete, então a moda '
          "não informa. <strong>Outliers:</strong> poucos pacientes de alto custo movem a média; a regra é fixada antes "
          "(ex.: média com e sem os 1% maiores, intervalo por bootstrap). <strong>Tickets</strong> são calculados só entre "
          "quem consumiu e por isso não decidem: a navegação pode trazer pacientes de ticket menor e mais receita "
          "total.</p></section>"]

    # 6. Comparabilidade
    H += ["<section><h2>6. Os grupos são comparáveis?</h2>",
          tabela(["Característica antes do sorteio", "Navegação", "Controle", "Diferença padronizada"],
                 [["Consumo por paciente nos 180 dias anteriores", PH, PH, PH], ["Idade média", PH, PH, PH],
                  ["% sexo feminino", PH, PH, PH], ["Distribuição por regional", PH, PH, PH]], minw=640),
          '<p class="note">Diferença padronizada acima de 0,1 indica desequilíbrio a observar. O indicador principal já é '
          "ajustado pelo consumo anterior.</p></section>"]

    # 7. Operacional (real)
    dias = [str(x["dia"]) for x in dia]
    H += ["<section><h2>7. Contexto operacional — envios e retidos</h2>", '<span class="nivel">operacionais</span>',
          legenda(("enviados à navegação", "var(--nav)"), ("retidos no controle", "var(--ctrl)")),
          '<div class="tbl" style="padding:12px 14px;margin-top:8px">' +
          svg_diario(dias, [n(x["nav"]) for x in dia], [n(x["ctrl"]) for x in dia], "Exames enviados e retidos por dia") +
          "</div>",
          '<p class="note"><strong>Dado real</strong> (exames por dia, onda 1). O controle não tem envio por desenho, então '
          "\"% de envios controle × intervenção\" não mede efeito — fica aqui como contexto, e no relatório da onda.</p>"
          "</section>"]

    # 8. Significância e horizonte
    meses = list(range(1, 13))
    H += ["<section><h2>8. Significância e horizonte</h2>",
          '<div class="grid2">',
          card("Como se decide", "<ul><li><strong>IC 95%</strong> do uplift que não contém zero.</li>"
               "<li><strong>p-valor</strong> do indicador principal, com o limiar fixado antes (0,05).</li>"
               "<li><strong>Probabilidade de ganho</strong> — chance de o uplift ser maior que zero, para leitura de "
               "negócio.</li><li>Secundários e descritivos <strong>não</strong> decidem.</li></ul>"),
          card("Quando fica legível", svg_acumulo(meses, [ctrl_mes_5 * m for m in meses],
                                                  [ctrl_mes_10 * m for m in meses]) +
               '<p class="note" style="margin:0">Controles acumulados da onda 1, a 5% e a 10%. As ondas 2 e 3 encurtam o '
               "prazo a partir de 17/11 e 24/11.</p>"),
          "</div></section>"]

    # 9. Futuros
    H += ["<section><h2>9. Indicadores futuros</h2>", '<span class="nivel">futuros</span>',
          '<div class="grid2" style="margin-top:10px">',
          card("Life time value", "<p>Consumo acumulado por paciente em 12 meses, por grupo. Exige o primeiro ano completo "
               "de acompanhamento.</p>"),
          card("Tempo até cirurgia", "<p>Curva de sobrevivência até o primeiro procedimento cirúrgico da linha. Exige a "
               "lista de procedimentos (TUSS ou tipo de produto) que contam como cirurgia, por linha.</p>"),
          "</div></section>"]

    # 10. Pendências
    pend = [("Indicador principal e janela", "proposto: consumo por paciente em 90 dias — confirmar com o time"),
            ("\"Captado\" = convertido", "confirmar a definição"),
            ("Separação corp × onco", "qual campo da base de consumo classifica"),
            ("Procedimentos que contam como cirurgia", "lista por linha"),
            ("Regra de outliers", "proposto: com e sem os 1% maiores, IC por bootstrap"),
            ("De acordo da onda 1", "previsto para 23/10")]
    H += ["<section><h2>10. Pendências para fechar o modelo</h2>", '<ul class="chk">']
    H += [f'<li><span class="st pend">PENDENTE</span><span><strong>{t}</strong> — {d}</span></li>' for t, d in pend]
    H += ["</ul></section>",
          "<section><h2>11. O que pode distorcer a leitura</h2><ul>",
          "<li><strong>Contaminação:</strong> paciente do controle relevante em linha não randomizada pode ser navegado "
          "por ela. Reportado com e sem esses pacientes.</li>",
          "<li><strong>Atraso da base de consumo:</strong> cerca de duas semanas; só entra quem tem a janela inteira.</li>",
          "<li><strong>Escopo de unidades:</strong> sorteados antes de 14/10 incluem unidades fora do escopo atual.</li>",
          "<li><strong>Sem CPF:</strong> vai à navegação e fica fora da análise.</li>",
          "</ul></section>",
          f'<footer>Esboço v1 gerado em {dt.date.today().isoformat()} por gerar_esboco_uplift_v1.py · dados reais só na '
          "população e no contexto operacional · nenhum resultado de uplift calculado.</footer>", "</div>"]

    destino = Path(__file__).parent.parent / "relatorios" / "esboco-relatorio-uplift-v1.html"
    destino.write_text("\n".join(H), encoding="utf-8")
    print(destino.name, "| sorteados", int(tot["sorteados"]), "| controle", int(tot["controle"]))


if __name__ == "__main__":
    main()
