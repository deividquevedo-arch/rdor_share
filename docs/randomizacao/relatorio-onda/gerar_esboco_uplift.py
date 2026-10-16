"""Esboço do relatório de uplift (fase 3). Estrutura final; resultados vazios até haver desfecho.

Uso: python gerar_esboco_uplift.py  ->  ../relatorios/esboco-relatorio-uplift-v0.html
Números reais só onde já existem: população randomizada e projeção de acúmulo de controles.
"""
import datetime as dt
from pathlib import Path

from gerar_relatorio_onda import CSS, LBL, TXT, _escala, fmt, legenda


def svg_acumulo(meses, s5, s10):
    """Controles acumulados por mês, 5% e 10%."""
    W, H, L, B, T, R = 760, 260, 54, 34, 16, 70
    top, passo = _escala(max(s10))
    X = lambda m: L + (W - L - R) * (m - meses[0]) / (meses[-1] - meses[0])
    Y = lambda v: T + (H - T - B) * (1 - v / top)
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Controles acumulados por mês, 5% e 10%">']
    for i in range(5):
        v = passo * i
        p.append(f'<line x1="{L}" y1="{Y(v):.1f}" x2="{W - R}" y2="{Y(v):.1f}" style="stroke:var(--line)"/>')
        p.append(f'<text x="{L - 6}" y="{Y(v) + 4:.1f}" text-anchor="end" {TXT}>{fmt(v)}</text>')
    for m in meses:
        p.append(f'<text x="{X(m):.1f}" y="{H - 12}" text-anchor="middle" {TXT}>mês {m}</text>')
    for serie, cor, rot in ((s5, "var(--ctrl)", "5%"), (s10, "var(--accent)", "10%")):
        pts = " ".join(f"{X(m):.1f},{Y(v):.1f}" for m, v in zip(meses, serie))
        p.append(f'<polyline points="{pts}" style="fill:none;stroke:{cor};stroke-width:2.5"/>')
        for m, v in zip(meses, serie):
            p.append(f'<circle cx="{X(m):.1f}" cy="{Y(v):.1f}" r="3.5" style="fill:{cor}"/>')
        p.append(f'<text x="{X(meses[-1]) + 8:.1f}" y="{Y(serie[-1]) + 4:.1f}" {LBL} style="fill:{cor}" '
                 f'font-weight="600">{rot} · {fmt(serie[-1])}</text>')
    p.append("</svg>")
    return "".join(p)


def svg_quadro_vazio(grupos):
    """Moldura do gráfico de uplift por janela: barras tracejadas, sem valor, com o aviso."""
    W, H, L, B, T = 760, 240, 40, 34, 16
    larg = (W - L - 10) / len(grupos)
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Gráfico de uplift por janela, aguardando desfecho">']
    p.append(f'<line x1="{L}" y1="{H - B}" x2="{W - 10}" y2="{H - B}" style="stroke:var(--line)"/>')
    alturas = [0.45, 0.6, 0.38, 0.52, 0.3, 0.44]
    for i, g in enumerate(grupos):
        cx = L + larg * i + larg / 2
        for k, cor in enumerate(("var(--nav)", "var(--ctrl)")):
            h = (H - T - B) * alturas[(2 * i + k) % len(alturas)]
            x = cx - 30 + k * 32
            p.append(f'<rect x="{x:.1f}" y="{H - B - h:.1f}" width="28" height="{h:.1f}" rx="3" '
                     f'style="fill:none;stroke:{cor};stroke-width:1.5;stroke-dasharray:4 3"/>')
        p.append(f'<text x="{cx:.1f}" y="{H - 12}" text-anchor="middle" {TXT}>{g}</text>')
    p.append(f'<rect x="{W / 2 - 150}" y="{H / 2 - 30}" width="300" height="40" rx="8" '
             f'style="fill:var(--surface);stroke:var(--line)"/>')
    p.append(f'<text x="{W / 2}" y="{H / 2 - 5}" text-anchor="middle" {LBL} font-weight="600">'
             f"aguardando desfecho — sem valores</text>")
    p.append("</svg>")
    return "".join(p)

# População da onda 1 em PRD, 12–16/10/2026 (relatório da onda, versão ajustada)
DIAS_UTEIS = 5
RANDOMIZADOS = 256
CONTROLE = 11
NAVEGACAO = 245
ELEG_DIA = RANDOMIZADOS / DIAS_UTEIS
MES = 21

EXTRA = """
.draft{background:var(--ctrl-soft);color:var(--ctrl);font:600 12px/1 var(--f-m);padding:6px 10px;border-radius:999px;
letter-spacing:.06em;text-transform:uppercase;align-self:flex-start}
.formula{background:var(--surface);border:1px solid var(--line);border-left:4px solid var(--accent);border-radius:0 8px 8px 0;
padding:16px 18px;font:500 16px/1.5 var(--f-b)}
.formula code{font:600 15px var(--f-m)}
.ph{color:var(--muted);font-style:italic}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:16px;display:flex;flex-direction:column;gap:8px;min-width:0}
.card h3{font:600 16px/1.25 var(--f-d);margin:0}
.chk{list-style:none;padding:0}
.chk li{display:flex;gap:10px}
.st{font:600 11.5px/1 var(--f-m);padding:4px 8px;border-radius:999px;white-space:nowrap;align-self:flex-start}
.st.ok{background:var(--ok-soft);color:var(--ok)}.st.pend{background:var(--ctrl-soft);color:var(--ctrl)}
"""


def tabela(cab, linhas, classe_num=None, minw=640):
    classe_num = classe_num or set()
    h = [f'<div class="tbl"><table style="min-width:{minw}px"><thead><tr>']
    h += [f'<th{" class=\"n\"" if i in classe_num else ""}>{c}</th>' for i, c in enumerate(cab)]
    h += ["</tr></thead><tbody>"]
    for ln in linhas:
        cls = ' class="total"' if ln and str(ln[0]).startswith("Onda") else ""
        h.append(f"<tr{cls}>" + "".join(f'<td{" class=\"n\"" if i in classe_num else ""}>{c}</td>'
                                         for i, c in enumerate(ln)) + "</tr>")
    h += ["</tbody></table></div>"]
    return "".join(h)


def main():
    ph = '<span class="ph">aguardando desfecho</span>'
    ctrl_mes_5 = ELEG_DIA * MES * 0.05
    ctrl_mes_10 = ELEG_DIA * MES * 0.10
    proj = [[f"mês {m}", fmt(ctrl_mes_5 * m), fmt(ctrl_mes_10 * m)] for m in (1, 3, 6, 9, 12)]

    H = ["<title>Uplift da captação — esboço</title>",
         '<meta name="claude:viewport-fit-sides" content="cover">',
         '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Condensed:wght@600;700'
         '&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&display=swap">',
         f"<style>{CSS}{EXTRA}</style>", '<div class="wrap">', "<header>",
         '<span class="draft">Esboço · sem desfecho ainda</span>',
         '<span class="eyebrow">Fábrica de IA · Captação · Fase 3 — uplift</span>',
         "<h1>Uplift da captação — relatório por onda</h1>",
         '<span class="meta">Estrutura do relatório mensal. Os campos de resultado ficam vazios até o consumo com '
         "janela completa estar disponível; a primeira leitura de 30 dias da onda 1 é possível a partir de meados de "
         "novembro.</span>", "</header>"]

    # 1. Pergunta e definição
    H += ["<section><h2>1. O que se mede</h2>",
          '<div class="formula"><strong>Uplift</strong> = consumo per capita do grupo <strong>navegação</strong> − '
          "consumo per capita do grupo <strong>controle</strong>, no mesmo período e na mesma janela.</div>",
          '<div class="grid2" style="margin-top:14px">',
          '<div class="card"><h3>Por intenção de tratar</h3><p>Compara-se o grupo <strong>sorteado</strong>, não quem a '
          "navegadora conseguiu contatar. É o que mantém os grupos comparáveis.</p></div>",
          '<div class="card"><h3>Janelas pré-registradas</h3><p>30, 90 e 180 dias depois do sorteio. Paciente cuja janela '
          "ainda não fechou não entra na conta daquela janela.</p></div>",
          '<div class="card"><h3>Total e específico</h3><p><strong>Total</strong>: todo consumo na Rede. '
          "<strong>Específico</strong>: consumo nos procedimentos da linha de cuidado.</p></div>",
          '<div class="card"><h3>Onda agregada</h3><p>A conclusão é da onda. A abertura por linha é descritiva: '
          "por linha, o número de controles só sustenta efeitos grandes.</p></div>",
          "</div></section>"]

    # 2. População
    H += ["<section><h2>2. População do estudo — onda 1</h2>",
          '<div class="kpis">',
          f'<div class="kpi"><span class="k">Sorteados</span><span class="v">{fmt(RANDOMIZADOS)}</span>'
          f'<span class="s">PRD, 12–16/10</span></div>',
          f'<div class="kpi nav"><span class="k">Navegação</span><span class="v">{fmt(NAVEGACAO)}</span></div>',
          f'<div class="kpi ctrl"><span class="k">Controle</span><span class="v">{fmt(CONTROLE)}</span></div>',
          f'<div class="kpi"><span class="k">Controle/mês (estimado)</span><span class="v">{fmt(ctrl_mes_5)}</span>'
          f'<span class="s">a 5% de fração</span></div>', "</div>",
          '<p class="note">Entram no estudo os pacientes sorteados em unidades do escopo de navegação. O escopo atual vale '
          "desde 14/10; os sorteados em 12 e 13/10 em unidades fora dele ficam fora da análise principal.</p></section>"]

    # 3. Resultado (vazio)
    linhas_res = [["Reumatologia", ph, ph, ph, ph, ph, ph],
                  ["Tireoide (TI-RADS)", ph, ph, ph, ph, ph, ph],
                  ["Onda 1", ph, ph, ph, ph, ph, ph]]
    H += ["<section><h2>3. Resultado por janela</h2>",
          legenda(("consumo per capita — navegação", "var(--nav)"), ("consumo per capita — controle", "var(--ctrl)")),
          '<div class="tbl" style="padding:12px 14px;margin:8px 0 12px">' +
          svg_quadro_vazio(["30 dias", "90 dias", "180 dias"]) + "</div>",
          tabela(["Linha", "Conversão — navegação", "Conversão — controle", "Per capita — navegação",
                  "Per capita — controle", "Uplift (IC 95%)", "Probabilidade de ganho"], linhas_res, minw=900),
          '<p class="note">Uma tabela por janela (30, 90, 180 dias) e por tipo de consumo (total, específico). '
          "<strong>Conversão</strong> = pacientes com algum consumo na janela ÷ pacientes do grupo. "
          "<strong>Probabilidade de ganho</strong> = chance de o uplift ser maior que zero.</p></section>"]

    # 4. Equilíbrio
    H += ["<section><h2>4. Os grupos são comparáveis?</h2>",
          tabela(["Característica (antes do sorteio)", "Navegação", "Controle", "Diferença padronizada"],
                 [["Consumo per capita nos 180 dias anteriores", ph, ph, ph],
                  ["Idade média", ph, ph, ph], ["% sexo feminino", ph, ph, ph],
                  ["Distribuição por regional", ph, ph, ph]], minw=640),
          '<p class="note">O sorteio garante equilíbrio em média, mas com poucos controles as diferenças por acaso são '
          "grandes. O uplift é ajustado pelo consumo anterior ao sorteio, o que reduz esse ruído.</p></section>"]

    # 5. Poder e horizonte
    meses = list(range(1, 13))
    H += ["<section><h2>5. Quando o resultado fica legível</h2>",
          '<div class="tbl" style="padding:12px 14px;margin-bottom:12px">' +
          svg_acumulo(meses, [ctrl_mes_5 * m for m in meses], [ctrl_mes_10 * m for m in meses]) + "</div>",
          tabela(["Acumulado", "Controles a 5%", "Controles a 10%"], proj, classe_num={1, 2}, minw=420),
          '<p class="note">Projeção da onda 1 sozinha, pelo volume de 12–16/10 (cerca de '
          f"{fmt(ELEG_DIA, 0)} elegíveis por dia útil). Com 5%, o mesmo número de controles leva o dobro do tempo que "
          "com 10%. As ondas 2 e 3 somam controles a partir de 17/11 e 24/11 e encurtam esse prazo. O efeito mínimo "
          "detectável em valor absoluto depende da variabilidade do desfecho, que só se conhece com os dados de consumo.</p>"
          "</section>"]

    # 6. Ressalvas
    H += ["<section><h2>6. O que pode distorcer a leitura</h2><ul>",
          "<li><strong>Contaminação:</strong> paciente de controle que também é relevante em linha ainda não randomizada "
          "pode ser navegado por ela. Reportado com e sem esses pacientes.</li>",
          "<li><strong>Unidades sem navegadora:</strong> fora do escopo, o efeito é zero por construção. A análise usa só "
          "unidades do escopo vigente.</li>",
          "<li><strong>Atraso da fonte de consumo:</strong> cerca de duas semanas. Só entra quem tem a janela inteira "
          "observada.</li>",
          "<li><strong>Paciente sem CPF:</strong> vai para a navegação e fica fora da análise.</li>",
          "</ul></section>"]

    # 7. Pré-condições
    pre = [("ok", "Linhas da onda em produção, randomizadas", "onda 1 desde 12/10"),
           ("pend", "De acordo do negócio", "previsto para 23/10"),
           ("pend", "Desfecho declarado por escrito (total, específico) e janelas registradas", "a definir com o negócio"),
           ("ok", "Chave de cruzamento com o consumo", "CPF; 85,5% dos sorteados casam com a base de consumo"),
           ("ok", "Escopo de unidades com navegação", "103 unidades, vigente desde 14/10"),
           ("pend", "Janela de 30 dias completa na fonte de consumo", "a partir de meados de novembro")]
    H += ["<section><h2>7. Pré-condições da primeira leitura</h2>", '<ul class="chk">']
    for st, txt, det in pre:
        H.append(f'<li><span class="st {st}">{"OK" if st == "ok" else "PENDENTE"}</span>'
                 f"<span><strong>{txt}</strong> — {det}</span></li>")
    H += ["</ul></section>",
          f'<footer>Esboço gerado em {dt.date.today().isoformat()} por gerar_esboco_uplift.py · números agregados · '
          "nenhum resultado de uplift calculado.</footer>", "</div>"]

    destino = Path(__file__).parent.parent / "relatorios" / "esboco-relatorio-uplift-v0.html"
    destino.write_text("\n".join(H), encoding="utf-8")
    print(destino.name)


if __name__ == "__main__":
    main()
