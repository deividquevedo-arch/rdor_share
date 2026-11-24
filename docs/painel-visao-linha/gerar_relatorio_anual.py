"""Relatório do ano (jan–out/2026, 16 linhas) — agenda KL 24/11. Planilha + HTML (para PDF), no formato do one page v18.

Uso: python gerar_relatorio_anual.py <pasta_saida> <versao>
Lê historico/relatorio-mensal-dados.json (montar_relatorio_mensal.py). Só agregados: nenhum dado de paciente.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from gerar_onepage import br
from gerar_onepage_v6 import CSS, aprox, faixa

AQUI = Path(__file__).parent
MESES = [f"2026-{i:02d}" for i in range(1, 11)]
NOME_MES = dict(zip(MESES, ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out"]))
CURTO = {"mama": "Mama", "hepatologia": "Fígado", "nefrologia": "Nefro", "pulmao_nodulo": "Nódulo pulm.",
         "cancer_colon": "Ca Cólon", "dii": "DII", "reumatologia": "Reumato", "doencas_biliares": "Biliares",
         "neuroimunologia": "Neuro", "pirads": "Próstata", "ateromatose": "Ateroma", "tirads": "Tireoide",
         "cancer_estomago": "Estômago", "transplante_pulmao": "Tx pulmão", "cancer_rim": "Ca Rim", "tumor_osseo": "TU ósseo"}
FONTE = {"legado_motor": "legado", "plataforma_prd": "plataforma", "legado_pacs_enave": "captação manual (PACS)",
         "monitoramento_job_legado": "legado (contagem do job)", None: "—"}


def reais(v):
    if v is None:
        return "—"
    return f"R$ {v / 1e6:.1f} mi".replace(".", ",") if v >= 1e6 else (f"R$ {v / 1e3:.0f} mil" if v else "R$ 0")


def carregar():
    d = json.loads((AQUI / "historico" / "relatorio-mensal-dados.json").read_text(encoding="utf-8"))
    linhas = list(dict.fromkeys(f["linha"] for f in d["funil"]))
    return d, linhas


def consolidado_mes(d):
    out = []
    for m in MESES:
        fs = [f for f in d["funil"] if f["mes"] == m]
        hs = [h for h in d["hospitais"] if h["mes"] == m]
        out.append({"mes": m, "ativas": d["meta"]["oficial"]["em_producao"].get(m),
                    **{c: sum(f[c] for f in fs) for c in ("exames", "relevantes")},
                    "enviados": sum(f["enviados_oficial"] or 0 for f in fs),
                    "captados": sum(f["captados"] or 0 for f in fs), "consumo": sum(f["consumo"] or 0 for f in fs),
                    "h_exame": len({h["hospital"] for h in hs if h["exames"]}),
                    "h_relev": len({h["hospital"] for h in hs if h["relevantes_execucao"]}),
                    "h_envio": len({h["hospital"] for h in hs if h["enviados"]})})
    return out


def funil_linha(d, linhas):
    out = []
    for l in linhas:
        fs = [f for f in d["funil"] if f["linha"] == l]
        hs = [h for h in d["hospitais"] if h["linha"] == l]
        ativos = [f["mes"] for f in fs if f["ativa"]]
        sp = fs[0]["subida_producao"]
        out.append({"linha": l, "rotulo": fs[0]["rotulo"], "desde": f"{sp[8:10]}/{sp[5:7]}/{sp[:4]}",
                    **{c: sum(f[c] for f in fs) for c in ("exames", "relevantes")},
                    "enviados": sum(f["enviados_oficial"] or 0 for f in fs),
                    "env_nd": [NOME_MES[f["mes"]] for f in fs if f["enviados_nd"]],
                    "captados": sum(f["captados"] or 0 for f in fs) if fs[0]["captados"] is not None or any(
                        f["captados"] for f in fs) else None,
                    "consumo": sum(f["consumo"] or 0 for f in fs) if any(f["consumo"] is not None for f in fs) else None,
                    "h_exame": len({h["hospital"] for h in hs if h["exames"]}),
                    "h_relev": len({h["hospital"] for h in hs if h["relevantes_execucao"]}),
                    "h_envio": len({h["hospital"] for h in hs if h["enviados"]})})
    return out


def notas_agrupadas(d, linhas):
    """Por linha, meses consecutivos com a mesma nota viram um intervalo."""
    out = []
    for l in linhas:
        fs = [f for f in d["funil"] if f["linha"] == l and f["nota"]]
        grupos = []
        for f in fs:
            if grupos and grupos[-1]["nota"] == f["nota"] and MESES.index(f["mes"]) == MESES.index(grupos[-1]["fim"]) + 1:
                grupos[-1]["fim"] = f["mes"]
            else:
                grupos.append({"linha": l, "rotulo": f["rotulo"], "ini": f["mes"], "fim": f["mes"], "nota": f["nota"],
                               "fonte": f["fonte_nota"]})
        out += grupos
    return out


def periodo(g):
    return NOME_MES[g["ini"]] if g["ini"] == g["fim"] else f"{NOME_MES[g['ini']]}–{NOME_MES[g['fim']]}"


def planilha(d, linhas, cons, fun, notas, destino: Path):
    wb = Workbook()
    lm = wb.active
    lm.title = "Leia-me"
    meta = d["meta"]
    for a, b in [
        ("Relatório do ano — captação por linha de cuidado (jan–out/2026)", ""),
        ("Período", "01/01/2026 a 31/10/2026; mês = mês em que o algoritmo rodou (exames processados, relevantes marcados e "
                    "enviados no mesmo dia), somando todos os algoritmos que operaram no mês (legado até parar, plataforma depois "
                    "de subir); Fígado só legado"),
        ("Hospitais", f"as {meta['planilha_80']} unidades da aba 'Unidades IAs' (VP e Estado de lá); "
                      f"sem cadastro no repositório: {', '.join(meta['sem_cadastro'])}"),
        ("Totais", "exames, relevantes e enviados por linha = total da rede (o enviado oficial também é da rede inteira); "
                   "abertura e contagem de hospitais = 80 unidades da planilha"),
        ("Fonte de cada mês", "a que operou a linha naquele mês: algoritmo legado, plataforma ou captação manual no PACS"),
        ("Hospital x linha x mês", "mesma base de data em todas as colunas; enviado contido em relevante, e a diferença é "
                                   "unidade não navegada naquele momento. Relevantes da Nefro por hospital: recontagem com a régua "
                                   "atual do job. Enviados: só registro real (plataforma antes de 23/10 sem registro)"),
        ("Relevante sem registro", "Pulmão (jan) e Doenças biliares (mar): o dado de relevante está "
                                   "incompleto na origem; o enviado oficial é mantido"),
        ("Em produção", "contagem de linhas em produção e datas de subida oficiais (gestão de captação); antes da subida, entrada e "
                        "relevantes vêm de processamento retroativo ou de captação manual no PACS"),
        ("Enviados", "oficiais: planilha 'Portfólio Forum de IAs RDOR', aba 'Envios IAs Captação' (linhas com envio por e-mail: contagem "
                     "do enviado no dia; Mama, Nefro e Nódulo pulmonar com fluxo próprio). Nefro: enviado = clearance <= 35 (filtro do "
                     "Salesforce); relevantes da Nefro = o mesmo número. Hospital com envio: registro real de envio: legado, contagem do job no dia; Nefro, relevantes registrados pelo job no dia (sem registro confiável em fev–mar); Mama, integrados na API das navegadoras; plataforma, log de envio desde 23/10 (antes, sem registro)"),
        ("Captados e consumo", f"BI de controle da captação (fonte oficial), por linha (captados até {meta['captados_ate']}, consumo até "
                               f"{meta['consumo_ate']}); incluem todas as origens de captação e não trazem o hospital. "
                               "Sem correspondência no BI: Fígado, Nefro, Nódulo pulmonar, Transplante de pulmão, Tumor ósseo"),
        ("Premissa", "somente dados consultados nas bases; nenhum valor estimado ou reconstruído"),
    ]:
        lm.append([a, b])
    lm.column_dimensions["A"].width = 24
    lm.column_dimensions["B"].width = 130
    lm["A1"].font = Font(bold=True, size=13)

    def aba(nome, cab, rows, larg=None, congela="B2"):
        ws = wb.create_sheet(nome)
        ws.append(cab)
        for c in ws[1]:
            c.font = Font(bold=True)
            c.fill = PatternFill("solid", fgColor="DDEBF7")
            c.alignment = Alignment(wrap_text=True, vertical="top")
        for r in rows:
            ws.append(r)
        for i, c in enumerate(cab, 1):
            ws.column_dimensions[ws.cell(1, i).column_letter].width = (larg or {}).get(c, max(11, min(40, len(c) + 3)))
        ws.freeze_panes = congela
        ws.auto_filter.ref = ws.dimensions

    aba("Consolidado mensal", ["Mês", "Linhas em produção", "Exames (entrada)", "Relevantes", "Enviados", "Captados", "Consumo (R$)",
                               "Hospitais c/ entrada", "Hospitais c/ relevantes", "Hospitais c/ envios"],
        [[NOME_MES[c["mes"]], c["ativas"], c["exames"], c["relevantes"], c["enviados"], c["captados"], round(c["consumo"], 2),
          c["h_exame"], c["h_relev"], c["h_envio"]] for c in cons])
    aba("Funil por linha (jan-out)", ["Linha", "Em produção desde", "Exames (entrada)", "Relevantes", "Enviados", "Captados",
                                      "Consumo (R$)", "Hospitais c/ entrada", "Hospitais c/ relevantes", "Hospitais c/ envios"],
        [[f["rotulo"], f["desde"], f["exames"], f["relevantes"], f["enviados"], f["captados"],
          None if f["consumo"] is None else round(f["consumo"], 2), f["h_exame"], f["h_relev"], f["h_envio"]] for f in fun],
        {"Linha": 24})
    aba("Funil linha x mês", ["Linha", "Mês", "Em produção", "Fonte", "Exames", "Relevantes", "Enviados", "Captados",
                              "Consumo (R$)", "Hospitais c/ entrada", "Hospitais c/ relevantes", "Hospitais c/ envios"],
        [[f["rotulo"], NOME_MES[f["mes"]], "sim" if f["ativa"] else "não", FONTE.get(f["fonte"], f["fonte"]),
          f["exames"], "sem registro" if f["relevantes_sem_registro"] else f["relevantes"], f["enviados_oficial"], f["captados"], f["consumo"],
          f["hosp_exame"] if f["hospital_disponivel"] else None, f["hosp_relevante"] if f["hospital_disponivel"] else None,
          (f["hosp_envio"] if f["hospital_disponivel"] and f["hosp_envio_disponivel"] else "sem registro")] for f in d["funil"]],
        {"Linha": 22}, "C2")
    aba("Hospital x linha x mês", ["VP", "Estado", "Unidade", "Linha", "Mês", "Exames", "Relevantes", "Enviados (registro real)"],
        [[h["vp"], h["estado"], h["hospital"], h["rotulo"], NOME_MES[h["mes"]], h["exames"], h["relevantes"], h["enviados"]]
         for h in sorted(d["hospitais"], key=lambda h: (h["estado"], h["hospital"], linhas.index(h["linha"]), h["mes"]))],
        {"Unidade": 46, "Linha": 22}, "D2")
    acum = defaultdict(lambda: defaultdict(int))
    unid = {}
    for h in d["hospitais"]:
        unid[h["hospital"]] = (h["vp"], h["estado"])
        for c in ("exames", "relevantes", "enviados"):
            acum[(h["hospital"], h["linha"])][c] += h[c]
    ordem = sorted(unid, key=lambda u: (unid[u][1], u))
    for campo, nome in (("exames", "Unid x linhas - entrada"), ("relevantes", "Unid x linhas - relevantes"),
                        ("enviados", "Unid x linhas - enviados")):
        aba(nome, ["VP", "Estado", "Unidade"] + [CURTO[l] for l in linhas],
            [[unid[u][0], unid[u][1], u] + [acum[(u, l)][campo] for l in linhas] for u in ordem], {"Unidade": 46}, "D2")
    assert not destino.exists(), destino
    wb.save(destino)


def html(d, linhas, cons, fun, notas, destino: Path):
    meta = d["meta"]
    tot = {c: sum(x[c] for x in cons) for c in ("exames", "relevantes", "enviados", "captados", "consumo")}
    por = {(f["linha"], f["mes"]): f for f in d["funil"]}

    def pct(n):
        return f"{n}/80 <span class=pc>({round(100 * n / 80)}%)</span>"

    t_cons = "".join(
        f"<tr><td>{NOME_MES[c['mes']]}</td><td class=n>{c['ativas']}</td><td class=n>{br(c['exames'])}</td>"
        f"<td class=n>{br(c['relevantes'])}</td><td class=n><b>{br(c['enviados'])}</b></td><td class='n g'>{br(c['captados'])}</td>"
        f"<td class=n>{reais(c['consumo'])}</td><td class='n g'>{c['h_exame']}</td><td class=n>{c['h_relev']}</td>"
        f"<td class=n>{c['h_envio']}</td></tr>" for c in cons)
    t_fun = "".join(
        f"<tr><td>{f['rotulo']}</td><td>{f['desde']}</td><td class=n>{br(f['exames'])}</td><td class=n>{br(f['relevantes'])}</td>"
        f"<td class=n><b>{br(f['enviados'])}</b></td><td class='n g'>{'—' if f['captados'] is None else br(f['captados'])}</td>"
        f"<td class=n>{reais(f['consumo'])}</td><td class='n g'>{pct(f['h_exame'])}</td><td class=n>{pct(f['h_relev'])}</td>"
        f"<td class=n>{pct(f['h_envio']) if f['h_envio'] is not None else 'n/d'}</td></tr>" for f in fun)

    fixas = {g["linha"] for g in notas if g["ini"] == MESES[0] and g["fim"] == MESES[-1]}

    def matriz(campo, titulo):
        cab = "".join(f"<th class=n>{NOME_MES[m]}</th>" for m in MESES)
        corpo = []
        for l in linhas:
            cels = []
            for m in MESES:
                f = por[(l, m)]
                v = f["enviados_oficial"] if campo == "enviados_80" else f[campo]
                if campo == "relevantes" and f["relevantes_sem_registro"]:
                    cels.append("<td class='n ina'>s/ reg.</td>")
                    continue
                marca = ""  # notas ficam no documento interno separado
                if campo.startswith("hosp") and not f["hospital_disponivel"]:
                    txt = "n/d"
                elif campo == "enviados_80" and f["enviados_nd"]:
                    txt = "n/d"
                elif not f["fonte"] and not v:
                    txt = ""
                else:
                    txt = br(v) if v else "0"
                if not txt:
                    marca = ""
                cls = "n" + ("" if f["ativa"] else " ina")
                cels.append(f"<td class='{cls}'>{txt}{marca}</td>")
            corpo.append(f"<tr><td>{CURTO[l]}</td>{''.join(cels)}</tr>")
        return f"<h2>{titulo}</h2><table class=mt><tr><th>Linha</th>{cab}</tr>{''.join(corpo)}</table>"

    t_notas = "".join(f"<tr><td>{g['rotulo']}</td><td>{periodo(g)}</td><td>{g['nota']}</td><td class=f>{g['fonte']}</td></tr>"
                      for g in notas)
    t_gerais = "".join(f"<li><b>{n['tema']}:</b> {n['nota']}</li>" for n in meta["notas_gerais"] if n["tema"] != "Enviados")

    acum = defaultdict(lambda: defaultdict(int))
    unid = {}
    for h in d["hospitais"]:
        unid[h["hospital"]] = (h["vp"], h["estado"])
        for c in ("exames", "relevantes", "enviados"):
            acum[(h["hospital"], h["linha"])][c] += h[c]
    ordem = sorted(unid, key=lambda u: (unid[u][1], u))

    def bloco(ls):
        cab1 = "".join(f"<th colspan=3 class=g>{CURTO[l]}</th>" for l in ls)
        cab2 = "".join("<th class=g1>entr.</th><th>relev.</th><th>env.</th>" for _ in ls)

        def cel(u, l):
            e, r, v = (acum[(u, l)][c] for c in ("exames", "relevantes", "enviados"))
            return (f"<td class='{faixa(e)} g1'>{aprox(e) or '—'}</td><td class={'v0' if not r else ''}>{r or '—'}</td>"
                    f"<td class={'v0' if not v else 'b'}>{v or '—'}</td>")
        corpo = "".join(f"<tr><td class=e>{unid[u][0]}</td><td class=e>{unid[u][1]}</td><td class=u>{u.title()}</td>"
                        + "".join(cel(u, l) for l in ls) + "</tr>" for u in ordem)
        return (f"<table class=mx><colgroup><col style='width:5%'><col style='width:2.6%'><col style='width:18%'>"
                f"{''.join('<col>' for _ in range(3 * len(ls)))}</colgroup>"
                f"<tr><th rowspan=2>VP</th><th rowspan=2>UF</th><th rowspan=2 style='text-align:left'>Unidade</th>{cab1}</tr>"
                f"<tr>{cab2}</tr>{corpo}</table>")
    partes = [linhas[i:i + 6] for i in range(0, len(linhas), 6)]
    tit = ("<h1>Hospitais × linhas — jan a out/2026 (parte {} de {})</h1><div class=sub>Unidades da planilha de portfólio com "
           "algum exame no período · entr. = exames na entrada do algoritmo (aproximado; cor: &lt; 100 · 100–999 · 1 mil ou "
           "mais) · relev. = relevantes · env. = envios por hospital, só registro real (job do legado no dia; log da plataforma desde 23/10; Mama: API das navegadoras); total oficial por linha na página 1 · — = zero · valores exatos e mês a mês na planilha anexa</div>")
    micro = "".join(f"<div class=quebra></div>{tit.format(i + 1, len(partes))}{bloco(p)}" for i, p in enumerate(partes))

    pagina = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Captação por linha 2026</title>
<style>{CSS}
.pc {{ color: #8a959f; font-size: 8.4px; }}
table.mt td, table.mt th {{ padding: 1.6px 4px; font-size: 8.6px; }}
td.ina {{ color: #9aa5ae; background: #f6f7f8; }}
td.f {{ color: #5d6b76; font-size: 8px; }}
sup {{ color: #0b7a80; font-weight: bold; }}
sup.p {{ color: #b26b00; }}
ul.gerais {{ margin: 2px 0 0 12px; padding: 0; line-height: 1.25; }}
.p1 td {{ padding: 1.3px 4px; font-size: 8.7px; }} .p1 th {{ padding: 2px 4px; font-size: 8.7px; }}
.p1 .kpis {{ margin-bottom: 4px; }} .p1 .kpi {{ padding: 3px 8px; }} .p1 .kpi b {{ font-size: 14px; }}
.p1 h2 {{ margin: 3px 0 2px; font-size: 11px; }} .p1 .nota {{ font-size: 7.9px; margin-top: 3px; }}</style></head><body><div class=p1>
<h1>Captação por linha de cuidado — janeiro a outubro de 2026</h1>
<div class="sub">16 linhas (algoritmos legados e plataforma) · mês = mês em que o algoritmo rodou · totais da rede; hospitais: as 80 unidades da
planilha de portfólio (aba "Unidades IAs") · somente dados consultados nas bases, sem estimativa · ciência de dados</div>
<div class="kpis">
 <div class="kpi"><b>{br(tot['exames'])}</b>exames na entrada</div>
 <div class="kpi"><b>{br(tot['relevantes'])}</b>relevantes encontrados</div>
 <div class="kpi"><b>{br(tot['enviados'])}</b>enviados à navegação</div>
 <div class="kpi"><b>{br(tot['captados'])}</b>pacientes captados</div>
 <div class="kpi"><b>{reais(tot['consumo'])}</b>consumo</div>
</div>
<h2>Consolidado mês a mês</h2>
<table><tr><th>Mês</th><th class=n>Linhas em produção</th><th class=n>Exames (entrada)</th><th class=n>Relevantes</th>
<th class=n>Enviados</th><th class='n g'>Captados</th><th class=n>Consumo</th><th class='n g'>Hospitais c/ entrada</th>
<th class=n>c/ relevantes</th><th class=n>c/ envios</th></tr>{t_cons}</table>
<h2>Funil por linha de cuidado — acumulado jan a out</h2>
<table><tr><th>Linha</th><th>Em produção desde</th><th class=n>Exames (entrada)</th><th class=n>Relevantes</th><th class=n>Enviados</th>
<th class='n g'>Captados</th><th class=n>Consumo</th><th class='n g'>Hospitais c/ entrada</th><th class=n>c/ relevantes</th>
<th class=n>c/ envios</th></tr>{t_fun}</table>
<div class="nota"><b>Leitura.</b><ul class=gerais>
<li><b>Exames:</b> entrada do algoritmo, depois do filtro de cada linha (não é o volume total do hospital); um exame pode entrar em mais
de uma linha.</li>
<li><b>Enviados:</b> oficiais, planilha do portfólio (aba "Envios IAs Captação"): envio por e-mail = contagem do enviado no dia; Mama,
Nefro e Nódulo pulmonar com fluxo próprio. Nefro: enviado = relevante = clearance ≤ 35 (filtro do Salesforce). Hospitais com envio:
só registro real (job do legado no dia, log da plataforma desde 23/10, API das navegadoras na Mama); plataforma antes de 23/10 sem registro.</li>
<li><b>Base:</b> exames processados, relevantes marcados e enviados no mês em que o algoritmo rodou, somando legado e plataforma no
mês da troca; o enviado sai dos relevantes. Linhas em produção e datas de subida oficiais. Relevante sem registro na origem: Pulmão
(jan) e Biliares (mar). Transplante em agosto: envio saiu do ambiente de HML (18 a 30/08), somado ao mês.</li>
{t_gerais}
<li><b>Captados e consumo:</b> BI de controle da captação (fonte oficial), por linha (captados até {meta['captados_ate'][8:10]}/{meta['captados_ate'][5:7]};
consumo até {meta['consumo_ate'][8:10]}/{meta['consumo_ate'][5:7]}). Incluem todas as origens de captação e não trazem o hospital;
consumo gerado, não uplift. Sem correspondência no BI: Fígado, Nefro, Nódulo pulmonar, Transplante de pulmão e Tumor ósseo (—).</li>
</ul></div></div>
<div class=quebra></div>
<h1>Mês a mês por linha</h1>
<div class=sub>Base: 80 unidades · cinza = linha ainda sem algoritmo no ar · n/d = sem abertura por hospital · s/ reg. = relevante incompleto na origem</div>
{matriz('exames', 'Exames na entrada')}
{matriz('relevantes', 'Relevantes')}
<div class=quebra></div>
{matriz('enviados_80', 'Enviados à navegação')}
{matriz('hosp_exame', 'Hospitais com exame (de 80)')}
{micro}
</body></html>"""
    assert not destino.exists(), destino
    destino.write_text(pagina, encoding="utf-8")


def documento_notas(d, notas, out: Path, versao):
    """Documento INTERNO (não vai ao negócio): desvios, lacunas e outliers com a fonte de cada um."""
    meta = d["meta"]
    base = f"notas-internas-captacao-jan-out-2026-{versao}"
    wb = Workbook()
    ws = wb.active
    ws.title = "Notas por linha e mês"
    for r in [["Linha", "Período", "Nota", "Fonte"]] + [[g["rotulo"], periodo(g), g["nota"], g["fonte"]] for g in notas]:
        ws.append(r)
    ws2 = wb.create_sheet("Notas gerais")
    for r in [["Tema", "Nota", "Fonte"]] + [[n["tema"], n["nota"], n["fonte"]] for n in meta["notas_gerais"]]:
        ws2.append(r)
    ws3 = wb.create_sheet("Unidades fora das 80")
    for r in [["Código da unidade", "Nome no cadastro da gold", "Exames (jan–out)", "Linhas"]] + [
            [u["id_unidade"], u["nome"], u["exames"], u["linhas"]] for u in meta.get("fora_80", [])]:
        ws3.append(r)
    for w, larg in ((ws, {"A": 22, "B": 10, "C": 95, "D": 80}), (ws2, {"A": 14, "B": 100, "C": 80}),
                    (ws3, {"A": 16, "B": 50, "C": 16, "D": 60})):
        for c in w[1]:
            c.font = Font(bold=True)
            c.fill = PatternFill("solid", fgColor="DDEBF7")
        for col, v in larg.items():
            w.column_dimensions[col].width = v
        w.freeze_panes = "A2"
    assert not (out / f"{base}.xlsx").exists()
    wb.save(out / f"{base}.xlsx")
    t_notas = "".join(f"<tr><td>{g['rotulo']}</td><td>{periodo(g)}</td><td>{g['nota']}</td><td class=f>{g['fonte']}</td></tr>"
                      for g in notas)
    t_gerais = "".join(f"<tr><td>{n['tema']}</td><td>{n['nota']}</td><td class=f>{n['fonte']}</td></tr>" for n in meta["notas_gerais"])
    t_fora = "".join(f"<tr><td>{u['id_unidade']}</td><td>{u['nome'] or '(sem nome no cadastro)'}</td><td class=n>{br(u['exames'])}</td>"
                     f"<td class=f>{u['linhas']}</td></tr>" for u in meta.get("fora_80", [])[:30])
    pagina = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Notas internas captação 2026</title>
<style>{CSS}
td.f {{ color: #5d6b76; font-size: 8px; }} td {{ vertical-align: top; }}</style></head><body>
<h1>Notas internas — captação por linha, jan a out/2026</h1>
<div class=sub>Uso interno do time de ciência de dados. Acompanha o relatório do ano ({versao}): explica cada desvio, lacuna e outlier
observado nos dados, com a fonte (commit, card, documento ou execução). Não acompanha o material enviado ao negócio.</div>
<h2>Notas gerais</h2>
<table><tr><th>Tema</th><th>Nota</th><th>Fonte</th></tr>{t_gerais}</table>
<h2>Por linha e mês</h2>
<table><tr><th>Linha</th><th>Mês</th><th>Nota</th><th>Fonte</th></tr>{t_notas}</table>
<h2>Unidades fora das 80 da planilha (não entram no relatório)</h2>
<div class=sub>{len(meta.get("fora_80", []))} códigos de unidade, {br(sum(u["exames"] for u in meta.get("fora_80", [])))} exames no período
(lista completa na planilha anexa). Parte não tem nome no cadastro da gold (códigos antigos das fontes do legado).</div>
<table><tr><th>Código</th><th>Nome no cadastro</th><th class=n>Exames</th><th>Linhas</th></tr>{t_fora}</table>
</body></html>"""
    assert not (out / f"{base}.html").exists()
    (out / f"{base}.html").write_text(pagina, encoding="utf-8")


def main(pasta, versao):
    d, linhas = carregar()
    cons, fun, notas = consolidado_mes(d), funil_linha(d, linhas), notas_agrupadas(d, linhas)
    out = Path(pasta)
    out.mkdir(parents=True, exist_ok=True)
    base = f"captacao-por-linha-jan-out-2026-{versao}"
    planilha(d, linhas, cons, fun, notas, out / f"{base}.xlsx")
    html(d, linhas, cons, fun, notas, out / f"{base}.html")
    documento_notas(d, notas, out, versao)
    for c in cons:
        print(c)
    print(len(notas), "notas agrupadas")


if __name__ == "__main__":
    main(*sys.argv[1:3])
