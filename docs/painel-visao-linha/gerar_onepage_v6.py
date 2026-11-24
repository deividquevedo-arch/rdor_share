"""One page "Visão por linha" — v6: base = as 80 unidades da aba "Unidades IAs", sem o status de navegação.

Página 1: funil por linha (exames, relevantes, enviados) e quantas das 80 unidades têm exame; randomização; captados.
Página 2: listão unidades × linhas com o volume de exames (aproximado) na entrada de cada algoritmo.

Uso: python gerar_onepage_v6.py <cruzamento.json> <unidades_ias.json> <randomizacao.json> <captados.json> <pasta> <versão>
Só agregados: nenhum dado de paciente.
"""
import json
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from gerar_onepage import ROT_RAND, br
from gerar_onepage_v4 import ORDEM, ddmm

CURTO = {"Ca de Cólon — colonoscopia": "Cólon colono", "Ca de Cólon — TC e RM": "Cólon TC/RM",
         "DII — colonoscopia": "DII colono", "DII — TC e RM": "DII TC/RM", "Ca de Rim": "Rim",
         "Doenças biliares": "Biliares", "Reumatologia": "Reumato", "Tumor ósseo": "TU ósseo",
         "Tireoide (TI-RADS)": "Tireoide", "Ateromatose": "Ateroma", "Neuroimunologia": "Neuro",
         "Próstata (PI-RADS)": "Próstata", "Ca de Estômago": "Estômago", "Transplante de pulmão": "Tx pulmão"}
TC_COLON = "Ca de Cólon — TC e RM"
LEGADO_TC = {"exames": 48942, "relevantes": 212, "mes": "agosto/2026", "parou": "01/09/2026"}


def aprox(n: int) -> str:
    """Volume 'por alto': vazio = nenhum exame; < 10; dezenas e centenas arredondadas; milhares com 1 casa."""
    if n == 0:
        return ""
    if n < 10:
        return "<10"
    if n < 950:
        return str(round(n, -1) if n < 100 else round(n, -2))
    return f"{n / 1000:.1f} mil".replace(".", ",").replace(",0 mil", " mil")


def montar(cruz: dict, unid: list) -> tuple[list[dict], list[dict]]:
    cab = unid[3]
    linhas_pl = [r for r in unid[4:] if r[4]]
    vp = {r[4]: (r[2], r[3]) for r in linhas_pl}
    rot = dict(ORDEM)
    hosp = [{**h, "vp": vp[h["hospital"]][0], "estado": vp[h["hospital"]][1], "rotulo": rot[h["coluna"]]}
            for h in cruz["hospitais"] if h["coluna"] in rot]
    resumo = []
    for col, r in ORDEM:
        hs = [h for h in hosp if h["coluna"] == col]
        resumo.append({"linha": r, "exames": sum(h["exames"] for h in hs), "relevantes": sum(h["relevantes"] for h in hs),
                       "enviados": sum(h["enviados"] for h in hs), "com_exame": sum(h["exames"] > 0 for h in hs),
                       "com_relevante": sum(h["relevantes"] > 0 for h in hs), "com_envio": sum(h["enviados"] > 0 for h in hs),
                       "unidades": len(hs)})
    assert all(r["unidades"] == len(linhas_pl) for r in resumo), "toda linha tem de ter as 80 unidades"
    # Reumatologia: casos da Central de Captação (envio por API, ligados à inferência), por hospital
    from cruzar_unidades_planilha import ids_por_hospital
    gold = json.loads((Path(__file__).parent / "unidades-gold-2026-11-19.json").read_text(encoding="utf-8"))
    ids = ids_por_hospital([(r[3], r[4]) for r in linhas_pl], gold)
    cod = {"cap:un:jabaquara": "100"}  # a Central grava algumas unidades pelo código da linha; id 100 = Sao Luiz Jabaquara
    central = {}
    for x in json.loads((Path(__file__).parent / "central-reumato-2026-11-18.json").read_text(encoding="utf-8")):
        u = cod.get(x["unidade"], x["unidade"])
        acc = central.setdefault(u, {"casos": 0, "abordados": 0, "captados": 0})
        for c in acc:
            acc[c] += int(x[c])
    for h in hosp:
        if h["rotulo"] == "Reumatologia":
            for c in ("casos", "abordados", "captados"):
                h[f"central_{c}"] = sum(int(central.get(u, {}).get(c, 0)) for u in ids[h["hospital"]])
    sem_mapa = set(central) - {u for v in ids.values() for u in v}
    assert not sem_mapa, f"unidades da Central fora das 80: {sem_mapa}"
    return resumo, hosp


def planilha(resumo, hosp, rand, cruz, destino: Path) -> None:
    wb = Workbook()
    lm = wb.active
    lm.title = "Leia-me"
    for a, b in [
        ("Visão por linha — dados do one page (v6)", ""),
        ("Janela", f"execuções do modelo de {cruz['inicio']} a {cruz['fim']} (30 dias), produção"),
        ("Unidades", "as 80 da aba 'Unidades IAs' da planilha de portfólio (VP e Estado de lá); status de navegação não usado"),
        ("Exames (entrada)", "exames distintos que chegaram ao algoritmo da linha, depois do filtro de entrada"),
        ("Relevantes / Enviados", "marcados como relevantes / constam do envio à navegação (grupo intervenção)"),
        ("Ca de Cólon TC e RM", "em levantamento"),
        ("Sem cadastro no repositório", ", ".join(cruz["sem_cadastro"])),
    ]:
        lm.append([a, b])
    lm.column_dimensions["A"].width = 28
    lm.column_dimensions["B"].width = 130
    lm["A1"].font = Font(bold=True, size=13)

    def aba(nome, cab, linhas, larg=None):
        ws = wb.create_sheet(nome)
        ws.append(cab)
        for c in ws[1]:
            c.font = Font(bold=True)
            c.fill = PatternFill("solid", fgColor="DDEBF7")
            c.alignment = Alignment(wrap_text=True, vertical="top")
        for l in linhas:
            ws.append(l)
        for i, c in enumerate(cab, 1):
            ws.column_dimensions[ws.cell(1, i).column_letter].width = (larg or {}).get(c, max(11, min(46, len(c) + 3)))
        ws.freeze_panes = "D2"
        ws.auto_filter.ref = ws.dimensions

    aba("Resumo por linha", ["Linha", "Exames (entrada)", "Relevantes", "Enviados", "Unidades com exame (de 80)",
                             "Unidades com relevante", "Unidades com envio"],
        [[r["linha"], r["exames"], r["relevantes"], r["enviados"], r["com_exame"], r["com_relevante"], r["com_envio"]]
         for r in resumo])
    unidades = sorted({(h["vp"], h["estado"], h["hospital"]) for h in hosp}, key=lambda t: (t[1], t[2]))
    idx = {(h["hospital"], h["rotulo"]): h for h in hosp}
    for campo, nome in (("exames", "Unid x linhas - entrada"), ("relevantes", "Unid x linhas - relevantes"),
                        ("enviados", "Unid x linhas - enviados")):
        aba(nome, ["VP", "Estado", "Unidade"] + [r for _, r in ORDEM],
            [[vp, uf, u] + [idx[(u, r)][campo] for _, r in ORDEM] for vp, uf, u in unidades], {"Unidade": 46})
    aba("Por unidade e linha", ["VP", "Estado", "Unidade", "Linha", "Exames (entrada)", "Relevantes", "Enviados"],
        [[h["vp"], h["estado"], h["hospital"], h["rotulo"], h["exames"], h["relevantes"], h["enviados"]]
         for h in sorted(hosp, key=lambda h: (h["estado"], h["hospital"]))], {"Unidade": 46})
    aba("Reumato - Central", ["VP", "Estado", "Unidade", "Enviados (OneDrive)", "Casos na Central", "Abordados", "Captados"],
        [[h["vp"], h["estado"], h["hospital"], h["enviados"], h.get("central_casos", 0), h.get("central_abordados", 0),
          h.get("central_captados", 0)] for h in sorted(hosp, key=lambda h: (h["estado"], h["hospital"]))
         if h["rotulo"] == "Reumatologia"], {"Unidade": 46})
    aba("Randomização", ["Linha", "Segregação desde", "Sorteados", "Grupo controle", "% controle", "Controle enviado"],
        [[ROT_RAND.get(r["linha"], r["linha"]), r["segregacao"].replace("✅ desde ", ""), int(r["sorteados"]),
          int(r["controle"]), float(r["pct_controle"]), int(r["enviados_fora_intervencao"])] for r in rand])
    assert not destino.exists(), destino
    wb.save(destino)


CSS = """
@page { size: A4 landscape; margin: 8mm; }
body { font-family: Segoe UI, Arial, sans-serif; color: #1d2733; font-size: 9.4px; margin: 0; }
h1 { font-size: 17px; margin: 0 0 2px; color: #0b4f6c; }
.sub { color: #5a6672; margin-bottom: 7px; }
.kpis { display: flex; gap: 8px; margin-bottom: 8px; }
.kpi { flex: 1; border: 1px solid #d6dde4; border-radius: 6px; padding: 5px 8px; }
.kpi b { display: block; font-size: 16px; color: #0b4f6c; }
.cols { display: flex; gap: 12px; } .esq { flex: 1.5; } .dir { flex: 1; }
h2 { font-size: 11.5px; margin: 4px 0 3px; color: #0b4f6c; }
table { border-collapse: collapse; width: 100%; }
th { background: #e8f0f6; text-align: left; padding: 3px 4px; font-weight: 600; vertical-align: bottom; }
td { border-bottom: 1px solid #e6eaee; padding: 2.3px 4px; }
tr.sep td { border-top: 1.5px solid #9fb3c4; }
.n { text-align: right; font-variant-numeric: tabular-nums; }
.nota { color: #5a6672; margin-top: 4px; line-height: 1.35; }
.pc { color: #8a959f; font-size: 8.4px; }
.cap { display: flex; flex-wrap: wrap; gap: 2px 11px; } .cap span { white-space: nowrap; color: #5a6672; } .cap b { color: #1d2733; }
.box2 { border-left: 3px solid #0b7a80; background: #eef7f7; padding: 5px 7px; margin-top: 4px; line-height: 1.4; }
.box { border-left: 3px solid #b3261e; background: #fbefee; padding: 5px 7px; margin-top: 4px; line-height: 1.4; }
.quebra { page-break-before: always; }
table.mx { font-size: 5.9px; line-height: 1.05; table-layout: fixed; }
table.mx th { font-size: 5.9px; padding: 1px 1px; text-align: center; }
th.g, td.g, td.g1, th.g1 { border-left: 1.5px solid #9fb3c4; } td.b { font-weight: 700; }
table.mx td { padding: 0.15px 1.5px; border-bottom: 1px solid #edf0f3; text-align: center; white-space: nowrap; overflow: hidden; }
table.mx td.u { text-align: left; } table.mx td.e { color: #5a6672; }
td.v0 { color: #c3cbd3; } td.v1 { background: #eef5fa; } td.v2 { background: #d5e7f3; } td.v3 { background: #aacde6; font-weight: 600; }
td.nd { color: #b3261e; }
"""


def faixa(n: int) -> str:
    return "v0" if n == 0 else ("v1" if n < 100 else ("v2" if n < 1000 else "v3"))


def html(resumo, hosp, rand, cap, cruz, destino: Path) -> None:
    capt = {nome: n for nome, n, _ in cap["linhas"]}
    bi = {"Ca de Cólon — colonoscopia": ("CA Colon", 2), "DII — colonoscopia": ("DII", 2), "Ca de Rim": ("CA Rim", 1),
          "Doenças biliares": ("Calculo Biliar", 1), "Reumatologia": ("Reumato", 1), "Tireoide (TI-RADS)": ("CA Tireoide", 1),
          "Ateromatose": ("Ateromatose", 1), "Neuroimunologia": ("Neuro", 1), "Próstata (PI-RADS)": ("CA Próstata", 1),
          "Ca de Estômago": ("CA Estômago", 1)}
    juntos = {"Ca de Cólon — TC e RM", "DII — TC e RM"}  # captados já mostrados na linha de cima (rowspan)

    def pct(n):
        return f"{n}/80 <span class=pc>({round(100 * n / 80)}%)</span>"

    cons = json.loads((Path(__file__).parent / "consumo-2026-11-18.json").read_text(encoding="utf-8"))["valor_janela"]

    def c_cons(r):
        if r["linha"] in juntos:
            return ""
        nome, span = bi.get(r["linha"], (None, 1))
        val = cons.get(nome) if nome else None
        txt = f"R$ {val / 1e6:.2f} mi".replace(".", ",") if val and val >= 1e6 else (f"R$ {val / 1e3:.0f} mil" if val else "—")
        return f"<td class=n rowspan={span}>{txt}</td>"

    def c_capt(r):
        if r["linha"] in juntos:
            return ""
        nome, span = bi.get(r["linha"], (None, 1))
        val = capt.get(nome) if nome else None
        return f"<td class='n g' rowspan={span}>{val if val is not None else '—'}</td>"

    def linha(r):
        sep = " class=sep" if r["linha"] in ("DII — colonoscopia", "Ca de Rim") else ""
        if r["linha"] == TC_COLON:
            return (f"<tr{sep}><td>{r['linha']}</td><td class=n colspan=3 style='color:#8a959f'>em levantamento</td>"
                    f"<td class='n g'>—</td><td class=n>—</td><td class=n>—</td></tr>")
        return (f"<tr{sep}><td>{r['linha']}</td><td class=n>{br(r['exames'])}</td><td class=n>{br(r['relevantes'])}</td>"
                f"<td class=n><b>{br(r['enviados'])}</b></td>{c_capt(r)}{c_cons(r)}"
                f"<td class='n g'>{pct(r['com_exame'])}</td><td class=n>{pct(r['com_relevante'])}</td>"
                f"<td class=n>{pct(r['com_envio'])}</td></tr>")
    funil = "".join(linha(r) for r in resumo)
    rand_l = "".join(
        f"<tr><td>{ROT_RAND.get(r['linha'], r['linha'])}</td><td>{r['segregacao'].replace('✅ desde ', '')}</td>"
        f"<td class=n>{br(int(r['sorteados']))}</td><td class=n><b>{r['controle']}</b></td>"
        f"<td class=n>{str(r['pct_controle']).replace('.', ',')}%</td></tr>" for r in rand)
    cap_l = "".join(f"<span><b>{n}</b> {l}</span>" for l, n, _ in cap["linhas"])
    ativos = [r for r in resumo if r["linha"] != TC_COLON]
    unidades = sorted({(h["vp"], h["estado"], h["hospital"]) for h in hosp}, key=lambda t: (t[1], t[2]))
    idx = {(h["hospital"], h["rotulo"]): h["exames"] for h in hosp}
    sem_cad = set(cruz["sem_cadastro"])
    idx = {(h["hospital"], h["rotulo"]): h for h in hosp}
    cols = [r for _, r in ORDEM if r != TC_COLON]
    metade = (len(cols) + 1) // 2

    def bloco(cs):
        reu = "Reumatologia"
        cab1 = "".join(f"<th colspan={4 if c == reu else 3} class=g>{CURTO[c]}</th>" for c in cs)
        cab2 = "".join("<th class=g1>entr.</th><th>relev.</th><th>env.</th>" + ("<th>capt.*</th>" if c == reu else "") for c in cs)
        def cel(u, c):
            h = idx[(u, c)]
            e, r, v = h["exames"], h["relevantes"], h["enviados"]
            cap_c = h.get("central_captados", 0)
            extra = f"<td class={'v0' if not cap_c else 'b'}>{cap_c or '—'}</td>" if c == reu else ""
            return (f"<td class='{faixa(e)} g1'>{aprox(e) or '—'}</td><td class={'v0' if not r else ''}>{r or '—'}</td>"
                    f"<td class={'v0' if not v else 'b'}>{v or '—'}</td>{extra}")
        corpo = "".join(
            f"<tr><td class=e>{vp}</td><td class=e>{uf}</td><td class=u>{u.title()}"
            f"{' <span style=color:#b3261e>(sem cadastro)</span>' if u in sem_cad else ''}</td>"
            + "".join(cel(u, c) for c in cs) + "</tr>" for vp, uf, u in unidades)
        return (f"<table class=mx><colgroup><col style='width:5%'><col style='width:2.6%'><col style='width:19%'>"
                f"{''.join('<col>' for _ in range(3 * len(cs) + (1 if reu in cs else 0)))}</colgroup>"
                f"<tr><th rowspan=2>VP</th><th rowspan=2>UF</th><th rowspan=2 style='text-align:left'>Unidade</th>{cab1}</tr>"
                f"<tr>{cab2}</tr>{corpo}</table>")
    titulo_mx = ("<h1>Unidades × linhas — {} (30 dias)</h1><div class=sub>As 80 unidades da planilha de portfólio · "
                 "entr. = exames na entrada do algoritmo (aproximado; cor: &lt; 100 · 100–999 · 1 mil ou mais) · "
                 "relev. = relevantes · env. = enviados à navegação · capt.* = captados na Central de Captação (só Reumatologia, envio por API) · "
                 "— = zero · Cólon TC/RM em levantamento · valores exatos na planilha anexa</div>")
    micro = (f"<div class=quebra></div>{titulo_mx.format('parte 1 de 2')}{bloco(cols[:metade])}"
             f"<div class=quebra></div>{titulo_mx.format('parte 2 de 2')}{bloco(cols[metade:])}")
    tot_ex = sum(r["exames"] for r in ativos)
    tot_env = sum(r["enviados"] for r in ativos)
    tot_rel = sum(r["relevantes"] for r in ativos)
    reu_h = [h for h in hosp if h["rotulo"] == "Reumatologia"]
    rc = {c: sum(h.get(f"central_{c}", 0) for h in reu_h) for c in ("casos", "abordados", "captados")}
    rc["unidades"] = sum(h.get("central_casos", 0) > 0 for h in reu_h)
    pagina = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Visão por linha</title>
<style>{CSS}</style></head><body>
<h1>Captação por linha de cuidado — visão consolidada</h1>
<div class="sub">Produção · execuções de {ddmm(cruz['inicio'])} a {ddmm(cruz['fim'])} (30 dias) · base: as 80 unidades da
planilha de portfólio (aba "Unidades IAs") · ciência de dados</div>
<div class="kpis">
 <div class="kpi"><b>{br(tot_ex)}</b>exames na entrada (soma das linhas)</div>
 <div class="kpi"><b>{br(tot_rel)}</b>relevantes encontrados (soma das linhas)</div>
 <div class="kpi"><b>{br(tot_env)}</b>enviados à navegação (soma das linhas)</div>
 <div class="kpi"><b>{len(ativos)}</b>linhas e recortes com algoritmo em produção</div>
</div>
<h2>Funil por linha de cuidado, janela de {ddmm(cruz['inicio'])} a {ddmm(cruz['fim'])} (30 dias)</h2>
<table><tr><th>Linha</th><th class=n>Exames (entrada)</th><th class=n>Relevantes</th><th class=n>Enviados</th>
<th class='n g'>Captados</th><th class=n>Consumo</th><th class='n g'>Hospitais com exame (entrada)</th>
<th class=n>Hospitais com relevantes</th><th class=n>Hospitais com envios</th></tr>{funil}</table>
<div class="cols"><div class="esq">
<div class="nota"><b>Premissas.</b> <b>Hospitais</b>: as 80 unidades da planilha de portfólio. <b>Hospitais com exame</b>: com
pelo menos um exame na entrada do algoritmo no período; <b>com relevantes</b>: com pelo menos um achado relevante; <b>com
envios</b>: com pelo menos um paciente enviado à navegação. <b>Exames</b>: entrada do algoritmo, depois do filtro de cada linha
(não é o volume total do hospital). <b>Enviados</b>: só o grupo intervenção. Um exame pode entrar em mais de uma linha (a mesma
colonoscopia no Ca de Cólon e na DII): os totais somam linhas.<br>
<b>Captados</b>: pacientes distintos com entrada na navegação no período, pela base do BI de controle da captação (registros
até 10/11). Inclui todas as origens de captação, não só os algoritmos; a base não traz o hospital. Ca de Cólon e DII: valor da
linha inteira. Linhas do BI sem algoritmo correspondente: Pneumo {capt.get('Pneumo', 0)}, Dermato {capt.get('Dermato', 0)},
Mama {capt.get('Mama', 0)}, Colo de útero {capt.get('Colo de útero', 0)}.<br>
<b>Consumo</b>: consumo dos pacientes captados no período, pela base do BI de controle da captação (dados até 12/11), por linha do BI; Ca de Cólon e DII: linha inteira. É consumo gerado, não uplift (não compara com o grupo controle). Linhas do BI sem algoritmo: Mama R$ 4,20 mi, Pneumo R$ 2,63 mi, Dermato R$ 1,52 mi, Colo de útero R$ 170 mil.<br><b>Captados por hospital</b>: dos 4.750 pacientes enviados no período, 120 constam da base de captados da mesma linha, quase todos com entrada na navegação <i>antes</i> do envio; a base ainda não registra a captação vinda dos envios — a confirmar com a área.<br>
Hospital a hospital: páginas 2 e 3 e planilha anexa.</div>

<h2>Reumatologia — captação registrada na Central de Captação</h2>
<div class="box2">Na Reumatologia o envio passa pela Central de Captação (API), que registra cada caso ligado ao achado do
algoritmo. No período: <b>{rc['casos']} casos → {rc['abordados']} abordados → {rc['captados']} captados</b>, em {rc['unidades']} hospitais.
Nas demais linhas o envio é por planilha e a captação é registrada manualmente, sem vínculo com o envio — por isso os captados
dessas linhas só existem como total por linha, sem hospital.</div>
</div><div class="dir">
<h2>Randomização (grupo controle)</h2>
<table><tr><th>Linha</th><th>Desde</th><th class=n>Sorteados</th><th class=n>Controle</th><th class=n>%</th></tr>{rand_l}</table>
<div class="nota">Controle = paciente encontrado e não enviado à navegação, para medir o efeito da navegação. Esperado ~5%;
nenhum paciente do controle foi enviado.</div>
</div></div>
{micro}
</body></html>"""
    assert not destino.exists(), destino
    destino.write_text(pagina, encoding="utf-8")


def main(arq_cruz, arq_unid, arq_rand, arq_cap, pasta, versao):
    cruz = json.loads(Path(arq_cruz).read_text(encoding="utf-8"))
    unid = json.loads(Path(arq_unid).read_text(encoding="utf-8"))
    rand = json.loads(Path(arq_rand).read_text(encoding="utf-8"))
    cap = json.loads(Path(arq_cap).read_text(encoding="utf-8"))
    resumo, hosp = montar(cruz, unid)
    out = Path(pasta)
    planilha(resumo, hosp, rand, cruz, out / f"visao-por-linha-{cruz['fim']}-{versao}.xlsx")
    html(resumo, hosp, rand, cap, cruz, out / f"visao-por-linha-{cruz['fim']}-{versao}.html")
    for r in resumo:
        print(r["linha"], r["exames"], r["relevantes"], r["enviados"], r["com_exame"])


if __name__ == "__main__":
    main(*sys.argv[1:7])
