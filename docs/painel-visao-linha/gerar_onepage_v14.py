"""One page "Visão por linha" — v14: Ca de Cólon e DII consolidados (TC + RM + colonoscopia) e quadro de colonoscopia.

Diferenças para a v13 (gerar_onepage_v6.py): cada linha de cuidado é uma linha só; a colonoscopia (Ca de Cólon e DII)
ganha um quadro próprio de cobertura de hospitais; o texto não cita o canal de registro da captação.

Uso: python gerar_onepage_v14.py <cruzamento.json> <unidades_ias.json> <randomizacao.json> <captados.json> <pasta> <versão>
Só agregados: nenhum dado de paciente.
"""
import json
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from gerar_onepage import ROT_RAND, br
from gerar_onepage_v4 import ddmm
from gerar_onepage_v6 import CSS, aprox, faixa, montar as montar_v13

AQUI = Path(__file__).parent
# linha consolidada -> rótulos da v13 que a compõem
LINHAS = [
    ("Ca de Cólon", ["Ca de Cólon — colonoscopia", "Ca de Cólon — TC e RM"]),
    ("DII", ["DII — colonoscopia", "DII — TC e RM"]),
    ("Ca de Rim", ["Ca de Rim"]), ("Doenças biliares", ["Doenças biliares"]), ("Reumatologia", ["Reumatologia"]),
    ("Tumor ósseo", ["Tumor ósseo"]), ("Tireoide (TI-RADS)", ["Tireoide (TI-RADS)"]), ("Ateromatose", ["Ateromatose"]),
    ("Neuroimunologia", ["Neuroimunologia"]), ("Próstata (PI-RADS)", ["Próstata (PI-RADS)"]),
    ("Ca de Estômago", ["Ca de Estômago"]), ("Transplante de pulmão", ["Transplante de pulmão"]),
]
CURTO = {"Ca de Cólon": "Ca Cólon", "DII": "DII", "Ca de Rim": "Rim", "Doenças biliares": "Biliares",
         "Reumatologia": "Reumato", "Tumor ósseo": "TU ósseo", "Tireoide (TI-RADS)": "Tireoide", "Ateromatose": "Ateroma",
         "Neuroimunologia": "Neuro", "Próstata (PI-RADS)": "Próstata", "Ca de Estômago": "Estômago",
         "Transplante de pulmão": "Tx pulmão"}
BI = {"Ca de Cólon": "CA Colon", "DII": "DII", "Ca de Rim": "CA Rim", "Doenças biliares": "Calculo Biliar",
      "Reumatologia": "Reumato", "Tireoide (TI-RADS)": "CA Tireoide", "Ateromatose": "Ateromatose", "Neuroimunologia": "Neuro",
      "Próstata (PI-RADS)": "CA Próstata", "Ca de Estômago": "CA Estômago"}
COLONO = ["Ca de Cólon — colonoscopia", "DII — colonoscopia"]
CAMPOS = ("exames", "relevantes", "enviados")


def consolidar(hosp13: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Linhas consolidadas por hospital, resumo por linha e o quadro de colonoscopia (recortes da v13)."""
    por = {(h["hospital"], h["rotulo"]): h for h in hosp13}
    unidades = sorted({(h["vp"], h["estado"], h["hospital"]) for h in hosp13}, key=lambda t: (t[1], t[2]))
    hosp = []
    for vp, uf, u in unidades:
        for linha, partes in LINHAS:
            hs = [por[(u, p)] for p in partes]
            reg = {"vp": vp, "estado": uf, "hospital": u, "linha": linha, **{c: sum(h[c] for h in hs) for c in CAMPOS}}
            reg["captados_reg"] = sum(h.get("central_captados", 0) for h in hs)
            reg["casos_reg"] = sum(h.get("central_casos", 0) for h in hs)
            reg["abordados_reg"] = sum(h.get("central_abordados", 0) for h in hs)
            hosp.append(reg)

    def resumo_de(regs, nome):
        return {"linha": nome, **{c: sum(h[c] for h in regs) for c in CAMPOS},
                "com_exame": sum(h["exames"] > 0 for h in regs), "com_relevante": sum(h["relevantes"] > 0 for h in regs),
                "com_envio": sum(h["enviados"] > 0 for h in regs), "unidades": len(regs)}
    resumo = [resumo_de([h for h in hosp if h["linha"] == l], l) for l, _ in LINHAS]
    colono = [resumo_de([h for h in hosp13 if h["rotulo"] == r], r.replace(" — colonoscopia", "")) for r in COLONO]
    assert all(r["unidades"] == len(unidades) for r in resumo + colono)
    return hosp, resumo, colono


def planilha(resumo, colono, hosp, rand, cruz, destino: Path) -> None:
    wb = Workbook()
    lm = wb.active
    lm.title = "Leia-me"
    for a, b in [
        ("Visão por linha — dados do one page (v14)", ""),
        ("Janela", f"execuções do modelo de {cruz['inicio']} a {cruz['fim']} (30 dias), produção"),
        ("Hospitais", "as 80 unidades da aba 'Unidades IAs' da planilha de portfólio (VP e Estado de lá)"),
        ("Ca de Cólon e DII", "linha consolidada: todos os exames que entram no algoritmo da linha"),
        ("Colonoscopia", "aba própria com a cobertura de hospitais só da colonoscopia (Ca de Cólon e DII)"),
        ("Exames / Relevantes / Enviados", "entrada do algoritmo / achados relevantes / enviados à navegação (grupo intervenção)"),
        ("Sem cadastro no repositório", ", ".join(cruz["sem_cadastro"])),
    ]:
        lm.append([a, b])
    lm.column_dimensions["A"].width = 30
    lm.column_dimensions["B"].width = 120
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

    cab_r = ["Linha", "Exames (entrada)", "Relevantes", "Enviados", "Hospitais com exame", "Hospitais com relevantes",
             "Hospitais com envios"]
    aba("Resumo por linha", cab_r, [[r["linha"], r["exames"], r["relevantes"], r["enviados"], r["com_exame"],
                                     r["com_relevante"], r["com_envio"]] for r in resumo])
    aba("Colonoscopia", cab_r, [[r["linha"] + " — colonoscopia", r["exames"], r["relevantes"], r["enviados"], r["com_exame"],
                                 r["com_relevante"], r["com_envio"]] for r in colono])
    unidades = sorted({(h["vp"], h["estado"], h["hospital"]) for h in hosp}, key=lambda t: (t[1], t[2]))
    idx = {(h["hospital"], h["linha"]): h for h in hosp}
    for campo, nome in (("exames", "Unid x linhas - entrada"), ("relevantes", "Unid x linhas - relevantes"),
                        ("enviados", "Unid x linhas - enviados")):
        aba(nome, ["VP", "Estado", "Unidade"] + [l for l, _ in LINHAS],
            [[vp, uf, u] + [idx[(u, l)][campo] for l, _ in LINHAS] for vp, uf, u in unidades], {"Unidade": 46})
    aba("Reumato - captação (em valid.)", ["VP", "Estado", "Unidade", "Enviados", "Casos registrados", "Abordados", "Captados"],
        [[h["vp"], h["estado"], h["hospital"], h["enviados"], h["casos_reg"], h["abordados_reg"], h["captados_reg"]]
         for h in hosp if h["linha"] == "Reumatologia"], {"Unidade": 46})
    aba("Randomização", ["Linha", "Segregação desde", "Sorteados", "Grupo controle", "% controle", "Controle enviado"],
        [[ROT_RAND.get(r["linha"], r["linha"]), r["segregacao"].replace("✅ desde ", ""), int(r["sorteados"]),
          int(r["controle"]), float(r["pct_controle"]), int(r["enviados_fora_intervencao"])] for r in rand])
    assert not destino.exists(), destino
    wb.save(destino)


def html(resumo, colono, hosp, rand, cap, cruz, destino: Path) -> None:
    capt = {nome: n for nome, n, _ in cap["linhas"]}
    cons = json.loads((AQUI / "consumo-2026-11-18.json").read_text(encoding="utf-8"))["valor_janela"]

    def pct(n):
        return f"{n}/80 <span class=pc>({round(100 * n / 80)}%)</span>"

    def reais(v):
        return f"R$ {v / 1e6:.2f} mi".replace(".", ",") if v and v >= 1e6 else (f"R$ {v / 1e3:.0f} mil" if v else "—")

    def linha(r, com_bi=True):
        nome = BI.get(r["linha"])
        capt_c = (capt.get(nome) if nome else None) if com_bi else None
        obs = ""
        meio = (f"<td class='n g'>{capt_c if capt_c is not None else '—'}</td><td class=n>{reais(cons.get(nome) if nome else None)}</td>"
                if com_bi else "")
        return (f"<tr><td>{r['linha']}{obs}</td><td class=n>{br(r['exames'])}</td><td class=n>{br(r['relevantes'])}</td>"
                f"<td class=n><b>{br(r['enviados'])}</b></td>{meio}<td class='n g'>{pct(r['com_exame'])}</td>"
                f"<td class=n>{pct(r['com_relevante'])}</td><td class=n>{pct(r['com_envio'])}</td></tr>")
    funil = "".join(linha(r) for r in resumo)
    quadro_col = "".join(f"<tr><td>{r['linha']}</td><td class='n g'>{pct(r['com_exame'])}</td>"
                         f"<td class=n>{pct(r['com_relevante'])}</td><td class=n>{pct(r['com_envio'])}</td></tr>" for r in colono)
    rand_l = "".join(
        f"<tr><td>{ROT_RAND.get(r['linha'], r['linha'])}</td><td>{r['segregacao'].replace('✅ desde ', '')}</td>"
        f"<td class=n>{br(int(r['sorteados']))}</td><td class=n><b>{r['controle']}</b></td>"
        f"<td class=n>{str(r['pct_controle']).replace('.', ',')}%</td></tr>" for r in rand)
    reu = [h for h in hosp if h["linha"] == "Reumatologia"]
    rc = {c: sum(h[f"{c}_reg"] for h in reu) for c in ("casos", "abordados", "captados")}
    rc["hospitais"] = sum(h["casos_reg"] > 0 for h in reu)
    tot = {c: sum(r[c] for r in resumo) for c in CAMPOS}
    leg = [x for x in json.loads((AQUI / "legibilidade-colono-endoscopia-2026-11-18.json").read_text(encoding="utf-8"))
           if x["linha"] == "cancer_colon"]
    ocr = {"exames": sum(int(x["exames"]) for x in leg), "sem_texto": sum(int(x["sem_texto"]) for x in leg)}
    ocr["pct"] = round(100 * ocr["sem_texto"] / ocr["exames"])

    unidades = sorted({(h["vp"], h["estado"], h["hospital"]) for h in hosp}, key=lambda t: (t[1], t[2]))
    idx = {(h["hospital"], h["linha"]): h for h in hosp}
    sem_cad = set(cruz["sem_cadastro"])
    cols = [l for l, _ in LINHAS]
    metade = (len(cols) + 1) // 2

    def bloco(cs):
        reu_c = "Reumatologia"
        cab1 = "".join(f"<th colspan=3 class=g>{CURTO[c]}</th>" for c in cs)
        cab2 = "".join("<th class=g1>entr.</th><th>relev.</th><th>env.</th>" for c in cs)

        def cel(u, c):
            h = idx[(u, c)]
            e, r, v = h["exames"], h["relevantes"], h["enviados"]
            extra = ""
            return (f"<td class='{faixa(e)} g1'>{aprox(e) or '—'}</td><td class={'v0' if not r else ''}>{r or '—'}</td>"
                    f"<td class={'v0' if not v else 'b'}>{v or '—'}</td>{extra}")
        corpo = "".join(
            f"<tr><td class=e>{vp}</td><td class=e>{uf}</td><td class=u>{u.title()}"
            f"{' <span style=color:#8a959f>(sem cadastro)</span>' if u in sem_cad else ''}</td>"
            + "".join(cel(u, c) for c in cs) + "</tr>" for vp, uf, u in unidades)
        return (f"<table class=mx><colgroup><col style='width:5%'><col style='width:2.6%'><col style='width:19%'>"
                f"{''.join('<col>' for _ in range(3 * len(cs)))}</colgroup>"
                f"<tr><th rowspan=2>VP</th><th rowspan=2>UF</th><th rowspan=2 style='text-align:left'>Unidade</th>{cab1}</tr>"
                f"<tr>{cab2}</tr>{corpo}</table>")
    titulo_mx = ("<h1>Hospitais × linhas — {} (30 dias)</h1><div class=sub>As 80 unidades da planilha de portfólio · "
                 "entr. = exames na entrada do algoritmo (aproximado; cor: &lt; 100 · 100–999 · 1 mil ou mais) · "
                 "relev. = relevantes · env. = enviados à navegação · — = zero · valores exatos na planilha anexa</div>")
    micro = (f"<div class=quebra></div>{titulo_mx.format('parte 1 de 2')}{bloco(cols[:metade])}"
             f"<div class=quebra></div>{titulo_mx.format('parte 2 de 2')}{bloco(cols[metade:])}")
    cab_cob = ("<th class='n g'>Hospitais com exame (entrada)</th><th class=n>Hospitais com relevantes</th>"
               "<th class=n>Hospitais com envios</th>")
    pagina = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Visão por linha</title>
<style>{CSS}
.box2 {{ border-left: 3px solid #0b7a80; background: #eef7f7; padding: 5px 7px; margin-top: 4px; line-height: 1.4; }}
.pc {{ color: #8a959f; font-size: 8.4px; }}</style></head><body>
<h1>Captação por linha de cuidado — visão consolidada</h1>
<div class="sub">Produção · execuções de {ddmm(cruz['inicio'])} a {ddmm(cruz['fim'])} (30 dias) · base: as 80 unidades da
planilha de portfólio (aba "Unidades IAs") · ciência de dados</div>
<div class="kpis">
 <div class="kpi"><b>{br(tot['exames'])}</b>exames na entrada</div>
 <div class="kpi"><b>{br(tot['relevantes'])}</b>relevantes encontrados</div>
 <div class="kpi"><b>{br(tot['enviados'])}</b>enviados à navegação</div>
 <div class="kpi"><b>{len(resumo)}</b>linhas de cuidado com algoritmo em produção</div>
</div>
<h2>Funil por linha de cuidado, janela de {ddmm(cruz['inicio'])} a {ddmm(cruz['fim'])} (30 dias)</h2>
<table><tr><th>Linha</th><th class=n>Exames (entrada)</th><th class=n>Relevantes</th><th class=n>Enviados</th>
<th class='n g'>Captados</th><th class=n>Consumo</th>{cab_cob}</tr>{funil}</table>
<div class="cols"><div class="esq">
<h2>Colonoscopia e OCR — Ca de Cólon e DII</h2>
<table><tr><th>Linha (só colonoscopia)</th>{cab_cob}</tr>{quadro_col}</table>
<div class="nota"><b>Premissas.</b> <b>Hospitais</b>: as 80 unidades da planilha de portfólio. <b>Hospitais com exame</b>: com
pelo menos um exame na entrada do algoritmo no período; <b>com relevantes</b>: com pelo menos um achado relevante; <b>com
envios</b>: com pelo menos um paciente enviado à navegação. <b>Exames</b>: entrada do algoritmo, depois do filtro de cada linha
(não é o volume total do hospital). <b>Enviados</b>: só o grupo intervenção. Um exame
pode entrar em mais de uma linha (a mesma colonoscopia no Ca de Cólon e na DII): os totais somam linhas.<br>
<b>Captados</b> e <b>consumo</b>: base do BI de controle da captação, por linha (captados com registros até 10/11; consumo até
12/11). Incluem todas as origens de captação, não só os algoritmos, e não trazem o hospital; consumo gerado, não uplift. Linhas do
BI sem algoritmo correspondente: Pneumo {capt.get('Pneumo', 0)} captados, Dermato {capt.get('Dermato', 0)},
Mama {capt.get('Mama', 0)}, Colo de útero {capt.get('Colo de útero', 0)}.<br>
Hospital a hospital: páginas 2 e 3 e planilha anexa.</div>
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
    _, hosp13 = montar_v13(cruz, unid)
    hosp, resumo, colono = consolidar(hosp13)
    out = Path(pasta)
    planilha(resumo, colono, hosp, rand, cruz, out / f"visao-por-linha-{cruz['fim']}-{versao}.xlsx")
    html(resumo, colono, hosp, rand, cap, cruz, out / f"visao-por-linha-{cruz['fim']}-{versao}.html")
    for r in resumo + colono:
        print(f"{r['linha']:28} exames {r['exames']:7} relev {r['relevantes']:5} env {r['enviados']:5} "
              f"hosp {r['com_exame']:2}/{r['com_relevante']:2}/{r['com_envio']:2}")


if __name__ == "__main__":
    main(*sys.argv[1:7])
