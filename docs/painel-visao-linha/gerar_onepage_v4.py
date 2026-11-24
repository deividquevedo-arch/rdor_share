"""One page e planilha da "Visão por linha" — v4: hospitais pela planilha de portfólio (aba "Unidades IAs").

Uso: python gerar_onepage_v4.py <cruzamento-unidades-AAAA-MM-DD.json> <randomizacao.json> <captados.json> <pasta> <versão>
Só agregados: nenhum dado de paciente.
"""
import json
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from gerar_onepage import ROT_RAND, br

ORDEM = [  # coluna da planilha, rótulo — Ca de Cólon e DII lado a lado
    ("Ca de Colon (Colonoscopia)", "Ca de Cólon — colonoscopia"), ("Ca de Colon (TC e RM)", "Ca de Cólon — TC e RM"),
    ("DII (Colonoscopia)", "DII — colonoscopia"), ("DII (TC e RM )", "DII — TC e RM"),
    ("Ca de Rim", "Ca de Rim"), ("Doenças Biliares", "Doenças biliares"), ("Reumatologia", "Reumatologia"),
    ("TU Osseo", "Tumor ósseo"), ("Tireóide", "Tireoide (TI-RADS)"), ("Ateromatose Coronariana", "Ateromatose"),
    ("Neuroimunologia", "Neuroimunologia"), ("Próstata", "Próstata (PI-RADS)"),
    ("Ca de Estomago (Endoscopias)", "Ca de Estômago"), ("Transplante de Pulmão", "Transplante de pulmão"),
]
NAVEGADO = "Navegado"
INDISP = "Não disponível"


def resumir(hosp: list[dict]) -> list[dict]:
    out = []
    for col, rot in ORDEM:
        hs = [h for h in hosp if h["coluna"] == col]
        nav = [h for h in hs if h["status_planilha"] == NAVEGADO]
        out.append({"linha": rot, "exames": sum(h["exames"] for h in hs), "rel_nav": sum(h["relevantes"] for h in hs),
                    "enviados": sum(h["enviados"] for h in hs), "navegados": len(nav),
                    "nav_com_exame": sum(h["exames"] > 0 for h in nav), "nav_sem_exame": sum(h["exames"] == 0 for h in nav),
                    "indisp_com_exame": sum(h["exames"] > 0 and h["status_planilha"] == INDISP for h in hs)})
    return out


def planilha(resumo, hosp, rand, cruz, destino: Path) -> None:
    rot = dict(ORDEM)
    wb = Workbook()
    lm = wb.active
    lm.title = "Leia-me"
    for a, b in [
        ("Visão por linha — dados do one page (v4)", ""),
        ("Janela", f"execuções do modelo de {cruz['inicio']} a {cruz['fim']} (30 dias), produção"),
        ("Hospitais", "80 unidades da aba 'Unidades IAs' da planilha de portfólio, com o status de navegação de cada linha"),
        ("Exames (entrada)", "exames distintos que chegaram ao algoritmo da linha, depois do filtro de entrada"),
        ("Relevantes", "exames marcados como relevantes pelo algoritmo"),
        ("Enviados", "exames que constam do envio à navegação (OneDrive) — só o grupo intervenção"),
        ("Navegado sem exame", "status 'Navegado' na planilha, mas nenhum exame chegou ao algoritmo no período — investigar"),
        ("Não disponível com exame", "status 'Não disponível' na planilha, mas há exame no algoritmo — a planilha pode estar desatualizada"),
        ("Sem cadastro no repositório", ", ".join(cruz["sem_cadastro"])),
        ("Controle do cruzamento", f"{cruz['controle_envio']['enviados']} enviados no período; "
                                   f"{cruz['controle_envio']['sem_saida']} sem correspondência na saída da linha"),
    ]:
        lm.append([a, b])
    lm.column_dimensions["A"].width = 30
    lm.column_dimensions["B"].width = 120
    lm["A1"].font = Font(bold=True, size=13)

    def aba(nome, cab, linhas):
        ws = wb.create_sheet(nome)
        ws.append(cab)
        for c in ws[1]:
            c.font = Font(bold=True)
            c.fill = PatternFill("solid", fgColor="DDEBF7")
            c.alignment = Alignment(wrap_text=True, vertical="top")
        for l in linhas:
            ws.append(l)
        for i, c in enumerate(cab, 1):
            ws.column_dimensions[ws.cell(1, i).column_letter].width = max(12, min(48, len(c) + 4))
        ws.freeze_panes = "B2"
        ws.auto_filter.ref = ws.dimensions

    aba("Resumo por linha", ["Linha", "Exames (entrada)", "Relevantes", "Enviados", "Navegados (planilha)",
                             "Navegados com exame", "Navegado sem exame", "Não disponível com exame"],
        [[r["linha"], r["exames"], r["rel_nav"], r["enviados"], r["navegados"], r["nav_com_exame"], r["nav_sem_exame"],
          r["indisp_com_exame"]] for r in resumo])

    def situacao(h):
        if h["status_planilha"] == NAVEGADO and h["exames"] == 0:
            return "navegado sem exame"
        if h["status_planilha"] == INDISP and h["exames"] > 0:
            return "não disponível com exame"
        return ""
    ordem = {c: i for i, (c, _) in enumerate(ORDEM)}
    aba("Por hospital", ["Linha", "UF", "Hospital", "Status na planilha", "Exames (entrada)", "Relevantes", "Enviados",
                         "Divergência", "Cadastro no repositório"],
        [[rot[h["coluna"]], h["uf"], h["hospital"], h["status_planilha"], h["exames"], h["relevantes"], h["enviados"],
          situacao(h), "sim" if h["cadastro_no_repositorio"] else "não"]
         for h in sorted(hosp, key=lambda h: (ordem[h["coluna"]], h["uf"], h["hospital"]))])
    aba("Randomização", ["Linha", "Segregação desde", "Sorteados", "Grupo controle", "% controle", "Controle enviado"],
        [[ROT_RAND.get(r["linha"], r["linha"]), r["segregacao"].replace("✅ desde ", ""), int(r["sorteados"]),
          int(r["controle"]), float(r["pct_controle"]), int(r["enviados_fora_intervencao"])] for r in rand])
    assert not destino.exists(), destino
    wb.save(destino)


def ddmm(s: str) -> str:
    a, m, d = s.split("-")
    return f"{d}/{m}/{a}"


def html(resumo, rand, cap, cruz, destino: Path) -> None:
    def linha(r):
        tcrm = r["linha"] == "Ca de Cólon — TC e RM"
        fim = "</td></tr>"
        if tcrm:
            return (f"<tr class=sep><td>{r['linha']}</td><td class=n colspan=3 style='color:#b3261e'>sem algoritmo em produção "
                    f"(a linha só recebe colonoscopia)</td><td class=n>{r['navegados']}</td><td class=n>—</td>"
                    f"<td class=n>—</td><td class=n>—{fim}")
        sep = " class=sep" if r["linha"].startswith(("DII — colonoscopia", "Ca de Rim")) else ""
        alerta = "alerta" if r["nav_sem_exame"] >= 3 else ""
        return (f"<tr{sep}><td>{r['linha']}</td><td class=n>{br(r['exames'])}</td><td class=n>{br(r['rel_nav'])}</td>"
                f"<td class=n><b>{br(r['enviados'])}</b></td><td class=n>{r['navegados']}</td>"
                f"<td class=n>{r['nav_com_exame']}</td><td class='n {alerta}'>{r['nav_sem_exame']}</td>"
                f"<td class=n>{r['indisp_com_exame']}{fim}")
    linhas = "".join(linha(r) for r in resumo)
    rand_l = "".join(
        f"<tr><td>{ROT_RAND.get(r['linha'], r['linha'])}</td><td>{r['segregacao'].replace('✅ desde ', '')}</td>"
        f"<td class=n>{br(int(r['sorteados']))}</td><td class=n><b>{r['controle']}</b></td>"
        f"<td class=n>{str(r['pct_controle']).replace('.', ',')}%</td><td class=n>{r['enviados_fora_intervencao']}</td></tr>"
        for r in rand)
    cel = "".join(f"<span><b>{n}</b> {l}</span>" for l, n, _ in cap["linhas"])
    tot_ex = sum(r["exames"] for r in resumo)
    tot_env = sum(r["enviados"] for r in resumo)
    pagina = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Visão por linha</title>
<style>
@page {{ size: A4 landscape; margin: 9mm; }}
body {{ font-family: Segoe UI, Arial, sans-serif; color: #1d2733; font-size: 9.4px; margin: 0; }}
h1 {{ font-size: 17px; margin: 0 0 2px; color: #0b4f6c; }}
.sub {{ color: #5a6672; margin-bottom: 7px; }}
.kpis {{ display: flex; gap: 8px; margin-bottom: 8px; }}
.kpi {{ flex: 1; border: 1px solid #d6dde4; border-radius: 6px; padding: 5px 8px; }}
.kpi b {{ display: block; font-size: 16px; color: #0b4f6c; }}
.cols {{ display: flex; gap: 12px; }} .esq {{ flex: 1.65; }} .dir {{ flex: 1; }}
h2 {{ font-size: 11.5px; margin: 4px 0 3px; color: #0b4f6c; }}
table {{ border-collapse: collapse; width: 100%; }}
th {{ background: #e8f0f6; text-align: left; padding: 3px 4px; font-weight: 600; vertical-align: bottom; }}
td {{ border-bottom: 1px solid #e6eaee; padding: 2.3px 4px; }}
tr.sep td {{ border-top: 1.5px solid #9fb3c4; }}
.n {{ text-align: right; font-variant-numeric: tabular-nums; }}
.alerta {{ color: #b3261e; font-weight: 700; }}
.nota {{ color: #5a6672; margin-top: 4px; line-height: 1.35; }}
.cap {{ display: flex; flex-wrap: wrap; gap: 2px 11px; }} .cap span {{ white-space: nowrap; color: #5a6672; }} .cap b {{ color: #1d2733; }}
.box {{ border-left: 3px solid #c9811a; background: #fbf6ee; padding: 5px 7px; margin-top: 6px; line-height: 1.4; }}
</style></head><body>
<h1>Captação por linha de cuidado — visão consolidada</h1>
<div class="sub">Produção · execuções de {ddmm(cruz['inicio'])} a {ddmm(cruz['fim'])} (30 dias) · hospitais e status de navegação
da planilha de portfólio (aba "Unidades IAs", 80 unidades) · ciência de dados</div>
<div class="kpis">
 <div class="kpi"><b>{br(tot_ex)}</b>exames na entrada (soma das linhas)</div>
 <div class="kpi"><b>{br(tot_env)}</b>envios à navegação (soma das linhas)</div>
 <div class="kpi"><b>13</b>linhas e recortes com algoritmo em produção</div>
 <div class="kpi"><b>6</b>linhas com grupo controle (randomização)</div>
</div>
<div class="cols"><div class="esq">
<h2>Funil por linha: exames → relevantes → enviados, e cobertura dos hospitais navegados</h2>
<table><tr><th>Linha</th><th class=n>Exames (entrada)</th><th class=n>Relevantes</th><th class=n>Enviados</th>
<th class=n>Navegados (planilha)</th><th class=n>Navegados com exame</th><th class=n>Navegado sem exame</th>
<th class=n>Não disponível com exame</th></tr>{linhas}</table>
<div class="nota"><b>Navegado sem exame</b>: a planilha marca a unidade como navegada, mas nenhum exame chegou ao algoritmo
no período — investigar filtro, tipo de exame ou cadastro. <b>Não disponível com exame</b>: há exame no algoritmo, mas a
planilha marca a unidade como não disponível — a planilha pode estar desatualizada. Um exame pode entrar em mais de uma
linha (ex.: a mesma colonoscopia no Ca de Cólon e na DII): os totais somam linhas. Hospital por hospital: planilha anexa.</div>
<h2>Pacientes captados (BI de controle da captação, {ddmm(cap['inicio'])} a {ddmm(cap['fim'])})</h2>
<div class="cap">{cel}</div>
<div class="nota">Pacientes distintos com entrada na navegação no período, por linha do BI. Inclui outras origens de captação,
não só os algoritmos; registros até 10/11. A base do BI não traz o hospital.</div>
</div><div class="dir">
<h2>Randomização (grupo controle)</h2>
<table><tr><th>Linha</th><th>Desde</th><th class=n>Sorteados</th><th class=n>Controle</th><th class=n>%</th>
<th class=n>Controle enviado</th></tr>{rand_l}</table>
<div class="nota">Controle = paciente encontrado e não enviado à navegação, para medir o efeito da navegação.
Esperado ~5%; "controle enviado" tem de ser 0.</div>
<h2>Pontos de atenção</h2>
<div class="box">
<b>Ca de Cólon — TC e RM</b>: navegado em 50 unidades na planilha, mas o algoritmo em produção só recebe colonoscopia.<br>
<b>Colonoscopia e endoscopia</b>: a planilha marca 11 unidades navegadas em colonoscopia e 10 em endoscopia, e 38/24 como "não disponível" mas com exame; o envio atual sai das 49 unidades da navegação — alinhar qual vale.<br>
<b>Ca de Estômago</b>: 4 das 10 unidades navegadas sem exame — o filtro de entrada não reconhece parte das endoscopias.<br>
<b>Neuroimunologia</b>: 9 das 47 unidades navegadas sem exame.<br>
<b>Hepatologia</b>: sem execução em produção desde 14/10/2026.<br>
<b>Sem cadastro no repositório</b>: {", ".join(n.title() for n in cruz['sem_cadastro'])}.
</div>
<h2>Ainda não incluído</h2>
<div class="nota"><b>Captados por hospital</b>: exige cruzar a base do BI pelo paciente (próxima versão).<br>
<b>Consumo e uplift/margem</b>: as bases de consumo disponíveis param em 29/09/2026 (última carga 17/09); o grupo
controle começou em 30/10 — sem consumo posterior não há como medir o efeito.</div>
</div></div></body></html>"""
    assert not destino.exists(), destino
    destino.write_text(pagina, encoding="utf-8")


def main(arq_cruz, arq_rand, arq_cap, pasta, versao):
    cruz = json.loads(Path(arq_cruz).read_text(encoding="utf-8"))
    rand = json.loads(Path(arq_rand).read_text(encoding="utf-8"))
    cap = json.loads(Path(arq_cap).read_text(encoding="utf-8"))
    resumo = resumir(cruz["hospitais"])
    out = Path(pasta)
    planilha(resumo, cruz["hospitais"], rand, cruz, out / f"visao-por-linha-{cruz['fim']}-{versao}.xlsx")
    html(resumo, rand, cap, cruz, out / f"visao-por-linha-{cruz['fim']}-{versao}.html")
    for r in resumo:
        print(r)


if __name__ == "__main__":
    main(*sys.argv[1:6])
