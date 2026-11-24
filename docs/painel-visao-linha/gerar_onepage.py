"""One page (HTML -> PDF) e planilha da "Visão por linha", a partir dos JSON agregados.

Uso: python gerar_onepage.py <visao-por-linha-AAAA-MM-DD.json> <randomizacao-AAAA-MM-DD.json> <pasta de saída>
Gera: visao-por-linha-<fim>-v1.xlsx e visao-por-linha-<fim>-v1.html (o PDF sai do HTML pelo Edge headless).
Só agregados: nenhum dado de paciente.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

ROTULO = {"ateromatose_coronariana": "Ateromatose", "cancer_colon": "Ca de Cólon", "cancer_estomago": "Ca de Estômago",
          "cancer_rim": "Ca de Rim", "doenca_inflamatoria_intestinal": "DII", "doencas_biliares": "Doenças biliares",
          "hepatologia": "Hepatologia", "neuroimunologia": "Neuroimunologia", "pirads": "Próstata (PI-RADS)",
          "reumatologia": "Reumatologia", "tirads": "Tireoide (TI-RADS)", "transplante_pulmao": "Transplante de pulmão",
          "tumor_osseo": "Tumor ósseo"}
ROT_RAND = {"Reumatologia": "Reumatologia", "Tireoide": "Tireoide (TI-RADS)", "Ateromatose": "Ateromatose",
            "DII": "DII", "Doenças biliares": "Doenças biliares", "Neuroimunologia": "Neuroimunologia"}
NAO_NAV = "(não navegado)"
BAIXO = 30


def nome_linha(linha: str, variante: str) -> str:
    return ROTULO[linha] + (f" — {variante}" if variante else "")


def br(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def consolidar(dados: dict) -> tuple[list[dict], list[dict]]:
    """Resumo por linha (variante) e tabela por hospital, com os 49 hospitais em toda linha (zero quando sem exame)."""
    por = defaultdict(lambda: {"exames": 0, "relevantes": 0, "enviados": 0, "rel_nav": 0, "exames_nav": 0})
    hosp = defaultdict(lambda: {"exames": 0, "relevantes": 0, "enviados": 0})
    chaves = set()
    for r in dados["linhas"]:
        k = (r["linha"], r["variante"])
        chaves.add(k)
        e, rel, env = int(r["exames"]), int(r["relevantes"]), int(r["enviados"])
        p = por[k]
        p["exames"] += e
        p["relevantes"] += rel
        p["enviados"] += env
        if r["hospital"] != NAO_NAV:
            p["exames_nav"] += e
            p["rel_nav"] += rel
            h = hosp[(k, r["uf"], r["hospital"])]
            h["exames"] += e
            h["relevantes"] += rel
            h["enviados"] += env
    nomes = [(h[:2], h[5:]) for h in dados["hospitais"]]
    tabela, resumo = [], []
    for k in sorted(chaves, key=lambda k: -por[k]["exames"]):
        sem = baixo = 0
        for uf, nome in nomes:
            h = hosp.get((k, uf, nome), {"exames": 0, "relevantes": 0, "enviados": 0})
            sit = "sem exame" if h["exames"] == 0 else ("baixo (≤ 30)" if h["exames"] <= BAIXO else "ok")
            sem += sit == "sem exame"
            baixo += sit.startswith("baixo")
            tabela.append({"linha": nome_linha(*k), "uf": uf, "hospital": nome, **h, "situacao": sit})
        resumo.append({"linha": nome_linha(*k), **por[k], "hospitais": len(nomes), "com_exame": len(nomes) - sem,
                       "sem_exame": sem, "baixo": baixo})
    return resumo, tabela


def planilha(resumo, tabela, rand, dados, destino: Path) -> None:
    wb = Workbook()
    lm = wb.active
    lm.title = "Leia-me"
    for a, b in [
        ("Visão por linha — dados do one page", ""),
        ("Janela", f"execuções do modelo de {dados['inicio']} a {dados['fim']} (30 dias), produção"),
        ("Exames (entrada)", "exames distintos que chegaram ao motor da linha, depois do filtro de entrada da linha"),
        ("Relevantes", "exames que o motor marcou como relevantes (todas as unidades, ou só as navegadas onde indicado)"),
        ("Enviados", "exames que constam do envio à navegação (OneDrive) — só unidades navegadas e só o grupo intervenção"),
        ("Hospitais", f"{len(dados['hospitais'])} hospitais da lista de navegação das linhas de captação — a validar (Natan e Rúbia)"),
        ("Situação", f"sem exame · baixo (≤ {BAIXO} exames no período) · ok"),
        ("Ca de Cólon e DII", "separados em colonoscopia × outros exames pelo tipo do exame; o Ca de Cólon só recebe colonoscopia"),
        ("Controle do cruzamento", f"{dados['controle_envio']['enviados']} exames enviados no período; "
                                   f"{dados['controle_envio']['sem_saida']} sem correspondência na saída da linha"),
        ("Não incluído", "pacientes captados (BI da Central), consumo e uplift — ver o one page"),
    ]:
        lm.append([a, b])
    lm.column_dimensions["A"].width = 26
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
            ws.column_dimensions[ws.cell(1, i).column_letter].width = max(12, min(45, len(c) + 4))
        ws.freeze_panes = "B2"
        ws.auto_filter.ref = ws.dimensions

    aba("Resumo por linha", ["Linha", "Exames (entrada)", "Exames em hospitais navegados", "Relevantes",
                             "Relevantes em navegados", "Enviados", "Hospitais navegados", "Com exame", "Sem exame",
                             "Baixo (≤ 30)"],
        [[r["linha"], r["exames"], r["exames_nav"], r["relevantes"], r["rel_nav"], r["enviados"], r["hospitais"],
          r["com_exame"], r["sem_exame"], r["baixo"]] for r in resumo])
    aba("Por hospital", ["Linha", "UF", "Hospital", "Exames (entrada)", "Relevantes", "Enviados", "Situação"],
        [[t["linha"], t["uf"], t["hospital"], t["exames"], t["relevantes"], t["enviados"], t["situacao"]] for t in tabela])
    aba("Randomização", ["Linha", "Segregação desde", "Sorteados", "Grupo controle", "% controle",
                         "Enviados fora da intervenção"],
        [[ROT_RAND.get(r["linha"], r["linha"]), r["segregacao"].replace("✅ desde ", ""), int(r["sorteados"]),
          int(r["controle"]), float(r["pct_controle"]), int(r["enviados_fora_intervencao"])] for r in rand])
    assert not destino.exists(), destino
    wb.save(destino)


def html(resumo, rand, dados, destino: Path, captados: dict | None = None) -> None:
    def ini_fim(s):
        a, m, d = s.split("-")
        return f"{d}/{m}/{a}"
    linhas = "".join(
        f"<tr><td>{r['linha']}</td><td class=n>{br(r['exames'])}</td><td class=n>{br(r['rel_nav'])}</td>"
        f"<td class=n><b>{br(r['enviados'])}</b></td><td class=n>{r['com_exame']}/{r['hospitais']}</td>"
        f"<td class='n {'alerta' if r['sem_exame'] >= 10 else ''}'>{r['sem_exame']}</td><td class=n>{r['baixo']}</td></tr>"
        for r in resumo)
    tot_ex = sum(r["exames"] for r in resumo)
    tot_env = sum(r["enviados"] for r in resumo)
    rand_l = "".join(
        f"<tr><td>{ROT_RAND.get(r['linha'], r['linha'])}</td><td>{r['segregacao'].replace('✅ desde ', '')}</td>"
        f"<td class=n>{br(int(r['sorteados']))}</td><td class=n><b>{r['controle']}</b></td>"
        f"<td class=n>{str(r['pct_controle']).replace('.', ',')}%</td><td class=n>{r['enviados_fora_intervencao']}</td></tr>"
        for r in rand)
    captados_html = ""
    if captados:
        cel = "".join(f"<td><b>{n}</b> <span class=lbl>{l}</span></td>" for l, n, _ in captados["linhas"])
        captados_html = (f'<h2>Pacientes captados (BI de controle da captação, {ini_fim(captados["inicio"])} a '
                         f'{ini_fim(captados["fim"])})</h2><table class=cap><tr>{cel}</tr></table>'
                         '<div class="nota">Pacientes distintos com entrada na navegação no período, por linha do BI (nomes do '
                         'BI). Inclui captação por outras origens, não só pelos algoritmos; registros até 10/11. '
                         'Por hospital: próxima versão (a base do BI não traz o hospital).</div>')
    pagina = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Visão por linha</title>
<style>
@page {{ size: A4 landscape; margin: 9mm; }}
body {{ font-family: Segoe UI, Arial, sans-serif; color: #1d2733; font-size: 9.6px; margin: 0; }}
h1 {{ font-size: 17px; margin: 0 0 2px; color: #0b4f6c; }}
.sub {{ color: #5a6672; margin-bottom: 7px; }}
.kpis {{ display: flex; gap: 8px; margin-bottom: 8px; }}
.kpi {{ flex: 1; border: 1px solid #d6dde4; border-radius: 6px; padding: 5px 8px; }}
.kpi b {{ display: block; font-size: 16px; color: #0b4f6c; }}
.cols {{ display: flex; gap: 12px; }}
.esq {{ flex: 1.55; }} .dir {{ flex: 1; }}
h2 {{ font-size: 11.5px; margin: 4px 0 3px; color: #0b4f6c; }}
table {{ border-collapse: collapse; width: 100%; }}
th {{ background: #e8f0f6; text-align: left; padding: 3px 4px; font-weight: 600; }}
td {{ border-bottom: 1px solid #e6eaee; padding: 2.4px 4px; }}
.n {{ text-align: right; font-variant-numeric: tabular-nums; }}
.alerta {{ color: #b3261e; font-weight: 700; }}
.cap td {{ border: none; padding: 1px 6px 1px 0; white-space: nowrap; }} .lbl {{ color: #5a6672; }} table.cap {{ width: auto; display: block; }}
.cap tr {{ display: flex; flex-wrap: wrap; gap: 2px 10px; }}
.nota {{ color: #5a6672; margin-top: 4px; line-height: 1.35; }}
.box {{ border-left: 3px solid #c9811a; background: #fbf6ee; padding: 5px 7px; margin-top: 6px; line-height: 1.4; }}
</style></head><body>
<h1>Captação por linha de cuidado — visão consolidada</h1>
<div class="sub">Produção · execuções de {ini_fim(dados['inicio'])} a {ini_fim(dados['fim'])} (30 dias) ·
{len(dados['hospitais'])} hospitais navegados (lista a validar) · ciência de dados</div>
<div class="kpis">
 <div class="kpi"><b>{br(tot_ex)}</b>exames na entrada (soma das linhas)</div>
 <div class="kpi"><b>{br(tot_env)}</b>envios à navegação (soma das linhas)</div>
 <div class="kpi"><b>{len(resumo)}</b>linhas e recortes em produção</div>
 <div class="kpi"><b>6</b>linhas com grupo controle (randomização)</div>
</div>
<div class="cols"><div class="esq">
<h2>Funil por linha: exames → relevantes → enviados</h2>
<table><tr><th>Linha</th><th class=n>Exames (entrada)</th><th class=n>Relevantes (hosp. navegados)</th>
<th class=n>Enviados</th><th class=n>Hospitais com exame</th><th class=n>Sem exame</th><th class=n>≤ 30 no mês</th></tr>
{linhas}</table>
{captados_html}
<div class="nota"><b>Exames</b>: entrada do algoritmo, depois do filtro de cada linha (não é o volume total do hospital).
<b>Enviados</b>: só unidades navegadas e só o grupo intervenção — por isso menor que os relevantes. Ca de Cólon e DII
separados por tipo de exame; o Ca de Cólon só recebe colonoscopia. Um exame pode entrar em mais de uma linha
(a mesma colonoscopia no Ca de Cólon e na DII): os totais somam linhas. Hospital por hospital: planilha anexa.</div>
</div><div class="dir">
<h2>Randomização (grupo controle)</h2>
<table><tr><th>Linha</th><th>Desde</th><th class=n>Sorteados</th><th class=n>Controle</th><th class=n>%</th>
<th class=n>Controle enviado</th></tr>{rand_l}</table>
<div class="nota">Controle = paciente encontrado e não enviado à navegação, para medir o efeito da navegação.
Esperado ~5%; "controle enviado" tem de ser 0.</div>
<h2>Pontos de atenção</h2>
<div class="box">
<b>Ca de Estômago</b>: 22 de 49 hospitais sem exame — o filtro de entrada não reconhece parte das endoscopias.<br>
<b>Transplante de pulmão</b>: 28 sem exame — esperado, função pulmonar só em parte dos hospitais.<br>
<b>Hepatologia</b>: sem execução em produção desde 14/10/2026.
</div>
<h2>Ainda não incluído</h2>
<div class="nota"><b>Pacientes captados por hospital</b>: a base do BI não traz o hospital — exige cruzar pelo paciente (próxima versão).<br>
<b>Consumo e uplift/margem</b>: as bases de consumo disponíveis param em 29/09/2026 (última carga 17/09);
o grupo controle começou em 30/10 — sem consumo posterior não há como medir o efeito.</div>
</div></div></body></html>"""
    assert not destino.exists(), destino
    destino.write_text(pagina, encoding="utf-8")


def main(arq_dados: str, arq_rand: str, pasta: str, versao: str = "v1", arq_captados: str = "") -> None:
    dados = json.loads(Path(arq_dados).read_text(encoding="utf-8"))
    rand = json.loads(Path(arq_rand).read_text(encoding="utf-8"))
    resumo, tabela = consolidar(dados)
    out = Path(pasta)
    out.mkdir(parents=True, exist_ok=True)
    planilha(resumo, tabela, rand, dados, out / f"visao-por-linha-{dados['fim']}-{versao}.xlsx")
    cap = json.loads(Path(arq_captados).read_text(encoding="utf-8")) if arq_captados else None
    html(resumo, rand, dados, out / f"visao-por-linha-{dados['fim']}-{versao}.html", cap)
    for r in resumo:
        print(r["linha"], r["exames"], r["rel_nav"], r["enviados"], r["com_exame"], r["sem_exame"], r["baixo"])


if __name__ == "__main__":
    main(*sys.argv[1:6])
