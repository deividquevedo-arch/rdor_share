"""Consolida o historico v2 por MES DO EXAME (linha x mes), a partir de mensal-por-linha-unidade-v2.json.

Regras (precisao):
- Mes = mes da data do exame. Carga retroativa deixa de inflar o mes em que rodou.
- Fonte por mes: a que OPEROU o mes (ver `escolher`). Figado: legado o ano todo (plataforma rodou
  em paralelo, sem entregar).
- Unidade: codigos antigos (ate jan/26) convertidos pelo `mapa-unidade-codigo-antigo.json`.
- Linha sem legado: o mes de estreia aparece marcado como parcial.
- Exame processado so retroativamente (antes de a fonte estar no ar) recebe a marca `retroativo`.
- Nefro e Mama: relevante = regra do `qtd_relevante` do monitoramento do proprio job, aplicada por
  exame (nefro: clearance 0,1-35, idade <= 80, sem AMIL, regional preenchida, fora de 43 unidades;
  mama: BI-RADS 4 ou 5). Arquivos nefro-/mama-relevante-regra-job.json.
- Biliares antes da plataforma: so existe a contagem do job (v1, monitoramento), por mes de execucao.
Gera mensal-consolidado-linha-v2.json. So agregados.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parent
MESES = [f"2026-{i:02d}" for i in range(1, 11)]
LEGADO_SEMPRE = {"hepatologia"}
PRIORIDADE_LEGADO = ["legado_motor", "legado_pacs_enave"]


def _anterior(m: str) -> str:
    y, mo = int(m[:4]), int(m[5:])
    return f"{y - 1}-12" if mo == 1 else f"{y}-{mo - 1:02d}"


def carregar():
    v2 = json.loads((OUT / "mensal-por-linha-unidade-v2.json").read_text(encoding="utf-8"))["linhas"]
    v1m = json.loads((OUT / "mensal-monitoramento-legado.json").read_text(encoding="utf-8"))["linhas"]
    # Nefro e Mama: relevante pela regra do proprio job (qtd_relevante do monitoramento), por exame.
    for linha, arq in (("nefrologia", "nefro-relevante-regra-job.json"), ("mama", "mama-relevante-regra-job.json")):
        sub = json.loads((OUT / arq).read_text(encoding="utf-8"))["linhas"]
        v2 = [r for r in v2 if not (r["linha"] == linha and r["fonte"] == "legado_motor")]
        v2 += [{"linha": linha, "fonte": "legado_motor", "recorte": "legado", "mes_execucao": s["mes_execucao"],
                "mes_exame": s["mes_exame"], "id_unidade": s["id_unidade"], "exames": int(s["exames"]),
                "relevantes": int(s["relevantes"])} for s in sub]
    return v2, v1m


def agregar(v2):
    """(linha, fonte, mes_exame) -> exames, relevantes, unidades{id: [exames, relevantes]}, retro."""
    tot = defaultdict(lambda: {"exames": 0, "relevantes": 0, "unidades": defaultdict(lambda: [0, 0]),
                               "exames_retroativos": 0})
    exec_ini = {}
    for r in v2:
        if r["mes_execucao"] and r["mes_execucao"] != "(sem data)":
            k = (r["linha"], r["fonte"])
            exec_ini[k] = min(exec_ini.get(k, "9999"), r["mes_execucao"])
    mapa = json.loads((OUT / "mapa-unidade-codigo-antigo.json").read_text(encoding="utf-8"))["mapa"]
    for r in v2:
        if r["fonte"] == "legado_motor_snapshot" or r["mes_exame"] not in MESES:
            continue
        r = dict(r, id_unidade=mapa.get(r["id_unidade"], {}).get("novo", r["id_unidade"]))
        rel = r["relevantes"]
        t = tot[(r["linha"], r["fonte"], r["mes_exame"])]
        t["exames"] += r["exames"] or 0
        t["relevantes"] += rel or 0
        u = t["unidades"][r["id_unidade"]]
        u[0] += r["exames"] or 0
        u[1] += rel or 0
        if r["mes_exame"] < exec_ini[(r["linha"], r["fonte"])]:
            t["exames_retroativos"] += r["exames"] or 0
    return tot, exec_ini


def _ultimo_dia(m: str) -> str:
    y, mo = int(m[:4]), int(m[5:])
    prox = f"{y + 1}-01" if mo == 12 else f"{y}-{mo + 1:02d}"
    return str(date.fromisoformat(prox + "-01") - timedelta(days=1))


def _cobertura():
    """(linha, fonte) -> (1a execucao, ultima execucao), de controles.json; ateromatose legado pelo espelho."""
    c = json.loads((OUT / "controles.json").read_text(encoding="utf-8"))["linhas"]
    cob = {}
    for x in c:
        if "dt_min" not in x:
            continue
        k = (x["linha"], x["fonte"])
        a, b = cob.get(k, ("9999", "0000"))
        cob[k] = (min(a, x["dt_min"]), max(b, x["dt_max"]))
    cob[("ateromatose", "legado_motor")] = ("2026-06-24", "2026-10-01")
    return cob


def escolher(linha, mes, tot, cob):
    """Fonte que operou o mes. Plataforma se ja estava no ar no inicio do mes (ate o dia 2); senao o
    legado, se rodou ate o fim do mes (tolerancia de 3 dias); senao a plataforma, se a janela
    retroativa da 1a execucao cobriu o mes inteiro; senao o que houver, marcado parcial."""
    plat = (linha, "plataforma_prd", mes)
    if linha not in LEGADO_SEMPRE and plat in tot and cob.get((linha, "plataforma_prd"), ("9999",))[0] <= mes + "-02":
        return "plataforma_prd", None
    fim = str(date.fromisoformat(_ultimo_dia(mes)) - timedelta(days=3))
    motor = (linha, "legado_motor", mes) in tot
    if motor and cob.get((linha, "legado_motor"), ("", "9999"))[1] >= fim:
        return "legado_motor", None
    if linha not in LEGADO_SEMPRE and plat in tot and (linha, "plataforma_prd", _anterior(mes)) in tot:
        tem_legado = any(k[0] == linha and k[1] != "plataforma_prd" for k in tot)
        return "plataforma_prd", ("mes coberto pela janela retroativa da 1a execucao"
                                  + ("; o legado parou no meio do mes" if tem_legado else ""))
    if motor:
        return "legado_motor", "legado parou no meio do mes (parcial)"
    if (linha, "legado_pacs_enave", mes) in tot:
        return "legado_pacs_enave", "captacao manual no PACS (enfermeira navegadora), nao e algoritmo"
    if plat in tot:
        return "plataforma_prd", "mes de estreia (parcial)"
    return None, None


def consolidar():
    v2, v1m = carregar()
    tot, exec_ini = agregar(v2)
    cob = _cobertura()
    linhas = sorted({k[0] for k in tot} | {"doencas_biliares"})
    out = []
    for linha in linhas:
        for mes in MESES:
            fonte, nota = escolher(linha, mes, tot, cob)
            if linha == "doencas_biliares" and (fonte is None or (nota or "").startswith("mes de estreia")):
                fonte = None  # o legado das biliares operou o mes (contagem do job); a estreia da plataforma so cobre parte
                ms = [m for m in v1m if m["linha"] == linha and m["mes"] == mes and m["fonte"] == "historico_legado_mlops"]
                if ms:
                    out.append({"linha": linha, "mes": mes, "fonte": "monitoramento_job_legado",
                                "base_do_mes": "execucao", "exames": sum(m["saida"] or 0 for m in ms),
                                "relevantes": sum(m["relevante"] or 0 for m in ms),
                                "hospitais_com_exame": None, "hospitais_com_relevante": None,
                                "nota": "contagem do job, nao deduplicada por exame; unidade por nome"})
                    continue
            if fonte is None:
                out.append({"linha": linha, "mes": mes, "fonte": None, "exames": None, "relevantes": None})
                continue
            t = tot[(linha, fonte, mes)]
            unid = {u: v for u, v in t["unidades"].items() if u != "(nulo)"}
            notas = [nota] if nota else []
            if t["exames_retroativos"]:
                notas.append(f"processado retroativamente: {t['exames_retroativos']} exames")
            outras = sorted({f for (l, f, m) in tot if l == linha and m == mes and f != fonte})
            out.append({"linha": linha, "mes": mes, "fonte": fonte, "base_do_mes": "data do exame",
                        "exames": t["exames"], "relevantes": t["relevantes"],
                        "hospitais_com_exame": sum(1 for v in unid.values() if v[0] > 0),
                        "hospitais_com_relevante": sum(1 for v in unid.values() if v[1] > 0),
                        "unidades": {u: {"exames": v[0], "relevantes": v[1]} for u, v in sorted(unid.items())},
                        "outras_fontes_no_mes": outras, "nota": "; ".join(notas) or None,
                        "fonte_no_ar_desde": exec_ini.get((linha, fonte))})
    (OUT / "mensal-consolidado-linha-v2.json").write_text(
        json.dumps({"meta": {"base": "mes da data do exame", "periodo": [MESES[0], MESES[-1]]}, "linhas": out},
                   ensure_ascii=False, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    for r in consolidar():
        if r["exames"] is None:
            continue
        print(f"{r['linha']:20} {r['mes']} {str(r['fonte']):26} ex {r['exames']:>7} rel {r['relevantes']:>6} "
              f"hosp {str(r.get('hospitais_com_exame')):>4}  {r.get('nota') or ''}")
