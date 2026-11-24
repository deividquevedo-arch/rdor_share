"""Monta os dados do relatório anual (jan–out/2026, 16 linhas): funil mensal por linha e abertura por hospital.

Entradas (nesta pasta e na pasta-mãe), todas já extraídas e só agregadas:
- mensal-consolidado-linha-v2-notas-v3.json  exames e relevantes por linha × mês × unidade (mês = data do exame), com notas
- mensal-monitoramento-legado.json           contagem do job legado por nome de unidade (biliares abr–ago; enviados do legado)
- enviados-mensal.json                       envios: legado (qtd_envio do job, por nome) e plataforma (log OneDrive, por id)
- captados-consumo-mensal.json               BI de controle da captação, por linha × mês
- ../unidades-ias-2026-11-19.json, ../unidades-gold-2026-11-19.json  as 80 unidades da planilha e o cadastro da gold

Regras:
- Totais = soma das 80 unidades da planilha + "demais unidades" (o que não é das 80), para conciliar hospital × consolidado.
- Enviados: legado = qtd_envio do monitoramento do job (mês da execução); plataforma = log de envio OneDrive (desde 23/10).
  O canal API repete exames do OneDrive (TI-RADS e reumato) e não é somado.
- Captados (CPF distinto no mês) e consumo (R$) vêm do BI por linha, sem hospital, e incluem todas as origens de captação.
Gera relatorio-mensal-dados.json.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

AQUI = Path(__file__).resolve().parent
MAE = AQUI.parent
sys.path.insert(0, str(MAE))
from cruzar_unidades_planilha import ids_por_hospital, norm  # noqa: E402

MESES = [f"2026-{i:02d}" for i in range(1, 11)]
# Nefro fev–mar: o monitoramento do job registrava a mais (data sobrescrita no reprocessamento; card 154348, PR 6019)
SEM_REGISTRO_NEFRO = {"2026-02", "2026-03"}
ROTULO = {
    "mama": "Mama (BI-RADS)", "hepatologia": "Fígado", "nefrologia": "Rim (nefrologia)", "pulmao_nodulo": "Pulmão – Nódulo",
    "cancer_colon": "Ca de Cólon", "dii": "DII", "reumatologia": "Reumatologia", "doencas_biliares": "Doenças biliares",
    "neuroimunologia": "Neuroimunologia", "pirads": "Próstata (PI-RADS)", "ateromatose": "Ateromatose",
    "tirads": "Tireoide (TI-RADS)", "cancer_estomago": "Ca de Estômago", "transplante_pulmao": "Transplante de pulmão",
    "cancer_rim": "Ca de Rim", "tumor_osseo": "Tumor ósseo",
}
ESPEC = {"ateromatose_coronariana": "ateromatose", "doenca_inflamatoria_intestinal": "dii"}
BI = {"CA Colon": "cancer_colon", "DII": "dii", "CA Rim": "cancer_rim", "Calculo Biliar": "doencas_biliares",
      "Reumato": "reumatologia", "CA Tireoide": "tirads", "Ateromatose": "ateromatose", "Neuro": "neuroimunologia",
      "CA Próstata": "pirads", "CA Estômago": "cancer_estomago", "Mama": "mama"}
CAMPOS = ("exames", "relevantes", "enviados")


def ler(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def planilha():
    v = ler(MAE / "unidades-ias-2026-11-19.json")
    cab = v[3]
    linhas = [r for r in v[4:] if r[4]]
    plan = [(r[cab.index("Estado")], r[4]) for r in linhas]
    vp = {r[4]: r[cab.index("VP")] for r in linhas}
    ids = ids_por_hospital(plan, ler(MAE / "unidades-gold-2026-11-19.json"))
    por_id = {i: nome for nome, lst in ids.items() for i in lst}
    por_nome = {norm(nome): nome for _, nome in plan}
    return plan, vp, por_id, por_nome, [n for n, i in ids.items() if not i]


def main() -> None:
    plan, vp, por_id, por_nome, sem_cad = planilha()
    cons = ler(AQUI / "mensal-consolidado-linha-v2-notas-v3.json")
    monit = ler(AQUI / "mensal-monitoramento-legado.json")["linhas"]
    env = ler(AQUI / "enviados-mensal.json")["linhas"]
    bi = ler(AQUI / "captados-consumo-mensal.json")

    # FUNIL NA BASE DA EXECUÇÃO: exames processados e relevantes marcados no mês em que o algoritmo rodou, somando
    # todos os algoritmos que operaram no mês (legado até parar, plataforma depois de subir). É a população de onde
    # sai o envio, então enviado fica contido em relevante. Fígado: só o legado (a plataforma rodou sem entregar).
    h = defaultdict(lambda: {c: 0 for c in CAMPOS})
    info = {}
    for r in cons["linhas"]:
        info[(r["linha"], r["mes"])] = {c: r.get(c) for c in ("nota_curta", "fonte_nota")}
    mapa_un = ler(AQUI / "mapa-unidade-codigo-antigo.json")["mapa"]
    v2 = ler(AQUI / "mensal-por-linha-unidade-v2.json")["linhas"]
    for linha_o, arq in (("nefrologia", "nefro-relevante-regra-job.json"), ("mama", "mama-relevante-regra-job.json")):
        v2 = [r for r in v2 if not (r["linha"] == linha_o and r["fonte"] == "legado_motor")]
        v2 += [{"linha": linha_o, "fonte": "legado_motor", "mes_execucao": x["mes_execucao"], "id_unidade": x["id_unidade"],
                "exames": int(x["exames"]), "relevantes": int(x["relevantes"])} for x in ler(AQUI / arq)["linhas"]]
    fontes_mes = defaultdict(set)
    for r in v2:
        if r["fonte"] not in ("legado_motor", "plataforma_prd") or r["mes_execucao"] not in MESES:
            continue
        if r["linha"] == "hepatologia" and r["fonte"] == "plataforma_prd":
            continue
        k = (r["linha"], r["mes_execucao"])
        hosp = por_id.get(mapa_un.get(r["id_unidade"], {}).get("novo", r["id_unidade"]))
        h[(*k, hosp)]["exames"] += r["exames"] or 0
        h[(*k, hosp)]["relevantes"] += r["relevantes"] or 0
        fontes_mes[k].add("legado" if r["fonte"] == "legado_motor" else "plataforma")
    # Transplante de pulmão, agosto: o envio saiu do job de HML (18 a 30/08); PRD só enviou a partir de 31/08
    for x in ler(AQUI / "transplante-hml-agosto.json")["linhas"]:
        k = ("transplante_pulmao", x["mes_execucao"])
        hosp = por_id.get(mapa_un.get(x["id_unidade"], {}).get("novo", x["id_unidade"]))
        h[(*k, hosp)]["exames"] += int(x["exames"])
        h[(*k, hosp)]["relevantes"] += int(x["relevantes"])
        fontes_mes[k].add("plataforma (hml)")
    for m in monit:  # Biliares no legado: só existe a contagem do próprio job (por nome de unidade)
        if m["linha"] == "doencas_biliares" and m["fonte"] == "historico_legado_mlops" and m["mes"] in MESES:
            k = ("doencas_biliares", m["mes"])
            hosp = por_nome.get(norm(m["unidade_nome"]))
            h[(*k, hosp)]["exames"] += m["saida"] or 0
            h[(*k, hosp)]["relevantes"] += m["relevante"] or 0
            fontes_mes[k].add("legado (contagem do job)")

    # Envios do legado em set–out: por projeto, a cópia do monitoramento que registra mais dias (hml.legado × hive)
    import extrair_historico as v1
    eh = ler(AQUI / "enviados-legado-hml-set-out.json")
    cob_env = {}
    troca = set()
    for proj, c in eh["cobertura_envio"].items():
        linha = v1.MAPA_PROJETO.get(proj)
        hml, hive = c.get("diamond_fabrica_ia_hml.legado", ""), c.get("hive_metastore.ia", "")
        cob_env[linha] = max(hml, hive)
        if hml > hive:
            troca.add(linha)
    env = [e for e in env if not (e["fonte"] == "legado_monitoramento_qtd_envio" and ESPEC.get(e["linha"], e["linha"]) in troca
                                  and e["mes"] >= "2026-09")]
    env += [{"linha": v1.MAPA_PROJETO.get(x["nm_projeto"]), "fonte": "legado_monitoramento_qtd_envio", "mes": x["mes"],
             "unidade_nome": x["unidade_nome"], "enviados": int(x["envio"] or 0)}
            for x in eh["linhas"] if v1.MAPA_PROJETO.get(x["nm_projeto"]) in troca]
    fonte_env = defaultdict(set)
    for e in env:
        linha = ESPEC.get(e["linha"], e["linha"])
        if e["mes"] not in MESES or linha not in ROTULO:
            continue
        if e["fonte"] == "legado_monitoramento_qtd_envio":
            hosp = por_nome.get(norm(e["unidade_nome"]))
        elif e["fonte"] == "plataforma_onedrive_prd":
            hosp = por_id.get(e["id_unidade"])
        else:
            continue
        h[(linha, e["mes"], hosp)]["enviados"] += e["enviados"] or 0
        fonte_env[(linha, e["mes"])].add(e["fonte"])

    cap = defaultdict(int)
    con = defaultdict(float)
    for x in bi["captados"]:
        if x["linha"] in BI:
            cap[(BI[x["linha"]], x["mes"])] += int(x["captados"])
    for x in bi["consumo"]:
        if x["linha"] in BI:
            con[(BI[x["linha"]], x["mes"])] += float(x["valor"] or 0)

    # Oficial: envios por linha × mês e datas de subida em produção (planilha do portfólio; gestão de captação)
    of = ler(AQUI / "oficial-portfolio-2026.json")
    # Nefro: "enviado" = clearance <= 35 (filtro do Salesforce, mesmo critério do relevante); Mama: o registro de
    # envio por hospital inclui todas as categorias de BI-RADS e não serve para hospital com envio.
    fonte_mes = {k: (" + ".join(sorted(v))) for k, v in fontes_mes.items()}
    # Envio por hospital: SÓ registro real de envio. Legado = contagem do job no dia; plataforma = log de envio
    # (desde 23/10); Mama = integração na API das navegadoras; Nefro = relevantes que o job registrou no dia, por unidade
    # (o envio da Nefro é o filtro <= 35 do Salesforce, mesmo critério). Nada é reconstruído.
    for k, v in h.items():
        if k[0] in ("mama", "nefrologia"):
            v["enviados"] = 0
    for m in monit:
        if m["linha"] == "nefrologia" and m["fonte"] == "monitoramento_hive" and m["mes"] in MESES and m["mes"] not in SEM_REGISTRO_NEFRO:
            h[("nefrologia", m["mes"], por_nome.get(norm(m["unidade_nome"])))]["enviados"] += m["relevante"] or 0
    # Mama: envio por hospital = integração concluída na API das navegadoras com BI-RADS 4 ou 5 (igual ao oficial jun–out)
    mapa_cod = ler(AQUI / "mapa-unidade-codigo-antigo.json")["mapa"]
    for x in ler(AQUI / "mama-envios-complemento.json")["linhas"]:
        uid = mapa_cod.get(x["id_unidade"], {}).get("novo", x["id_unidade"])
        h[("mama", x["mes"], por_id.get(uid))]["enviados"] += int(x["enviados"])

    # relevantes já estão na base da execução
    rel_exec = {k: v["relevantes"] for k, v in h.items()}
    rel_presenca = rel_exec

    funil, hospitais = [], []
    for linha in ROTULO:
        for mes in MESES:
            regs = {hp: v for (l, m, hp), v in h.items() if l == linha and m == mes}
            tot = {c: sum(v[c] for v in regs.values()) for c in CAMPOS}
            em80 = {hp: v for hp, v in regs.items() if hp}
            i = info.get((linha, mes), {})
            ativo = of["subida_producao"][linha][:7] <= mes
            fts = fontes_mes.get((linha, mes), set())
            so_plataforma = fts == {"plataforma"}
            registro_envio = ("sem registro (log de envio da plataforma desde 23/10)" if so_plataforma and mes < "2026-10"
                              else None)
            funil.append({
                "linha": linha, "rotulo": ROTULO[linha], "mes": mes, "ativa": ativo, "fonte": " + ".join(sorted(fts)) or None,
                **tot, **{f"{c}_80": sum(v[c] for v in em80.values()) for c in CAMPOS},
                "hosp_exame": sum(v["exames"] > 0 for v in em80.values()),
                # mesma base de data do envio (execução), para hospital com envio ficar contido em hospital com relevante
                "hosp_relevante": sum(v["relevantes"] > 0 for v in em80.values()),
                "hosp_envio": sum(v["enviados"] > 0 for v in em80.values()),
                "hospital_disponivel": bool(em80) or not fts,
                "captados": cap.get((linha, mes)) if linha in BI.values() else None,
                "consumo": round(con.get((linha, mes), 0.0), 2) if linha in BI.values() else None,
                "fonte_envio": sorted(fonte_env.get((linha, mes), [])),
                "registro_envio": registro_envio,
                # célula com texto na planilha oficial -> None (n/d); célula vazia -> 0 (sem envio no mês)
                "enviados_oficial": of["envios"].get(linha, {}).get(mes, 0),
                "enviados_nd": mes in of["envios"].get(linha, {}) and of["envios"][linha][mes] is None,
                "subida_producao": of["subida_producao"][linha],
                "hosp_envio_disponivel": not ((so_plataforma and mes < "2026-10")
                                              or (linha == "nefrologia" and mes in SEM_REGISTRO_NEFRO)),
                "nota": i.get("nota_curta"), "fonte_nota": i.get("fonte_nota"),
            })
            for hp, v in em80.items():
                rx = rel_exec.get((linha, mes, hp), 0)
                if any(v.values()) or rx:
                    hospitais.append({"linha": linha, "rotulo": ROTULO[linha], "mes": mes, "hospital": hp, "vp": vp.get(hp),
                                      "estado": next(uf for uf, n in plan if n == hp), **v, "relevantes_execucao": rx})

    # Conciliação: soma por hospital (80) + demais unidades = total da linha no mês
    for f in funil:
        s80 = {c: sum(x[c] for x in hospitais if x["linha"] == f["linha"] and x["mes"] == f["mes"]) for c in CAMPOS}
        assert all(s80[c] == f[f"{c}_80"] for c in CAMPOS), (f["linha"], f["mes"], s80)
    # Relevante incompleto na origem (enviado oficial > relevante medido): Pulmão jan (tabela do job desde 27/01),
    # Biliares mar (tabela do legado desde 03/04), Transplante ago. Não se fabrica número: fica "sem registro".
    for f in funil:
        f["relevantes_sem_registro"] = bool(f["ativa"] and f["linha"] != "nefrologia"
                                            and (f["enviados_oficial"] or 0) > f["relevantes"])
    for f in funil:
        if f["linha"] == "nefrologia":
            f["relevantes_recontagem"] = f["relevantes_80"]
            f["relevantes_80"] = f["relevantes"] = f["enviados_oficial"]
    nomes_gold = {g["id_unidade"]: g["nme_hospital"] for g in ler(MAE / "unidades-gold-2026-11-19.json")}
    fora = defaultdict(lambda: defaultdict(int))
    for r in cons["linhas"]:
        for uid, x in (r.get("unidades") or {}).items():
            u = mapa_un.get(uid, {}).get("novo", uid)
            if u not in por_id:
                fora[u][ROTULO.get(r["linha"], r["linha"])] += x["exames"]
    fora_80 = sorted(({"id_unidade": u, "nome": nomes_gold.get(u), "exames": sum(c.values()),
                       "linhas": ", ".join(f"{k} {v}" for k, v in sorted(c.items(), key=lambda kv: -kv[1]))}
                      for u, c in fora.items()), key=lambda x: -x["exames"])
    meta = {**cons["meta"], "planilha_80": len(plan), "sem_cadastro": sem_cad,
            "fora_80": fora_80,
            "captados_ate": bi["controle"][0]["cap_ate"], "consumo_ate": bi["controle"][0]["con_ate"][:10],
            "oficial": {k: of[k] for k in ("fonte", "em_producao", "captados_total", "consumo_total_mi", "observacoes",
                                           "fonte_subida")}}
    (AQUI / "relatorio-mensal-dados.json").write_text(json.dumps({"meta": meta, "funil": funil, "hospitais": hospitais},
                                                                ensure_ascii=False, indent=1), encoding="utf-8")
    print("funil", len(funil), "| hospital×linha×mês", len(hospitais))
    for linha in ROTULO:
        fs = [f for f in funil if f["linha"] == linha]
        print(f"{ROTULO[linha]:24} ex {sum(f['exames'] for f in fs):>9} (80: {sum(f['exames_80'] for f in fs):>9}) "
              f"rel {sum(f['relevantes'] for f in fs):>6} env {sum(f['enviados'] for f in fs):>7} "
              f"cap {sum(f['captados'] or 0 for f in fs):>5} R$ {sum(f['consumo'] or 0 for f in fs):>14,.0f}")


if __name__ == "__main__":
    main()
