"""Relatório de avaliação da randomização por onda (HML shadow ou PRD).

Uso:
  python gerar_relatorio_onda.py --onda 1 --ambiente hml --linhas reumatologia,tirads \
      --inicio 2026-09-30 --fim 2026-10-14 [--perfil nlp-platform] [--saida DIR]

Escreve relatorio-onda<N>-<ambiente>-<inicio>_<fim>.md com números agregados (sem dado de paciente).
Método e critérios: ../metodo-avaliacao-por-onda-v0.md.
"""
import argparse
import datetime as dt
import json
import math
import subprocess
from pathlib import Path

OP = "mlops_fabrica_ia.operacao"
TODAS = [
    "ateromatose_coronariana", "cancer_colon", "cancer_estomago", "cancer_rim",
    "doenca_inflamatoria_intestinal", "doencas_biliares", "hepatologia", "neuroimunologia",
    "pirads", "transplante_pulmao", "tumor_osseo", "reumatologia", "tirads",
]
NOMES = {
    "reumatologia": "Reumatologia", "tirads": "Tireoide (TI-RADS)", "doenca_inflamatoria_intestinal": "DII",
    "cancer_colon": "Câncer de cólon", "doencas_biliares": "Doenças biliares", "neuroimunologia": "Neuroimunologia",
    "pirads": "Câncer de próstata (PI-RADS)", "ateromatose_coronariana": "Ateromatose coronariana",
    "transplante_pulmao": "Transplante de pulmão", "cancer_estomago": "Câncer de estômago",
    "cancer_rim": "Câncer de rim", "tumor_osseo": "Tumor ósseo", "hepatologia": "Hepatologia",
}
# Critérios de aceite (ver o método). Alterar aqui altera o veredito de todas as ondas.
LIM_EXECUCAO = 0.90      # fração mínima de dias úteis com execução registrada
LIM_SEM_CPF = 0.02       # fração máxima de elegíveis sem CPF
LIM_Z = 3.0              # desvio máximo da fração de controle (agregado da onda)
DIAS_UTEIS_MES = 21


def consulta(sql, perfil):
    r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", sql, "--profile", perfil],
                       capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=900)
    saida = r.stdout + r.stderr
    i = saida.find("[")
    if i < 0:
        raise RuntimeError(saida.strip()[-400:])
    return json.loads(saida[i:])


def n(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def fmt(v, casas=0):
    s = f"{v:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def dias_uteis(ini, fim):
    d, out = ini, []
    while d <= fim:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def coletar(a):
    amb, ini, fim, linhas = a.ambiente, a.inicio, a.fim, a.linhas
    cat = "diamond_fabrica_ia" + ("" if amb == "prd" else f"_{amb}")
    lst = ", ".join(f"'{l}'" for l in linhas)
    per = f"to_date(dt_execucao_modelo) BETWEEN '{ini}' AND '{fim}'"

    rel = consulta(f"""
      SELECT nm_especialidade linha, to_date(dt_execucao_modelo) dia,
             max_by(st_execucao, dh_registro) st, max_by(coalesce(ds_etapas_falha,''), dh_registro) falhas,
             max_by(qt_exportados, dh_registro) exportados, max_by(qt_controle, dh_registro) controle,
             max_by(qt_intervencao, dh_registro) navegacao, max_by(qt_sem_chave, dh_registro) sem_cpf
      FROM {OP}.tb_mov_randomizacao_relatorio
      WHERE tp_ambiente = '{amb}' AND nm_especialidade IN ({lst}) AND {per}
      GROUP BY 1, 2""", a.perfil)

    grp = consulta(f"""
      SELECT nm_especialidade linha,
             count(DISTINCT num_cpf) randomizados,
             count(DISTINCT CASE WHEN tp_grupo='controle' THEN num_cpf END) controle,
             count(DISTINCT CASE WHEN tp_grupo='intervencao' THEN num_cpf END) navegacao,
             sum(CASE WHEN tp_grupo='controle' AND fl_enviado THEN 1 ELSE 0 END) controle_enviado,
             count(*) registros
      FROM {OP}.tb_mov_randomizacao_log
      WHERE tp_ambiente = '{amb}' AND nm_especialidade IN ({lst}) AND {per}
      GROUP BY 1""", a.perfil)

    integ = consulta(f"""
      WITH l AS (SELECT * FROM {OP}.tb_mov_randomizacao_log
                 WHERE tp_ambiente = '{amb}' AND nm_especialidade IN ({lst}) AND {per})
      SELECT (SELECT count(*) FROM l) registros,
             (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf HAVING count(DISTINCT tp_grupo) > 1)) dois_grupos,
             (SELECT count(*) FROM l LEFT JOIN {OP}.tb_mov_randomizacao_controle b
                ON lpad(cast(l.num_cpf AS string),11,'0') = lpad(cast(b.num_cpf AS string),11,'0')
              WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) divergentes,
             (SELECT avg(CASE WHEN tp_grupo='controle' THEN 1.0 ELSE 0.0 END) FROM {OP}.tb_mov_randomizacao_controle) fracao_base
      """, a.perfil)[0]

    funil = consulta(" UNION ALL ".join(
        f"""SELECT '{l}' linha, count(DISTINCT id_paciente) analisados,
                   count(DISTINCT CASE WHEN fl_relevante = 1 THEN id_paciente END) relevantes
            FROM {cat}.{l}.tb_mod_diamond_{l}_saida_v0 WHERE {per}""" for l in linhas), a.perfil)

    regional = consulta(f"""
      SELECT nm_especialidade linha, coalesce(uf_regional, '(sem regional)') regional,
             count(DISTINCT cpf_paciente) pacientes
      FROM {OP}.tb_mov_randomizacao_intervencao
      WHERE tp_ambiente = '{amb}' AND nm_especialidade IN ({lst}) AND {per}
      GROUP BY 1, 2""", a.perfil)

    rel_outras = " UNION ALL ".join(
        f"SELECT '{l}' linha, cast(id_paciente AS string) idp, to_date(dt_execucao_modelo) dia "
        f"FROM {cat}.{l}.tb_mod_diamond_{l}_saida_v0 WHERE fl_relevante = 1" for l in TODAS)
    contam = consulta(f"""
      WITH g AS (SELECT nm_especialidade linha, tp_grupo grupo, num_cpf cpf, cast(id_paciente AS string) idp,
                        min(to_date(dt_execucao_modelo)) d0
                 FROM {OP}.tb_mov_randomizacao_log
                 WHERE tp_ambiente = '{amb}' AND nm_especialidade IN ({lst}) AND {per} GROUP BY 1,2,3,4),
           r AS ({rel_outras})
      SELECT g.linha, g.grupo, count(DISTINCT g.cpf) pacientes,
             count(DISTINCT CASE WHEN r.linha IS NOT NULL THEN g.cpf END) em_outra_linha
      FROM g LEFT JOIN r ON r.idp = g.idp AND r.linha <> g.linha AND r.dia >= g.d0
      GROUP BY 1, 2""", a.perfil)
    return rel, grp, integ, funil, regional, contam


def avaliar(a, rel, grp, integ, funil, regional, contam):
    ini = dt.date.fromisoformat(a.inicio)
    fim = dt.date.fromisoformat(a.fim)
    uteis = dias_uteis(ini, fim)
    p = n(integ["fracao_base"])
    por = {}
    for l in a.linhas:
        rl = [x for x in rel if x["linha"] == l]
        dias_exec = {x["dia"] for x in rl}
        falhas = [x["dia"] for x in rl if x["st"] != "SUCESSO" or x["falhas"]]
        g = next((x for x in grp if x["linha"] == l), {})
        f = next((x for x in funil if x["linha"] == l), {})
        eleg = sum(n(x["exportados"]) for x in rl)
        sem_cpf = sum(n(x["sem_cpf"]) for x in rl)
        por[l] = {
            "uteis": len(uteis), "executados": sum(1 for d in uteis if d.isoformat() in dias_exec),
            "falhas": sorted(falhas), "analisados": n(f.get("analisados")), "relevantes": n(f.get("relevantes")),
            "randomizados": n(g.get("randomizados")), "controle": n(g.get("controle")),
            "navegacao": n(g.get("navegacao")), "controle_enviado": n(g.get("controle_enviado")),
            "eleg_exames": eleg, "sem_cpf": sem_cpf,
            "regional": sorted([x for x in regional if x["linha"] == l], key=lambda x: -n(x["pacientes"])),
            "contam": {x["grupo"]: x for x in contam if x["linha"] == l},
        }
    tot_r = sum(v["randomizados"] for v in por.values())
    tot_c = sum(v["controle"] for v in por.values())
    z = (tot_c - tot_r * p) / math.sqrt(tot_r * p * (1 - p)) if tot_r else 0.0
    return uteis, p, por, tot_r, tot_c, z


def criterios(a, integ, por, tot_r, z):
    out = []
    for l, v in por.items():
        exec_ok = v["uteis"] and v["executados"] / v["uteis"] >= LIM_EXECUCAO and not v["falhas"]
        out.append((f"Execução — {NOMES.get(l, l)}",
                    f"{v['executados']} de {v['uteis']} dias úteis; falhas: {', '.join(v['falhas']) or 'nenhuma'}",
                    "OK" if exec_ok else "ALERTA"))
        if v["randomizados"] == 0:
            out.append((f"Randomização ativa — {NOMES.get(l, l)}", "nenhum paciente randomizado no período", "ALERTA"))
        semcpf = v["sem_cpf"] / v["eleg_exames"] if v["eleg_exames"] else 0
        out.append((f"Sem CPF — {NOMES.get(l, l)}", f"{fmt(100 * semcpf, 1)}% dos elegíveis",
                    "OK" if semcpf <= LIM_SEM_CPF else "ALERTA"))
    ctrl_env = sum(v["controle_enviado"] for v in por.values())
    out.append(("Controle enviado (onda)", f"{fmt(ctrl_env)} registros", "OK" if ctrl_env == 0 else "ALERTA"))
    out.append(("CPF com dois grupos (onda)", f"{integ['dois_grupos']} em {integ['registros']} registros",
                "OK" if n(integ["dois_grupos"]) == 0 else "ALERTA"))
    out.append(("Aderência à base (onda)", f"{integ['divergentes']} divergentes",
                "OK" if n(integ["divergentes"]) == 0 else "ALERTA"))
    out.append(("Proporção de controle (onda)", f"z = {fmt(z, 2)} sobre {fmt(tot_r)} pacientes",
                "OK" if tot_r and abs(z) <= LIM_Z else "ALERTA"))
    return out


def escrever(a, uteis, p, por, tot_r, tot_c, z, crit):
    L = []
    ok = all(c[2] == "OK" for c in crit)
    L += [f"# Randomização — relatório da onda {a.onda}", "",
          f"**Ambiente:** {a.ambiente.upper()} · **Período:** {a.inicio} a {a.fim} ({len(uteis)} dias úteis) · "
          f"**Linhas:** {', '.join(NOMES.get(l, l) for l in a.linhas)} · **Fração de controle vigente:** {fmt(100 * p, 2)}%",
          f"Gerado em {dt.date.today().isoformat()} por `gerar_relatorio_onda.py`. Números agregados; nenhum dado de paciente.", "",
          f"## Veredito: {'✅ APTA' if ok else '⚠️ COM PENDÊNCIAS'}", "",
          "| critério | resultado | status |", "|---|---|---|"]
    L += [f"| {c[0]} | {c[1]} | {'✅ OK' if c[2] == 'OK' else '⚠️ ALERTA'} |" for c in crit]
    L += ["", "## Resumo por linha (pacientes)", "",
          "| linha | analisados | relevantes | randomizados | navegação | controle | % controle | navegação/dia útil | controle/mês (projeção) |",
          "|---|---|---|---|---|---|---|---|---|"]
    for l, v in por.items():
        pct = 100 * v["controle"] / v["randomizados"] if v["randomizados"] else 0
        nav_dia = v["navegacao"] / v["executados"] if v["executados"] else 0
        ctrl_mes = v["controle"] / v["executados"] * DIAS_UTEIS_MES if v["executados"] else 0
        L.append(f"| {NOMES.get(l, l)} | {fmt(v['analisados'])} | {fmt(v['relevantes'])} | {fmt(v['randomizados'])} | "
                 f"{fmt(v['navegacao'])} | {fmt(v['controle'])} | {fmt(pct, 1)}% | {fmt(nav_dia, 1)} | {fmt(ctrl_mes, 1)} |")
    pct_t = 100 * tot_c / tot_r if tot_r else 0
    L.append(f"| **Onda {a.onda}** | {fmt(sum(v['analisados'] for v in por.values()))} | "
             f"{fmt(sum(v['relevantes'] for v in por.values()))} | **{fmt(tot_r)}** | "
             f"**{fmt(sum(v['navegacao'] for v in por.values()))}** | **{fmt(tot_c)}** | **{fmt(pct_t, 1)}%** | | |")
    L += ["", "> Paciente conta uma vez por linha. *Analisados* e *relevantes* vêm da saída do motor no período; "
              "*randomizados* são os elegíveis ao envio (após deduplicação e validações), que é onde o sorteio se aplica.",
          "> A proporção de controle só é avaliada no **agregado da onda**: por linha, o número de controles em 10 dias úteis "
          "é pequeno demais para leitura isolada.", ""]
    L += ["## Navegação por regional (pacientes enviados)", ""]
    for l, v in por.items():
        if v["regional"]:
            L.append(f"- **{NOMES.get(l, l)}:** " + " · ".join(f"{x['regional']} {fmt(n(x['pacientes']))}" for x in v["regional"]))
    L += ["", "## Contaminação entre linhas (informativo)", "",
          "| linha | grupo | pacientes | também relevantes em outra linha |", "|---|---|---|---|"]
    for l, v in por.items():
        for g in ("controle", "intervencao"):
            x = v["contam"].get(g)
            if x:
                L.append(f"| {NOMES.get(l, l)} | {'controle' if g == 'controle' else 'navegação'} | "
                         f"{fmt(n(x['pacientes']))} | {fmt(n(x['em_outra_linha']))} |")
    L += ["", "> Paciente de controle relevante numa linha **não randomizada** pode ser navegado por ela. "
              "Quanto mais linhas randomizadas, menor esse efeito, porque o grupo é do CPF e vale para todas.", "",
          "## O que este relatório não mede", "",
          "- **Impacto financeiro.** Exige o desfecho (consumo) com janela completa — a fonte tem ~2 semanas de atraso — "
          "e acúmulo de pacientes por meses. É a fase 2 do método.",
          "- **Acerto clínico da régua.** A régua de cada linha já foi homologada; este relatório avalia a randomização "
          "e a operação, não o laudo.", ""]
    nome = f"relatorio-onda{a.onda}-{a.ambiente}-{a.inicio}_{a.fim}.md"
    destino = Path(a.saida) / nome
    destino.write_text("\n".join(L), encoding="utf-8")
    return destino, ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--onda", required=True)
    ap.add_argument("--ambiente", required=True, choices=["hml", "prd"])
    ap.add_argument("--linhas", required=True, type=lambda s: [x.strip() for x in s.split(",") if x.strip()])
    ap.add_argument("--inicio", required=True)
    ap.add_argument("--fim", required=True)
    ap.add_argument("--perfil", default="nlp-platform")
    ap.add_argument("--saida", default=str(Path(__file__).parent.parent / "relatorios"))
    a = ap.parse_args()
    Path(a.saida).mkdir(parents=True, exist_ok=True)
    dados = coletar(a)
    uteis, p, por, tot_r, tot_c, z = avaliar(a, *dados)
    crit = criterios(a, dados[2], por, tot_r, z)
    destino, ok = escrever(a, uteis, p, por, tot_r, tot_c, z, crit)
    print(f"{destino.name}: {'APTA' if ok else 'COM PENDENCIAS'} · {int(tot_r)} randomizados · {int(tot_c)} controle")


if __name__ == "__main__":
    main()
