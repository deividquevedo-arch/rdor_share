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
import re
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


def escopo_unidades(linha, repo, ambiente):
    """Lista `IN (...)` de unidades da validação da navegação da linha, lida da config da plataforma (main)."""
    pasta = "prd" if ambiente == "prd" else ambiente
    r = subprocess.run(["git", "-C", repo, "show",
                        f"origin/main:plataform/config/exchange/{pasta}/ntb_ia_{linha}_navegacao.py"],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    t = r.stdout
    i = t.find('"e.id_unidade"')
    if r.returncode or i < 0:
        return None
    ini = t.find("IN (", i)
    fim = t.find(")", ini)
    return "(" + "".join(re.findall(r"'[^']*'", t[ini:fim])).replace("''", "', '") + ")"


def escopo_vigente_desde(linha, repo, ambiente):
    """1º dia de execução da versão atual da config de navegação (última mudança na main).

    Merge depois das 04:00 (BRT) só vale na execução do dia seguinte.
    """
    pasta = "prd" if ambiente == "prd" else ambiente
    r = subprocess.run(["git", "-C", repo, "log", "origin/main", "--first-parent", "-1", "--format=%cI",
                        "--", f"plataform/config/exchange/{pasta}/ntb_ia_{linha}_navegacao.py"],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode or not r.stdout.strip():
        return None
    quando = dt.datetime.fromisoformat(r.stdout.strip()).astimezone(dt.timezone(dt.timedelta(hours=-3)))
    dia = quando.date()
    return (dia + dt.timedelta(days=1)) if quando.hour >= 4 else dia


def funil_exames(a):
    """Exames por etapa da view de exportação: relevantes → unidade navegada → sem repetição."""
    cat = "diamond_fabrica_ia" + ("" if a.ambiente == "prd" else f"_{a.ambiente}")
    per = f"CAST(s.dt_execucao_modelo AS DATE) BETWEEN '{a.inicio}' AND '{a.fim}'"
    out = {}
    for l in a.linhas:
        ids = escopo_unidades(l, a.repo, a.ambiente)
        desde = escopo_vigente_desde(l, a.repo, a.ambiente) if ids else None
        if ids and desde:
            filtro_un = f"WHERE dia < DATE'{desde}' OR CAST(id_unidade AS STRING) IN {ids}"
        elif ids:
            filtro_un = f"WHERE CAST(id_unidade AS STRING) IN {ids}"
        else:
            filtro_un = ""
        r = consulta(f"""
          WITH s AS (SELECT s.id_exame, CAST(s.dt_execucao_modelo AS DATE) AS dia
                     FROM {cat}.{l}.tb_mod_diamond_{l}_saida_v0 s
                     WHERE s.fl_relevante = 1 AND {per}),
               j AS (SELECT s.dia, s.id_exame, e.id_unidade,
                            sha2(concat_ws('|', lower(trim(CAST(e.id_patient AS STRING))),
                                 date_format(to_date(e.dt_exame), 'yyyy-MM-dd'),
                                 lower(trim(e.proced_descricao_ajustado))), 256) AS h
                     FROM s JOIN gold_corporativo_ia.corporativo.tb_gold_mov_exame e ON s.id_exame = e.id_exame),
               u AS (SELECT * FROM j {filtro_un}),
               -- a view roda por dia: conta por dia e soma, como a soma diária dos exportados
               ds AS (SELECT dia, count(DISTINCT id_exame) n FROM s GROUP BY dia),
               du AS (SELECT dia, count(DISTINCT id_exame) n, count(DISTINCT h) nh FROM u GROUP BY dia)
          SELECT (SELECT coalesce(sum(n), 0) FROM ds) AS relevantes,
                 (SELECT coalesce(sum(n), 0) FROM du) AS unidade_navegada,
                 (SELECT coalesce(sum(nh), 0) FROM du) AS sem_repeticao""", a.perfil)[0]
        out[l] = {k: n(v) for k, v in r.items()} | {"tem_escopo": ids is not None,
                                                     "n_unidades": len(re.findall(r"'", ids or "")) // 2,
                                                     "escopo_desde": desde.isoformat() if desde else None}
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
            "ex_controle": sum(n(x["controle"]) for x in rl), "ex_navegacao": sum(n(x["navegacao"]) for x in rl),
            "diario": sorted((x["dia"], n(x["navegacao"]), n(x["controle"])) for x in rl),
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
          "> A proporção de controle só é avaliada no **agregado da onda**: por linha, o número de controles no período "
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
          "e acúmulo de pacientes por meses. É a fase 3 do método (uplift), depois do de acordo e com a linha em PRD.",
          "- **Acerto clínico da régua.** A régua de cada linha já foi homologada; este relatório avalia a randomização "
          "e a operação, não o laudo.", ""]
    nome = f"relatorio-onda{a.onda}-{a.ambiente}-{a.inicio}_{a.fim}.md"
    destino = Path(a.saida) / nome
    destino.write_text("\n".join(L), encoding="utf-8")
    return destino, ok


def wilson(k, nn, zc=1.96):
    """Intervalo de confiança de 95% (Wilson) para a proporção k/nn."""
    if not nn:
        return 0.0, 0.0
    ph = k / nn
    den = 1 + zc * zc / nn
    centro = (ph + zc * zc / (2 * nn)) / den
    meia = zc * math.sqrt(ph * (1 - ph) / nn + zc * zc / (4 * nn * nn)) / den
    return max(0.0, centro - meia), min(1.0, centro + meia)


CSS = """
:root{--bg:#f4f7f7;--surface:#fff;--fg:#14262a;--muted:#55696d;--line:#d6e0e2;--accent:#0e6e74;--accent-soft:#e0efef;
--nav:#0e6e74;--ctrl:#c07a12;--ctrl-soft:#f6ecd9;--ok:#2f7d4f;--ok-soft:#e3f2e8;--bad:#a6382f;--bad-soft:#f6e3e1;
--f-d:"IBM Plex Sans Condensed","Arial Narrow",system-ui,sans-serif;--f-b:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;
--f-m:"IBM Plex Mono",ui-monospace,Consolas,monospace}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#0f181b;--surface:#152227;--fg:#e3ecee;--muted:#93a8ad;
--line:#2a3b41;--accent:#5cc2c6;--accent-soft:#16363a;--nav:#5cc2c6;--ctrl:#e2b05c;--ctrl-soft:#3a2f14;--ok:#7fcf9c;--ok-soft:#163223;
--bad:#ec8f86;--bad-soft:#3d1d1a;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#0f181b;--surface:#152227;--fg:#e3ecee;--muted:#93a8ad;--line:#2a3b41;--accent:#5cc2c6;
--accent-soft:#16363a;--nav:#5cc2c6;--ctrl:#e2b05c;--ctrl-soft:#3a2f14;--ok:#7fcf9c;--ok-soft:#163223;--bad:#ec8f86;--bad-soft:#3d1d1a;color-scheme:dark}
*{box-sizing:border-box}body{background:var(--bg);color:var(--fg);font:15px/1.55 var(--f-b)}
.wrap{max-width:1040px;margin:0 auto;padding-block:36px 56px;padding-left:max(16px,env(safe-area-inset-left,0px));
padding-right:max(16px,env(safe-area-inset-right,0px));display:flex;flex-direction:column;gap:36px}
header{display:flex;flex-direction:column;gap:8px;border-bottom:2px solid var(--fg);padding-bottom:18px}
.eyebrow{font:500 12px/1 var(--f-m);letter-spacing:.08em;text-transform:uppercase;color:var(--accent)}
h1{font:700 clamp(28px,4.6vw,40px)/1.08 var(--f-d);margin:0;text-wrap:balance}
h2{font:600 21px/1.2 var(--f-d);margin:0 0 12px}
.meta{font:400 13px/1.5 var(--f-m);color:var(--muted)}
section{display:flex;flex-direction:column}
.verdict{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.badge{font:600 14px/1 var(--f-m);padding:8px 12px;border-radius:999px}
.ok{background:var(--ok-soft);color:var(--ok)}.bad{background:var(--bad-soft);color:var(--bad)}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.kpi{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:14px 16px;display:flex;flex-direction:column;gap:4px;min-width:0}
.kpi .k{font:500 12px/1.3 var(--f-m);letter-spacing:.04em;text-transform:uppercase;color:var(--muted)}
.kpi .v{font:600 30px/1.1 var(--f-d);font-variant-numeric:tabular-nums}
.kpi .s{font-size:13px;color:var(--muted)}
.kpi.nav .v{color:var(--nav)}.kpi.ctrl .v{color:var(--ctrl)}
.tbl{overflow-x:auto;border:1px solid var(--line);border-radius:8px;background:var(--surface)}
table{border-collapse:collapse;width:100%;min-width:640px}
th,td{text-align:left;padding:9px 12px;border-bottom:1px solid var(--line);vertical-align:top}
tr:last-child td{border-bottom:0}
th{font:600 11.5px/1.3 var(--f-m);letter-spacing:.04em;text-transform:uppercase;color:var(--muted);background:var(--bg)}
td.n{font-family:var(--f-m);font-variant-numeric:tabular-nums;text-align:right;white-space:nowrap}
th.n{text-align:right}
tr.total td{font-weight:600;background:var(--accent-soft)}
.pill{font:500 12px/1 var(--f-m);padding:4px 8px;border-radius:999px;white-space:nowrap}
.bar{display:flex;height:14px;border-radius:7px;overflow:hidden;background:var(--line);min-width:120px}
.bar span{display:block;height:100%}
.note{font-size:13.5px;color:var(--muted);max-width:80ch;margin-top:10px}
.prop{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:16px;display:flex;flex-direction:column;gap:10px}
.prop .row{display:flex;gap:28px;flex-wrap:wrap}
.prop .lbl{font:500 12px/1.3 var(--f-m);text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
.prop .val{font:600 24px/1.1 var(--f-d);font-variant-numeric:tabular-nums}
ul{margin:0;padding-left:18px;display:flex;flex-direction:column;gap:6px;max-width:80ch}
footer{font:400 12.5px/1.6 var(--f-m);color:var(--muted);border-top:1px solid var(--line);padding-top:14px}
"""


NUM = ' class="n"'

# ---------------------------------------------------------------- gráficos (SVG inline, cores do tema)
TXT = 'font-family="IBM Plex Mono, monospace" font-size="11" style="fill:var(--muted)"'
LBL = 'font-family="IBM Plex Sans, sans-serif" font-size="12.5" style="fill:var(--fg)"'


def _escala(maximo):
    """Topo 'redondo' do eixo e passo das marcas."""
    if maximo <= 0:
        return 1, 1
    exp = 10 ** math.floor(math.log10(maximo))
    for m in (1, 2, 2.5, 5, 10):
        if m * exp >= maximo:
            topo = m * exp
            break
    return topo, topo / 4


def svg_funil(linhas):
    """linhas: [(nome, [(rótulo, valor), ...])] — barras horizontais por etapa, escala comum."""
    etapas = len(linhas[0][1])
    alt_bloco = 22 + etapas * 22
    W, L, R = 760, 190, 70
    H = 10 + alt_bloco * len(linhas)
    vmax = max(v for _, et in linhas for _, v in et) or 1
    cores = ["var(--muted)", "var(--accent)", "var(--accent)", "var(--nav)"]
    op = ["0.35", "0.55", "0.8", "1"]
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Funil por linha, do achado relevante ao sorteio">']
    y = 10
    for nome, et in linhas:
        p.append(f'<text x="0" y="{y + 12}" {LBL} font-weight="600">{nome}</text>')
        for k, (rot, v) in enumerate(et):
            yy = y + 20 + k * 22
            w = (W - L - R) * v / vmax
            p.append(f'<text x="{L - 8}" y="{yy + 12}" text-anchor="end" {TXT}>{rot}</text>')
            p.append(f'<rect x="{L}" y="{yy}" width="{w:.1f}" height="16" rx="3" '
                     f'style="fill:{cores[min(k, 3)]};fill-opacity:{op[min(k, 3)]}"/>')
            p.append(f'<text x="{L + w + 6:.1f}" y="{yy + 12}" {TXT} style="fill:var(--fg)">{fmt(v)}</text>')
        y += alt_bloco
    p.append("</svg>")
    return "".join(p)


def svg_diario(dias, nav, ctrl, titulo):
    """Colunas empilhadas por dia: navegação embaixo, controle em cima."""
    W, H, L, B, T = 760, 240, 40, 34, 14
    top, passo = _escala(max((a + b) for a, b in zip(nav, ctrl)) if dias else 1)
    larg = (W - L - 10) / max(len(dias), 1)
    bw = min(46, larg * 0.6)
    Y = lambda v: T + (H - T - B) * (1 - v / top)
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{titulo}">']
    for i in range(5):
        v = passo * i
        p.append(f'<line x1="{L}" y1="{Y(v):.1f}" x2="{W - 10}" y2="{Y(v):.1f}" style="stroke:var(--line)"/>')
        p.append(f'<text x="{L - 6}" y="{Y(v) + 4:.1f}" text-anchor="end" {TXT}>{fmt(v)}</text>')
    for i, d in enumerate(dias):
        cx = L + larg * i + larg / 2
        y_nav = Y(nav[i])
        p.append(f'<rect x="{cx - bw / 2:.1f}" y="{y_nav:.1f}" width="{bw:.1f}" height="{Y(0) - y_nav:.1f}" '
                 f'style="fill:var(--nav)"/>')
        if ctrl[i]:
            y_ct = Y(nav[i] + ctrl[i])
            p.append(f'<rect x="{cx - bw / 2:.1f}" y="{y_ct:.1f}" width="{bw:.1f}" height="{y_nav - y_ct:.1f}" '
                     f'style="fill:var(--ctrl)"/>')
        p.append(f'<text x="{cx:.1f}" y="{Y(nav[i] + ctrl[i]) - 5:.1f}" text-anchor="middle" {TXT} '
                 f'style="fill:var(--fg)">{fmt(nav[i] + ctrl[i])}</text>')
        p.append(f'<text x="{cx:.1f}" y="{H - 14}" text-anchor="middle" {TXT}>{d[8:10]}/{d[5:7]}</text>')
    p.append("</svg>")
    return "".join(p)


def svg_proporcao(real, lo, hi, esperado):
    """Ponto do real com intervalo de 95% e a linha do esperado, em escala de 0 a 12%."""
    W, H, L, R = 760, 96, 20, 20
    vmax = max(0.12, hi * 1.15)
    X = lambda v: L + (W - L - R) * v / vmax
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Proporção de controle real com intervalo e esperada">']
    for v in [i / 100 for i in range(0, int(vmax * 100) + 1, 2)]:
        p.append(f'<line x1="{X(v):.1f}" y1="22" x2="{X(v):.1f}" y2="62" style="stroke:var(--line)"/>')
        p.append(f'<text x="{X(v):.1f}" y="80" text-anchor="middle" {TXT}>{fmt(100 * v)}%</text>')
    p.append(f'<rect x="{X(lo):.1f}" y="36" width="{X(hi) - X(lo):.1f}" height="12" rx="6" '
             f'style="fill:var(--ctrl);fill-opacity:.25"/>')
    p.append(f'<line x1="{X(esperado):.1f}" y1="16" x2="{X(esperado):.1f}" y2="66" '
             f'style="stroke:var(--accent);stroke-width:2;stroke-dasharray:4 3"/>')
    p.append(f'<text x="{X(esperado) + 6:.1f}" y="14" {TXT} style="fill:var(--accent)">esperado {fmt(100 * esperado, 1)}%</text>')
    p.append(f'<circle cx="{X(real):.1f}" cy="42" r="7" style="fill:var(--ctrl)"/>')
    p.append(f'<text x="{X(real):.1f}" y="30" text-anchor="middle" {TXT} style="fill:var(--ctrl)">real {fmt(100 * real, 1)}%</text>')
    p.append("</svg>")
    return "".join(p)


def svg_barras_h(itens, cor="var(--nav)"):
    """itens: [(rótulo, valor)] — barras horizontais simples."""
    if not itens:
        return ""
    W, L, R = 760, 70, 50
    H = 8 + 26 * len(itens)
    vmax = max(v for _, v in itens) or 1
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Pacientes enviados por regional">']
    for i, (rot, v) in enumerate(itens):
        y = 6 + 26 * i
        w = (W - L - R) * v / vmax
        p.append(f'<text x="{L - 8}" y="{y + 13}" text-anchor="end" {LBL}>{rot}</text>')
        p.append(f'<rect x="{L}" y="{y}" width="{w:.1f}" height="18" rx="3" style="fill:{cor}"/>')
        p.append(f'<text x="{L + w + 6:.1f}" y="{y + 13}" {TXT} style="fill:var(--fg)">{fmt(v)}</text>')
    p.append("</svg>")
    return "".join(p)


# Horizonte do uplift — premissas medidas em 19/10/2026 (docs/randomizacao/horizonte-uplift-por-linha-v0.md)
CV_CONSUMO = 3.5        # coeficiente de variação do consumo em 30 dias, sem o 1% mais alto
CONV_BASE = 0.58        # % de pacientes relevantes que consomem em 30 dias
Z_PODER = 1.959964 + 0.841621   # IC 95% e poder de 80%


def meses_conv(d, f, vol):
    return Z_PODER ** 2 * CONV_BASE * (1 - CONV_BASE) / d ** 2 * (1 / f + 1 / (1 - f)) / vol


def meses_valor(mde, f, vol):
    return (Z_PODER * CV_CONSUMO / mde) ** 2 * (1 / f + 1 / (1 - f)) / vol


def volume_mensal(a, desde):
    """Pacientes sorteados por mês desde o escopo atual até o dado mais recente (projeção, não a janela do relatório)."""
    lst = ", ".join(f"'{l}'" for l in a.linhas)
    r = consulta(f"""
      SELECT nm_especialidade linha, count(DISTINCT num_cpf) pacientes, count(DISTINCT to_date(dt_execucao_modelo)) dias,
             max(to_date(dt_execucao_modelo)) ate
      FROM {OP}.tb_mov_randomizacao_log
      WHERE tp_ambiente = '{a.ambiente}' AND nm_especialidade IN ({lst})
        AND to_date(dt_execucao_modelo) >= '{desde}'
      GROUP BY 1""", a.perfil)
    a.horizonte_periodo = (desde, max((x["ate"] for x in r), default=desde))
    vol = {x["linha"]: n(x["pacientes"]) / max(n(x["dias"]), 1) * DIAS_UTEIS_MES for x in r}
    vol["onda"] = sum(vol.values())
    return vol


def svg_horizonte(cenarios):
    """cenarios: [(rótulo, meses_5, meses_10)] — barras horizontais pareadas, escala comum."""
    W, L, R = 760, 200, 70
    H = 10 + 46 * len(cenarios)
    vmax = max(max(m5, m10) for _, m5, m10 in cenarios) or 1
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Meses até o resultado com 5% e 10% de controle">']
    for i, (rot, m5, m10) in enumerate(cenarios):
        y = 8 + 46 * i
        p.append(f'<text x="{L - 10}" y="{y + 19}" text-anchor="end" {LBL}>{rot}</text>')
        for k, (m, cor) in enumerate(((m5, "var(--ctrl)"), (m10, "var(--nav)"))):
            yy = y + k * 18
            w = (W - L - R) * m / vmax
            p.append(f'<rect x="{L}" y="{yy}" width="{w:.1f}" height="15" rx="3" style="fill:{cor}"/>')
            p.append(f'<text x="{L + w + 6:.1f}" y="{yy + 12}" {TXT} style="fill:var(--fg)">'
                     f'{fmt(m, 0)} {"mês" if round(m) == 1 else "meses"}</text>')
    p.append("</svg>")
    return "".join(p)


def secao_horizonte(a, vol):
    o = vol["onda"]
    graf = [("conversão +10 p.p.", meses_conv(.10, .05, o), meses_conv(.10, .10, o)),
            ("conversão +8 p.p.", meses_conv(.08, .05, o), meses_conv(.08, .10, o)),
            ("conversão +5 p.p.", meses_conv(.05, .05, o), meses_conv(.05, .10, o)),
            ("consumo em R$ +30%", meses_valor(.30, .05, o), meses_valor(.30, .10, o))]
    alvos = [("conversão +10 p.p.", lambda f, v: meses_conv(.10, f, v)),
             ("conversão +8 p.p.", lambda f, v: meses_conv(.08, f, v)),
             ("conversão +5 p.p.", lambda f, v: meses_conv(.05, f, v)),
             ("consumo em R$ +30%", lambda f, v: meses_valor(.30, f, v)),
             ("consumo em R$ +20%", lambda f, v: meses_valor(.20, f, v))]
    cols = [("onda", f"Onda {a.onda}")] + [(l, NOMES.get(l, l)) for l in a.linhas]
    H = ["<section><h2>Quando teremos o resultado de uplift</h2>",
         '<p class="note" style="margin:0 0 10px">Meses de operação para um resultado confiável (intervalo de 95%, '
         "poder de 80%), conforme o tamanho do ganho que se quer enxergar. <strong>Com 10% de controle, o resultado sai "
         "na metade do tempo de 5%.</strong></p>",
         legenda(("5% de controle", "var(--ctrl)"), ("10% de controle", "var(--nav)")),
         f'<div class="tbl" style="padding:12px 14px;margin:8px 0 12px"><div class="note" style="margin:0 0 6px">'
         f"Onda {a.onda} — meses de acúmulo</div>" + svg_horizonte(graf) + "</div>",
         '<div class="tbl"><table style="min-width:760px"><thead><tr><th>Ganho a detectar</th>']
    H += [f'<th{NUM}>{nome}<br><span class="note" style="margin:0">5% · 10%</span></th>' for _, nome in cols]
    H += ["</tr></thead><tbody>"]
    for rot, fn in alvos:
        H.append("<tr><td>" + rot + "</td>" + "".join(
            f"<td{NUM}>{fmt(fn(.05, vol[k]), 0)} · <strong>{fmt(fn(.10, vol[k]), 0)}</strong></td>" if vol.get(k) else
            f"<td{NUM}>—</td>" for k, _ in cols) + "</tr>")
    H += ["</tbody></table></div>",
          '<p class="note"><strong>Somar a esse prazo</strong> a janela de observação (90 dias depois do último sorteado) e '
          "o atraso da base de consumo (cerca de 2 semanas). Exemplo: conversão +8 p.p. com 10% na onda → "
          f"{fmt(meses_conv(.08, .10, o), 0)} meses de acúmulo + 90 dias + 2 semanas ≈ "
          f"{fmt(meses_conv(.08, .10, o) + 3.5, 1)} meses até o primeiro resultado confiável.</p>",
          '<p class="note"><strong>Por que conversão como indicador principal:</strong> o consumo em R$ tem cauda muito longa '
          "(mediana de cerca de R$ 130 e média de R$ 1.900 a R$ 3.500 em 30 dias), e esse ruído esconde ganhos de 20% por "
          "anos. A conversão (voltou ou não a consumir) varia pouco e responde em meses. <strong>Por linha isolada, só a "
          "tireoide tem volume</strong>; a leitura é pela onda, e as ondas 2 e 3 encurtam o prazo ao somar pacientes.</p>",
          '<p class="note"><strong>Base do cálculo:</strong> pacientes sorteados por dia útil desde o escopo atual de '
          f"unidades, de {a.horizonte_periodo[0][8:10]}/{a.horizonte_periodo[0][5:7]} a "
          f"{a.horizonte_periodo[1][8:10]}/{a.horizonte_periodo[1][5:7]} ({', '.join(f'{nome} {fmt(vol[k], 0)}/mês' for k, nome in cols if vol.get(k))}); variação do consumo "
          "medida em pacientes relevantes com exame até 30/08 e consumo nos 30 dias seguintes "
          f"(coeficiente de variação {fmt(CV_CONSUMO, 1)}, conversão base {fmt(100 * CONV_BASE, 0)}%). Com a janela de "
          "90 dias completa, a estimativa será refeita.</p></section>"]
    return H


def legenda(*itens):
    return ('<div style="display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:var(--muted)">' +
            "".join(f'<span><i style="display:inline-block;width:11px;height:11px;border-radius:3px;'
                    f'background:{c};margin-right:6px;vertical-align:-1px"></i>{t}</span>' for t, c in itens) +
            "</div>")


def escrever_html(a, uteis, p, por, tot_r, tot_c, z, crit, versao):
    ajustada = versao == "ajustada"
    ok = all(c[2] == "OK" for c in crit)
    tot_nav = sum(v["navegacao"] for v in por.values())
    tot_an = sum(v["analisados"] for v in por.values())
    tot_rel = sum(v["relevantes"] for v in por.values())
    lo, hi = wilson(tot_c, tot_r)
    lbl_rel = "Com achado relevante" if ajustada else "Relevantes"
    lbl_eleg = "Elegíveis para navegação (sorteados)" if ajustada else "Randomizados"
    titulo_v = "versão ajustada" if ajustada else "versão original"
    H = [f"<title>Randomização — onda {a.onda} ({'ajustada' if ajustada else 'original'})</title>",
         '<meta name="claude:viewport-fit-sides" content="cover">',
         '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Condensed:wght@600;700'
         '&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&display=swap">',
         f"<style>{CSS}</style>", '<div class="wrap">', "<header>",
         f'<span class="eyebrow">Fábrica de IA · Captação · {titulo_v}</span>',
         f"<h1>Randomização — relatório da onda {a.onda}</h1>",
         f'<span class="meta">{a.ambiente.upper()} · {a.inicio} a {a.fim} ({len(uteis)} dias úteis) · '
         f'{", ".join(NOMES.get(l, l) for l in a.linhas)} · fração de controle vigente {fmt(100 * p, 1)}%</span>',
         "</header>"]
    # KPIs
    H += ["<section><h2>Pacientes no período</h2>", '<div class="kpis">',
          f'<div class="kpi"><span class="k">Analisados pelo algoritmo</span><span class="v">{fmt(tot_an)}</span></div>',
          f'<div class="kpi"><span class="k">{lbl_rel}</span><span class="v">{fmt(tot_rel)}</span></div>',
          f'<div class="kpi"><span class="k">{lbl_eleg}</span><span class="v">{fmt(tot_r)}</span></div>',
          f'<div class="kpi nav"><span class="k">Navegação</span><span class="v">{fmt(tot_nav)}</span>'
          f'<span class="s">enviados às navegadoras</span></div>',
          f'<div class="kpi ctrl"><span class="k">Controle</span><span class="v">{fmt(tot_c)}</span>'
          f'<span class="s">registrados, não enviados</span></div>', "</div>"]
    if ajustada:
        H += ['<p class="note">Paciente conta uma vez por linha. Nem todo paciente com achado relevante é elegível: '
              "o sorteio vale para quem passa pela deduplicação e pelas validações de envio.</p>"]
    H += ["</section>"]
    # Veredito
    H += ["<section><h2>Critérios de aceite</h2>", '<div class="verdict">',
          f'<span class="badge {"ok" if ok else "bad"}">{"APTA" if ok else "COM PENDÊNCIAS"}</span></div>',
          '<div class="tbl" style="margin-top:12px"><table><thead><tr><th>Critério</th><th>Resultado</th><th>Status</th></tr></thead><tbody>']
    for c in crit:
        H.append(f'<tr><td>{c[0]}</td><td>{c[1]}</td><td><span class="pill {"ok" if c[2] == "OK" else "bad"}">'
                 f'{"OK" if c[2] == "OK" else "ALERTA"}</span></td></tr>')
    H += ["</tbody></table></div></section>"]
    # Proporção (ajustada): real x esperado
    if ajustada:
        H += ["<section><h2>Proporção de controle — real e esperada</h2>", '<div class="prop"><div class="row">',
              f'<div><div class="lbl">Real (onda)</div><div class="val">{fmt(100 * tot_c / tot_r if tot_r else 0, 1)}%</div>'
              f'<div class="note" style="margin:0">{fmt(tot_c)} de {fmt(tot_r)} pacientes</div></div>',
              f'<div><div class="lbl">Esperada</div><div class="val">{fmt(100 * p, 1)}%</div>'
              f'<div class="note" style="margin:0">fração configurada</div></div>',
              f'<div><div class="lbl">Intervalo de 95% do real</div><div class="val">{fmt(100 * lo, 1)}% – {fmt(100 * hi, 1)}%</div>'
              f'<div class="note" style="margin:0">contém a esperada: {"sim" if lo <= p <= hi else "não"}</div></div>',
              "</div>",
              '<div class="tbl" style="border:0;background:transparent">' + svg_proporcao(
                  tot_c / tot_r if tot_r else 0, lo, hi, p) + "</div>",
              legenda(("real (ponto)", "var(--ctrl)"), ("intervalo de 95% do real", "var(--ctrl-soft)"),
                      ("esperado (linha tracejada)", "var(--accent)")),
              '<p class="note" style="margin:0">A proporção é lida no total da onda. Por linha, o número de controles no período '
              "é pequeno e o percentual oscila por acaso — por isso a tabela abaixo mostra contagens reais por linha e a "
              "estimativa pela fração, sem percentual por linha.</p></div></section>"]
    # Funil da view de exportação (ajustada)
    funil = getattr(a, "funil", None)
    if ajustada and funil:
        dados_funil = []
        for l, v in por.items():
            f = funil.get(l, {})
            dados_funil.append((NOMES.get(l, l), [("com achado relevante", f.get("relevantes", 0)),
                                                  ("em unidade c/ navegação", f.get("unidade_navegada", 0)),
                                                  ("sem exame repetido", f.get("sem_repeticao", 0)),
                                                  ("sorteados", v["eleg_exames"])]))
        H += ["<section><h2>Do achado relevante ao sorteio — exames</h2>",
              '<div class="tbl" style="padding:14px 16px">' + svg_funil(dados_funil) + "</div>",
              '<div class="tbl" style="margin-top:12px"><table style="min-width:900px"><thead><tr><th>Linha</th>'
              '<th class="n">Com achado relevante</th><th class="n">Em unidade com navegação</th>'
              '<th class="n">Sem exame repetido</th><th class="n">Sorteados</th>'
              '<th class="n">Navegação</th><th class="n">Controle</th><th>Retido em cada etapa</th></tr></thead><tbody>']
        for l, v in por.items():
            f = funil.get(l, {})
            rel, un, sr = f.get("relevantes", 0), f.get("unidade_navegada", 0), f.get("sem_repeticao", 0)
            sort = v["eleg_exames"]
            larg = [100.0 * x / rel if rel else 0 for x in (un, sr, sort)]
            if f.get("tem_escopo"):
                desde = f.get("escopo_desde")
                vig = (f", vigente desde {desde[8:10]}/{desde[5:7]}" if desde and desde > a.inicio else "")
                escopo = f' <span class="note">({f["n_unidades"]} unidades{vig})</span>'
            else:
                escopo = ' <span class="note">(sem escopo de unidade)</span>'
            H.append(
                f"<tr><td>{NOMES.get(l, l)}</td><td{NUM}>{fmt(rel)}</td><td{NUM}>{fmt(un)}{escopo}</td>"
                f"<td{NUM}>{fmt(sr)}</td><td{NUM}>{fmt(sort)}</td><td{NUM}>{fmt(v['ex_navegacao'])}</td>"
                f"<td{NUM}>{fmt(v['ex_controle'])}</td><td>"
                + "".join(f'<div class="bar" style="margin:2px 0"><span style="width:{w:.1f}%;background:var(--accent)">'
                          f"</span></div>" for w in larg)
                + "</td></tr>")
        H += ["</tbody></table></div>",
              '<p class="note"><strong>Onde isso acontece:</strong> na view de exportação da linha, antes do sorteio. '
              "(1) só exames de unidades atendidas pelas navegadoras — escopo definido na configuração de navegação; "
              "(2) o mesmo evento clínico registrado com mais de um exame conta uma vez (paciente + data + tipo de exame); "
              "(3) só datas liberadas para envio. As barras mostram quanto dos relevantes resta após cada etapa. "
              "<em>Sorteados</em> vem do registro da operação; as duas colunas anteriores são reproduzidas a partir da "
              "saída do motor, contando por dia como a view.</p>"]
        tardios = sorted({f["escopo_desde"] for f in funil.values() if f.get("escopo_desde") and f["escopo_desde"] > a.inicio})
        if tardios:
            d = tardios[0]
            H += [f'<p class="note"><strong>Atenção:</strong> o escopo atual de unidades passou a valer em {a.ambiente.upper()} '
                  f"em {d[8:10]}/{d[5:7]}. Antes disso valia um escopo anterior, mais amplo — os pacientes sorteados "
                  "nesses dias incluem unidades fora do escopo atual. Nesses dias, as colunas reproduzidas contam todas as "
                  "unidades, por isso ficam acima dos sorteados. Para o uplift, a população deve ser restrita às "
                  "unidades do escopo.</p>"]
        H += ["</section>"]
    # Sorteados por dia (ajustada)
    if ajustada:
        dias = sorted({d for v in por.values() for d, _, _ in v["diario"]})
        H += ["<section><h2>Sorteados por dia — navegação e controle (exames)</h2>",
              legenda(("navegação", "var(--nav)"), ("controle", "var(--ctrl)"))]
        nav_t = [sum(nv for v in por.values() for dd, nv, _ in v["diario"] if dd == d) for d in dias]
        ct_t = [sum(c for v in por.values() for dd, _, c in v["diario"] if dd == d) for d in dias]
        H += [f'<div class="tbl" style="padding:12px 14px;margin-top:8px"><div class="note" style="margin:0 0 4px">'
              f"Onda {a.onda} — total</div>" + svg_diario(dias, nav_t, ct_t, "Sorteados por dia, total da onda") + "</div>"]
        for l, v in por.items():
            mapa = {d: (nv, c) for d, nv, c in v["diario"]}
            H += [f'<div class="tbl" style="padding:12px 14px;margin-top:8px"><div class="note" style="margin:0 0 4px">'
                  f"{NOMES.get(l, l)}</div>" + svg_diario(dias, [mapa.get(d, (0, 0))[0] for d in dias],
                                                         [mapa.get(d, (0, 0))[1] for d in dias],
                                                         f"Sorteados por dia, {NOMES.get(l, l)}") + "</div>"]
        H += ['<p class="note">Cada coluna é uma execução diária. O controle aparece em poucos dias e em pequeno número — '
              "é o esperado com 5%: cerca de 1 a cada 20 sorteados.</p></section>"]
    # Tabela por linha
    if ajustada:
        cab =["Linha", "Analisados", lbl_rel, "Elegíveis", "Navegação", "Controle (real)", "Distribuição",
               "Navegação/mês (estimada)", "Controle/mês (estimado)"]
    else:
        cab = ["Linha", "Analisados", "Relevantes", "Randomizados", "Navegação", "Controle", "% controle",
               "Navegação/dia útil", "Controle/mês (projeção)"]
    H += ["<section><h2>Por linha de cuidado</h2>", '<div class="tbl"><table style="min-width:900px"><thead><tr>']
    H += [f'<th{"" if i == 0 or c == "Distribuição" else NUM}>{c}</th>' for i, c in enumerate(cab)]
    H += ["</tr></thead><tbody>"]
    for l, v in por.items():
        ex = v["executados"] or 1
        if ajustada:
            eleg_dia = v["randomizados"] / ex
            share_nav = 100 * v["navegacao"] / v["randomizados"] if v["randomizados"] else 0
            cel = [NOMES.get(l, l), fmt(v["analisados"]), fmt(v["relevantes"]), fmt(v["randomizados"]),
                   fmt(v["navegacao"]), fmt(v["controle"]),
                   f'<div class="bar"><span style="width:{share_nav:.1f}%;background:var(--nav)"></span>'
                   f'<span style="width:{100 - share_nav:.1f}%;background:var(--ctrl)"></span></div>',
                   fmt((1 - p) * eleg_dia * DIAS_UTEIS_MES, 0), fmt(p * eleg_dia * DIAS_UTEIS_MES, 1)]
        else:
            pct = 100 * v["controle"] / v["randomizados"] if v["randomizados"] else 0
            cel = [NOMES.get(l, l), fmt(v["analisados"]), fmt(v["relevantes"]), fmt(v["randomizados"]),
                   fmt(v["navegacao"]), fmt(v["controle"]), f"{fmt(pct, 1)}%",
                   fmt(v["navegacao"] / ex, 1), fmt(v["controle"] / ex * DIAS_UTEIS_MES, 1)]
        H.append("<tr>" + "".join(f'<td{"" if i == 0 or (ajustada and i == 6) else NUM}>{c}</td>'
                                  for i, c in enumerate(cel)) + "</tr>")
    ex_t = max(v["executados"] for v in por.values()) or 1
    if ajustada:
        eleg_t = tot_r / ex_t
        tot = [f"Onda {a.onda}", fmt(tot_an), fmt(tot_rel), fmt(tot_r), fmt(tot_nav), fmt(tot_c), "",
               fmt((1 - p) * eleg_t * DIAS_UTEIS_MES, 0), fmt(p * eleg_t * DIAS_UTEIS_MES, 1)]
    else:
        tot = [f"Onda {a.onda}", fmt(tot_an), fmt(tot_rel), fmt(tot_r), fmt(tot_nav), fmt(tot_c),
               f"{fmt(100 * tot_c / tot_r if tot_r else 0, 1)}%", "", ""]
    H.append('<tr class="total">' + "".join(f'<td{"" if i == 0 else NUM}>{c}</td>' for i, c in enumerate(tot)) + "</tr>")
    H += ["</tbody></table></div>"]
    if ajustada:
        H += ['<p class="note"><strong>Estimativa mensal</strong> = elegíveis por dia útil no período × 21 dias úteis × '
              f"fração ({fmt(100 * p, 0)}% controle, {fmt(100 * (1 - p), 0)}% navegação). Não depende dos poucos controles "
              "já sorteados; o <strong>controle real</strong> mostra quantos de fato ficaram retidos.</p>"]
    else:
        H += ['<p class="note">Projeção mensal = controles observados por dia útil × 21. Com poucos dias, oscila muito.</p>']
    H += ["</section>"]
    # Regional
    H += ["<section><h2>Navegação por regional (pacientes enviados)</h2>"]
    if ajustada:
        H += ['<div class="grid2" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px">']
        for l, v in por.items():
            if v["regional"]:
                H += [f'<div class="tbl" style="padding:12px 14px"><div class="note" style="margin:0 0 6px">'
                      f"{NOMES.get(l, l)}</div>" +
                      svg_barras_h([(x["regional"], n(x["pacientes"])) for x in v["regional"]]) + "</div>"]
        H += ["</div>"]
    else:
        H += ["<ul>"]
        for l, v in por.items():
            if v["regional"]:
                H.append(f"<li><strong>{NOMES.get(l, l)}:</strong> " +
                         " · ".join(f"{x['regional']} {fmt(n(x['pacientes']))}" for x in v["regional"]) + "</li>")
        H += ["</ul>"]
    H += ["</section>"]
    # Contaminação
    H += ["<section><h2>Contaminação entre linhas (informativo)</h2>",
          '<div class="tbl"><table style="min-width:520px"><thead><tr><th>Linha</th><th>Grupo</th>'
          '<th class="n">Pacientes</th><th class="n">Também relevantes em outra linha</th></tr></thead><tbody>']
    for l, v in por.items():
        for g in ("controle", "intervencao"):
            x = v["contam"].get(g)
            if x:
                H.append(f'<tr><td>{NOMES.get(l, l)}</td><td>{"Controle" if g == "controle" else "Navegação"}</td>'
                         f'<td class="n">{fmt(n(x["pacientes"]))}</td><td class="n">{fmt(n(x["em_outra_linha"]))}</td></tr>')
    H += ["</tbody></table></div>",
          '<p class="note">Paciente de controle relevante numa linha não randomizada pode ser navegado por ela. '
          "Quanto mais linhas randomizadas, menor esse efeito: o grupo é do CPF e vale para todas.</p></section>"]
    if ajustada and getattr(a, "vol_horizonte", None):
        H += secao_horizonte(a, a.vol_horizonte)
    H += ["<section><h2>O que este relatório não mede</h2><ul>",
          "<li><strong>Impacto financeiro (uplift).</strong> Medido depois, com a linha em produção e após o de acordo, "
          "quando houver desfecho de consumo com janela completa.</li>",
          "<li><strong>Acerto clínico da régua.</strong> Este relatório avalia a randomização e a operação, não o laudo.</li>",
          "</ul></section>",
          f'<footer>Gerado em {dt.date.today().isoformat()} por gerar_relatorio_onda.py · números agregados, '
          "nenhum dado de paciente · fonte: mlops_fabrica_ia.operacao e saídas do motor.</footer>", "</div>"]
    suf = f"-{a.sufixo}" if getattr(a, "sufixo", "") else ""
    nome = f"relatorio-onda{a.onda}-{a.ambiente}-{a.inicio}_{a.fim}-{versao}{suf}.html"
    destino = Path(a.saida) / nome
    destino.write_text("\n".join(H), encoding="utf-8")
    return destino


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", action="store_true", help="gera também as páginas original e ajustada")
    ap.add_argument("--onda", required=True)
    ap.add_argument("--ambiente", required=True, choices=["hml", "prd"])
    ap.add_argument("--linhas", required=True, type=lambda s: [x.strip() for x in s.split(",") if x.strip()])
    ap.add_argument("--inicio", required=True)
    ap.add_argument("--fim", required=True)
    ap.add_argument("--perfil", default="nlp-platform")
    ap.add_argument("--repo", default=str(Path(__file__).resolve().parents[3] / "fabrica-ia-nlp-platform"),
                    help="clone do fabrica-ia-nlp-platform (escopo de unidades da navegação)")
    ap.add_argument("--saida", default=str(Path(__file__).parent.parent / "relatorios"))
    ap.add_argument("--horizonte", action="store_true", help="inclui a seção 'Quando teremos o resultado de uplift'")
    ap.add_argument("--sufixo", default="", help="nova versão (ex.: v2): grava só a ajustada, com sufixo, sem tocar nas anteriores")
    a = ap.parse_args()
    Path(a.saida).mkdir(parents=True, exist_ok=True)
    dados = coletar(a)
    uteis, p, por, tot_r, tot_c, z = avaliar(a, *dados)
    crit = criterios(a, dados[2], por, tot_r, z)
    if not a.sufixo:   # versão nova não regrava o markdown da versão anterior
        destino, ok = escrever(a, uteis, p, por, tot_r, tot_c, z, crit)
        print(f"{destino.name}: {'APTA' if ok else 'COM PENDENCIAS'} · {int(tot_r)} randomizados · {int(tot_c)} controle")
    if a.html:
        a.funil = funil_exames(a)
        if a.horizonte:
            datas = [f["escopo_desde"] for f in a.funil.values() if f.get("escopo_desde")]
            desde = max([a.inicio] + datas)
            a.vol_horizonte = volume_mensal(a, desde)
        for versao in (("ajustada",) if a.sufixo else ("original", "ajustada")):
            print(escrever_html(a, uteis, p, por, tot_r, tot_c, z, crit, versao).name)


if __name__ == "__main__":
    main()
