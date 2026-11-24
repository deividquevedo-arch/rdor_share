"""Extrai o historico mensal (so agregados) das 16 linhas de captacao por IA.

Uso:  python extrair_historico.py [--inicio 2026-01-01] [--fim 2026-10-31] [--so fonte1,fonte2]

Gera, na mesma pasta:
  mensal-por-linha-unidade.json   exames/relevantes distintos por linha x fonte x recorte x mes x unidade
  mensal-monitoramento-legado.json contagens do proprio job legado (monitoramento diario somado por mes)
  enviados-mensal.json            envios por linha x mes x unidade (o que existe)
  mensal-consolidado-linha.json   linha x mes com a fonte escolhida pela regra do inventario
  controles.json                  min/max de data e total por fonte (controle de "zero suspeito")

LGPD: toda consulta devolve so contagem agregada. Nenhuma coluna de paciente, laudo,
CPF ou nome e lida para fora do warehouse; id de exame so aparece dentro do SQL.

Requisitos: CLI `databricks` autenticado nos perfis `nlp-platform` (workspace novo) e
`adb-2013197995950192` (workspace antigo). No Git Bash o script ja seta MSYS_NO_PATHCONV=1.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

OLD = "adb-2013197995950192"
NEW = "nlp-platform"
OUT = Path(__file__).resolve().parent

LINHAS = {
    "mama": "Mama (BI-RADS)",
    "nefrologia": "Rim (nefrologia/clearance)",
    "hepatologia": "Fígado (hepatologia)",
    "pulmao_nodulo": "Pulmão – Nódulo",
    "dii": "DII",
    "reumatologia": "Reumatologia",
    "cancer_colon": "Ca de Cólon",
    "doencas_biliares": "Doenças biliares",
    "neuroimunologia": "Neuroimunologia",
    "pirads": "Próstata (PI-RADS)",
    "ateromatose": "Ateromatose coronariana",
    "tirads": "Tireoide (TI-RADS)",
    "cancer_estomago": "Ca de Estômago",
    "transplante_pulmao": "Transplante de pulmão",
    "cancer_rim": "Ca de Rim",
    "tumor_osseo": "Tumor ósseo",
}

RECORTE_COLONO = (
    "CASE WHEN lower(coalesce(exm_mod,'')||' '||coalesce(exm_tipo,'')) "
    "RLIKE 'colonosc|retossig|polipect' THEN 'colonoscopia' ELSE 'imagem' END"
)
HML_LEG = "diamond_fabrica_ia_hml.legado"


def _plataforma(linha: str, schema: str, recorte: str = "'plataforma'") -> dict:
    return dict(
        linha=linha, fonte="plataforma_prd", profile=NEW,
        tabela=f"diamond_fabrica_ia.{schema}.tb_mod_diamond_{schema}_saida_v0",
        id="id_exame", unidade="id_unidade", data="dt_execucao_modelo",
        rel="cast(fl_relevante as int) = 1", recorte=recorte, unidade_tipo="id",
    )


def _legado_padrao(linha: str, tabela: str, recorte: str, fonte: str, profile: str = NEW) -> dict:
    return dict(
        linha=linha, fonte=fonte, profile=profile, tabela=tabela,
        id="exm_an", unidade="idunidade", data="dataExecucaoModelo",
        rel="cast(fl_relevante as int) = 1", recorte=recorte, unidade_tipo="id",
    )


# Fontes por exame (contagem de id distinto, primeiro mes em que o exame aparece).
FONTES = [
    # --- plataforma nova (prd) ---
    _plataforma("ateromatose", "ateromatose_coronariana"),
    _plataforma("cancer_colon", "cancer_colon", RECORTE_COLONO),
    _plataforma("cancer_estomago", "cancer_estomago"),
    _plataforma("cancer_rim", "cancer_rim"),
    _plataforma("dii", "doenca_inflamatoria_intestinal", RECORTE_COLONO),
    _plataforma("doencas_biliares", "doencas_biliares"),
    _plataforma("hepatologia", "hepatologia"),
    _plataforma("neuroimunologia", "neuroimunologia"),
    _plataforma("pirads", "pirads"),
    _plataforma("reumatologia", "reumatologia"),
    _plataforma("tirads", "tirads"),
    _plataforma("transplante_pulmao", "transplante_pulmao"),
    _plataforma("tumor_osseo", "tumor_osseo"),
    # --- legado copiado para o catalogo novo (superconjunto do hive ate ~01/10) ---
    _legado_padrao("cancer_colon", f"{HML_LEG}.tb_diamond_mod_colon_saida", "'imagem_tc'", "legado_motor"),
    _legado_padrao("cancer_colon", f"{HML_LEG}.tb_diamond_mod_colon_colonoscopia_saida", "'colonoscopia'", "legado_motor"),
    _legado_padrao("dii", f"{HML_LEG}.tb_diamond_mod_dii_saida", "'imagem'", "legado_motor"),
    _legado_padrao("dii", f"{HML_LEG}.tb_diamond_mod_dii_colonoscopia_saida", "'colonoscopia'", "legado_motor"),
    _legado_padrao("reumatologia", f"{HML_LEG}.tb_diamond_mod_reumatologia_saida", "'legado'", "legado_motor"),
    _legado_padrao("neuroimunologia", f"{HML_LEG}.tb_diamond_mod_neuroimunologia_saida", "'legado'", "legado_motor"),
    _legado_padrao("ateromatose", f"{HML_LEG}.tb_diamond_mod_ateromatose_coronariana_saida", "'legado'", "legado_motor"),
    # biliares: tabelas sobrescritas a cada versao (snapshot de 1 mes cada) -> piloto
    _legado_padrao("doencas_biliares", f"{HML_LEG}.tb_diamond_mod_doencas_biliares_saida", "'v1_snapshot'", "legado_motor_snapshot"),
    _legado_padrao("doencas_biliares", f"{HML_LEG}.tb_diamond_mod_doencas_biliares_saida_v2", "'v2_snapshot_hml'", "legado_motor_snapshot"),
    _legado_padrao("doencas_biliares", "hive_metastore.ia.tb_diamond_mod_doencas_biliares_saida_v2", "'v2_snapshot_hive'", "legado_motor_snapshot", OLD),
    _legado_padrao("doencas_biliares", "hive_metastore.ia.tb_diamond_mod_doencas_biliares_saida_v3", "'v3_snapshot_hive'", "legado_motor_snapshot", OLD),
    # --- legado no workspace antigo ---
    _legado_padrao("pirads", "diamond_pirads.pirads.tb_diamond_mod_pirads_saida", "'legado'", "legado_motor", OLD),
    _legado_padrao("tirads", "diamond_tirads.tirads.tb_diamond_mod_tirads_saida", "'legado'", "legado_motor", OLD),
    dict(linha="tirads", fonte="legado_pacs_enave", profile=OLD,
         tabela="hive_metastore.ia.enave_df_historico_tirads", id="AN", unidade="idUnidade",
         data="data_carga", rel="try_cast(tirads as int) >= 4", recorte="'pacs_enave'", unidade_tipo="id"),
    dict(linha="pirads", fonte="legado_pacs_enave", profile=OLD,
         tabela="hive_metastore.ia.enave_df_historico_pirads", id="AN", unidade="idUnidade",
         data="data_carga", rel="try_cast(pirads as int) >= 4", recorte="'pacs_enave'", unidade_tipo="id"),
    dict(linha="mama", fonte="legado_motor", profile=OLD,
         tabela="diamond_birads.birads.tb_diamond_mod_birads_saida", id="id_exame", unidade="id_unidade",
         data="dt_execucao", rel="try_cast(vl_proced_birads as int) >= 4", recorte="'legado'", unidade_tipo="id"),
    dict(linha="hepatologia", fonte="legado_motor", profile=OLD,
         tabela="diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_saida", id="s.id_exame",
         unidade="u.un", data="s.dt_execucao", rel="cast(s.fl_relevante as int) = 1",
         recorte="'legado'", unidade_tipo="id",
         join=("s LEFT JOIN (SELECT cast(id_exame as string) id_exame, max(cast(id_unidade as string)) un "
               "FROM diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_entrada_v2 GROUP BY 1) u "
               "ON cast(s.id_exame as string) = u.id_exame")),
    dict(linha="pulmao_nodulo", fonte="legado_motor", profile=OLD,
         tabela="hive_metastore.ia.tb_diamond_mod_pulmao", id="e.an", unidade="e.idunidade",
         data="e.dataExecucaoModelo", rel="r.id IS NOT NULL", recorte="'legado'", unidade_tipo="id",
         join=("e LEFT JOIN (SELECT DISTINCT cast(exm_an as string) id FROM hive_metastore.ia.tb_diamond_mod_pulmao_saida "
               "WHERE exm_encaminhamento_nlp = 'S') r ON cast(e.an as string) = r.id")),
    dict(linha="nefrologia", fonte="legado_motor", profile=OLD,
         tabela="hive_metastore.ia.tb_diamond_nefrologia_clearance_saida", id="idExame", unidade="idUnidade",
         data="dt_execucao", rel=None, recorte="'legado'", unidade_tipo="id",
         extras={"clearance_lt60": "cast(valorClearance as double) < 60",
                 "clearance_lt30": "cast(valorClearance as double) < 30"}),
]

# Monitoramento diario do proprio job legado (contagem do job, NAO deduplicada por exame).
MAPA_PROJETO = {
    "Mama": "mama", "Rim": "nefrologia", "Figado": "hepatologia", "Pulmao": "pulmao_nodulo",
    "DII": "dii", "Reumatologia": "reumatologia", "Colon": "cancer_colon",
    "DoencasBiliares": "doencas_biliares", "Neuroimunologia": "neuroimunologia",
    "Prostata": "pirads", "Ateromatose": "ateromatose", "Tireoide": "tirads",
    "ateromatose_coronariana": "ateromatose", "doenca_inflamatoria_intestinal": "dii",
    "doencas_biliares": "doencas_biliares", "neuroimunologia": "neuroimunologia",
    "pirads": "pirads", "reumatologia": "reumatologia", "tirads": "tirads",
}
MONITORAMENTO = [
    dict(fonte="monitoramento_hive", profile=OLD, tabela="hive_metastore.ia.tb_diamond_mod_monitoramento",
         cols="sum(qtd_entrada) entrada, sum(qtd_saida) saida, sum(qtd_relevante) relevante, "
              "sum(qtd_envio) envio, sum(qtd_navegacao) navegacao"),
    dict(fonte="historico_legado_mlops", profile=NEW,
         tabela="mlops_fabrica_ia.operacao.tb_nlp_platform_historico_legado",
         cols="sum(qtd_entrada) entrada, sum(qtd_saida) saida, sum(qtd_relevante) relevante, "
              "cast(NULL as bigint) envio, cast(NULL as bigint) navegacao"),
]

ESPEC_PARA_LINHA = {
    "ateromatose_coronariana": "ateromatose", "cancer_colon": "cancer_colon",
    "cancer_estomago": "cancer_estomago", "cancer_rim": "cancer_rim",
    "doenca_inflamatoria_intestinal": "dii", "doencas_biliares": "doencas_biliares",
    "hepatologia": "hepatologia", "neuroimunologia": "neuroimunologia", "pirads": "pirads",
    "reumatologia": "reumatologia", "tirads": "tirads", "transplante_pulmao": "transplante_pulmao",
    "tumor_osseo": "tumor_osseo",
}

# Ordem de preferencia das fontes legadas por linha, para o consolidado.
PRIORIDADE_LEGADO = {
    "tirads": ["legado_motor", "legado_pacs_enave"],
    "pirads": ["legado_motor", "legado_pacs_enave"],
    "doencas_biliares": ["monitoramento"],
}
# Linhas em que a plataforma nao substitui o legado no periodo (plataforma roda em paralelo sem entregar).
LEGADO_SEMPRE = {"hepatologia"}


def q(sql: str, profile: str, timeout: int = 1800) -> list[dict]:
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    p = subprocess.run(
        ["databricks", "experimental", "aitools", "tools", "query", sql, "--profile", profile, "-o", "json"],
        capture_output=True, text=True, encoding="utf-8", env=env, timeout=timeout,
    )
    if p.returncode != 0:
        raise RuntimeError(f"exit={p.returncode}: {p.stderr[:600]}")
    out = p.stdout.strip()
    return json.loads(out) if out else []


def _num(v) -> int | None:
    return None if v in (None, "") else int(float(v))


def sql_por_exame(f: dict, ini: str, fim: str) -> str:
    """Um exame conta uma vez, no PRIMEIRO mes em que aparece na fonte (historico inteiro);
    unidade = a da primeira aparicao; relevante = se o exame foi relevante em qualquer execucao."""
    join = f.get("join") or ""
    rel = f["rel"] or "false"
    extras = f.get("extras", {})
    ext_base = "".join(f", CASE WHEN {e} THEN 1 ELSE 0 END x_{k}" for k, e in extras.items())
    ext_ex = "".join(f", max(x_{k}) x_{k}" for k in extras)
    ext_out = "".join(f", sum(x_{k}) {k}" for k in extras)
    return f"""
WITH base AS (
  SELECT cast({f['id']} as string) id, cast({f['unidade']} as string) un,
         to_date(cast({f['data']} as string)) d, {f['recorte']} rec,
         CASE WHEN {rel} THEN 1 ELSE 0 END r{ext_base}
  FROM {f['tabela']} {join}
  WHERE {f['id']} IS NOT NULL
),
ex AS (
  SELECT rec, id, min(d) d0, min_by(un, d) un, max(r) r{ext_ex}
  FROM base WHERE d IS NOT NULL GROUP BY rec, id
)
SELECT rec recorte, date_format(d0, 'yyyy-MM') mes, coalesce(un, '(nulo)') id_unidade,
       count(*) exames, sum(r) relevantes{ext_out}
FROM ex WHERE d0 BETWEEN '{ini}' AND '{fim}'
GROUP BY 1, 2, 3"""


def sql_controle(f: dict) -> str:
    join = f.get("join") or ""
    return (f"SELECT min(to_date(cast({f['data']} as string))) dt_min, "
            f"max(to_date(cast({f['data']} as string))) dt_max, count(*) linhas, "
            f"count(DISTINCT date_format(to_date(cast({f['data']} as string)),'yyyy-MM-dd')) dias "
            f"FROM {f['tabela']} {join}")


def extrair_por_exame(ini: str, fim: str, so: set[str] | None) -> tuple[list, list]:
    linhas, controles = [], []
    for f in FONTES:
        chave = f"{f['linha']}|{f['tabela']}"
        if so and f["tabela"] not in so and f["linha"] not in so:
            continue
        print(f"[exame] {chave}", flush=True)
        try:
            ctl = q(sql_controle(f), f["profile"])[0]
            controles.append({"linha": f["linha"], "fonte": f["fonte"], "tabela": f["tabela"],
                              "profile": f["profile"], **ctl})
            rows = q(sql_por_exame(f, ini, fim), f["profile"])
        except Exception as e:  # registra e segue; o resumo acusa a falha
            controles.append({"linha": f["linha"], "fonte": f["fonte"], "tabela": f["tabela"], "erro": str(e)[:400]})
            print(f"   ERRO {e}", flush=True)
            continue
        for r in rows:
            item = {"linha": f["linha"], "linha_rotulo": LINHAS[f["linha"]], "fonte": f["fonte"],
                    "tabela": f["tabela"], "recorte": r["recorte"], "mes": r["mes"],
                    "id_unidade": r["id_unidade"], "unidade_tipo": f["unidade_tipo"],
                    "exames": _num(r["exames"]),
                    "relevantes": _num(r["relevantes"]) if f["rel"] else None}
            for k in f.get("extras", {}):
                item[f"relevantes_proxy_{k}"] = _num(r[k])
            linhas.append(item)
        print(f"   {len(rows)} grupos; controle {ctl}", flush=True)
    return linhas, controles


def extrair_monitoramento(ini: str, fim: str) -> list:
    saida = []
    for m in MONITORAMENTO:
        print(f"[monit] {m['tabela']}", flush=True)
        sql = (f"SELECT nm_projeto, date_format(to_date(cast(dt_execucao as string)),'yyyy-MM') mes, "
               f"coalesce(nm_unidade,'(nulo)') unidade_nome, coalesce(nm_regional,'(nulo)') regional, "
               f"count(DISTINCT to_date(cast(dt_execucao as string))) dias, {m['cols']} "
               f"FROM {m['tabela']} WHERE to_date(cast(dt_execucao as string)) BETWEEN '{ini}' AND '{fim}' "
               f"GROUP BY 1,2,3,4")
        for r in q(sql, m["profile"]):
            saida.append({"linha": MAPA_PROJETO.get(r["nm_projeto"], r["nm_projeto"]),
                          "linha_rotulo": LINHAS.get(MAPA_PROJETO.get(r["nm_projeto"], ""), r["nm_projeto"]),
                          "fonte": m["fonte"], "nm_projeto": r["nm_projeto"], "mes": r["mes"],
                          "unidade_nome": r["unidade_nome"], "regional": r["regional"],
                          "unidade_tipo": "nome", "dias_com_registro": _num(r["dias"]),
                          **{k: _num(r[k]) for k in ("entrada", "saida", "relevante", "envio", "navegacao")}})
    return saida


def extrair_enviados(ini: str, fim: str, monit: list) -> list:
    env = []
    # Plataforma: OneDrive (desde 23/10) e API (desde 30/10), prd; unidade via tabela de saida.
    for canal, tab in (("onedrive", "tb_mov_onedrive_saida"), ("api", "tb_mov_api_saida")):
        specs = q(f"SELECT DISTINCT specialty FROM mlops_fabrica_ia.operacao.{tab} WHERE environment='prd'", NEW)
        for s in (x["specialty"] for x in specs):
            print(f"[envio] {canal} {s}", flush=True)
            sql = f"""
WITH e AS (SELECT exame_an, min(dt_execucao_modelo) d FROM mlops_fabrica_ia.operacao.{tab}
           WHERE environment='prd' AND specialty='{s}' GROUP BY exame_an),
u AS (SELECT cast(id_exame as string) id, max(cast(id_unidade as string)) un
      FROM diamond_fabrica_ia.{s}.tb_mod_diamond_{s}_saida_v0 GROUP BY 1)
SELECT date_format(e.d,'yyyy-MM') mes, coalesce(u.un,'(sem unidade)') id_unidade, count(*) enviados
FROM e LEFT JOIN u ON e.exame_an = u.id
WHERE e.d BETWEEN '{ini}' AND '{fim}' GROUP BY 1,2"""
            for r in q(sql, NEW):
                env.append({"linha": ESPEC_PARA_LINHA.get(s, s), "linha_rotulo": LINHAS.get(ESPEC_PARA_LINHA.get(s, s), s),
                            "fonte": f"plataforma_{canal}_prd", "mes": r["mes"], "id_unidade": r["id_unidade"],
                            "unidade_tipo": "id", "enviados": _num(r["enviados"]),
                            "unidade_contagem": "exame_an distinto"})
    # Hepatologia legado: linhas de navegacao enviadas (id_exame vem nulo nessa tabela).
    print("[envio] hepatologia navegacao_v2", flush=True)
    sql = (f"SELECT date_format(to_date(cast(dt_enviado_navegacao as string)),'yyyy-MM') mes, "
           f"coalesce(cast(id_unidade as string),'(nulo)') id_unidade, count(DISTINCT id_navegacao) enviados "
           f"FROM diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_navegacao_v2 "
           f"WHERE to_date(cast(dt_enviado_navegacao as string)) BETWEEN '{ini}' AND '{fim}' GROUP BY 1,2")
    for r in q(sql, OLD):
        env.append({"linha": "hepatologia", "linha_rotulo": LINHAS["hepatologia"],
                    "fonte": "legado_hepatologia_navegacao_v2", "mes": r["mes"], "id_unidade": r["id_unidade"],
                    "unidade_tipo": "id", "enviados": _num(r["enviados"]),
                    "unidade_contagem": "linha de navegacao (id_navegacao distinto)"})
    # Legado em geral: qtd_envio do monitoramento do job (por nome de unidade).
    for m in monit:
        if m["fonte"] == "monitoramento_hive" and m["envio"]:
            env.append({"linha": m["linha"], "linha_rotulo": m["linha_rotulo"], "fonte": "legado_monitoramento_qtd_envio",
                        "mes": m["mes"], "unidade_nome": m["unidade_nome"], "unidade_tipo": "nome",
                        "enviados": m["envio"], "unidade_contagem": "contagem diaria do job, somada no mes"})
    return env


def _meses(ini: str, fim: str) -> list[str]:
    y, mo = int(ini[:4]), int(ini[5:7])
    out = []
    while f"{y:04d}-{mo:02d}" <= fim[:7]:
        out.append(f"{y:04d}-{mo:02d}")
        mo += 1
        if mo == 13:
            y, mo = y + 1, 1
    return out


def consolidar(por_exame: list, monit: list, controles: list, ini: str, fim: str) -> list:
    """Regra: plataforma a partir do 1o mes COMPLETO nela; antes, legado (por prioridade)."""
    tot: dict = {}
    for r in por_exame:
        k = (r["linha"], r["fonte"], r["mes"])
        t = tot.setdefault(k, {"exames": 0, "relevantes": 0, "rel_nulo": r["relevantes"] is None})
        t["exames"] += r["exames"] or 0
        t["relevantes"] += r["relevantes"] or 0
    for m in monit:
        if m["fonte"] != "historico_legado_mlops":
            continue
        k = (m["linha"], "monitoramento", m["mes"])
        t = tot.setdefault(k, {"exames": 0, "relevantes": 0, "rel_nulo": False})
        t["exames"] += m["saida"] or 0
        t["relevantes"] += m["relevante"] or 0
    inicio_plat = {c["linha"]: c["dt_min"] for c in controles if c.get("fonte") == "plataforma_prd" and c.get("dt_min")}
    out = []
    for linha in LINHAS:
        dt0 = inicio_plat.get(linha)
        if dt0:
            d = date.fromisoformat(dt0)
            pm = dt0[:7] if d.day <= 2 else _meses(f"{d.year:04d}-{d.month:02d}-01", "9999-12")[1]
        else:
            pm = "9999-99"
        prio = PRIORIDADE_LEGADO.get(linha, ["legado_motor"])
        for mes in _meses(ini, fim):
            escolha = None
            if linha not in LEGADO_SEMPRE and mes >= pm and (linha, "plataforma_prd", mes) in tot:
                escolha = "plataforma_prd"
            else:
                for f in prio:
                    if (linha, f, mes) in tot:
                        escolha = f
                        break
                if escolha is None and (linha, "plataforma_prd", mes) in tot:
                    escolha = "plataforma_prd (mes parcial)"
            t = tot.get((linha, escolha.split(" ")[0], mes)) if escolha else None
            outras = sorted({f for (l, f, m2) in tot if l == linha and m2 == mes and f != (escolha or "").split(" ")[0]})
            out.append({"linha": linha, "linha_rotulo": LINHAS[linha], "mes": mes,
                        "fonte_escolhida": escolha, "exames": t["exames"] if t else None,
                        "relevantes": (None if (t and t["rel_nulo"]) else (t["relevantes"] if t else None)),
                        "metrica": ("contagem do job (nao deduplicada)" if escolha == "monitoramento"
                                    else "exame distinto (1a aparicao)" if escolha else None),
                        "outras_fontes_no_mes": outras, "primeiro_mes_completo_plataforma": pm if dt0 else None})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inicio", default="2026-01-01")
    ap.add_argument("--fim", default="2026-10-31")
    ap.add_argument("--so", default="", help="restringe a linhas/tabelas (separadas por virgula)")
    a = ap.parse_args()
    so = set(x for x in a.so.split(",") if x) or None
    por_exame, controles = extrair_por_exame(a.inicio, a.fim, so)
    monit = extrair_monitoramento(a.inicio, a.fim)
    env = extrair_enviados(a.inicio, a.fim, monit)
    cons = consolidar(por_exame, monit, controles, a.inicio, a.fim)
    meta = {"gerado_em": date.today().isoformat(), "periodo": [a.inicio, a.fim]}
    for nome, dados in (("mensal-por-linha-unidade.json", por_exame), ("mensal-monitoramento-legado.json", monit),
                        ("enviados-mensal.json", env), ("mensal-consolidado-linha.json", cons),
                        ("controles.json", controles)):
        (OUT / nome).write_text(json.dumps({"meta": meta, "linhas": dados}, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"gravado {nome}: {len(dados)} registros", flush=True)
    falhas = [c for c in controles if "erro" in c]
    print(f"FALHAS: {len(falhas)}", flush=True)
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
