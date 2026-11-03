"""Relatórios das Ondas 2 e 3 no formato APROVADO — v2: sorteio e garantias lidos de HOMOLOGAÇÃO (prévia)
até a ativação em produção; volume e prazo seguem com o volume real de produção.

v1: formato APROVADO pelo time (painel da Onda 1, v2, versão de 26/10/2026).

Uso: python gerar_relatorio_onda_n.py  ->  painel-negocio-onda{2,3}-v2.lvdash.json
Parte do JSON aprovado (`painel-negocio-v2-aprovado-2026-10-26.lvdash.json`, baixado do workspace) e troca:
  - linhas e rótulos da onda nos datasets do sorteio (pacientes, distribuição, garantias) — leem PRODUÇÃO e se
    preenchem sozinhos quando a randomização da onda for ativada;
  - volume: exames ELEGÍVEIS por mês em produção (relatório de execução), que já existe antes da ativação;
  - prazo: meses de acúmulo CALCULADOS na consulta com o volume da própria onda (mesma referência da Onda 1:
    retorno de 58% para 66%, 95% de confiança, 80% de poder) — nenhum prazo fixo em texto;
  - textos: onda, linhas, datas e um aviso enquanto a onda não estiver ativa.
"""
import json
from pathlib import Path

AQUI = Path(__file__).parent
BASE = json.loads((AQUI / "painel-negocio-v2-aprovado-2026-10-26.lvdash.json").read_text(encoding="utf-8"))

ROTULO = {"doenca_inflamatoria_intestinal": "DII", "cancer_colon": "Câncer de cólon", "doencas_biliares": "Doenças biliares",
          "neuroimunologia": "Neuroimunologia", "pirads": "Próstata (PI-RADS)", "ateromatose_coronariana": "Ateromatose",
          "transplante_pulmao": "Transplante de pulmão", "cancer_estomago": "Câncer de estômago",
          "cancer_rim": "Câncer de rim", "tumor_osseo": "Tumor ósseo"}
ONDAS = {
    2: {"linhas": ["doenca_inflamatoria_intestinal", "cancer_colon", "doencas_biliares", "neuroimunologia", "pirads"],
        "prd": "17/11/2026",
        "nomes": "DII, Câncer de cólon, Doenças biliares, Neuroimunologia e Próstata (PI-RADS)",
        "sem_volume": "Doenças biliares, Neuroimunologia e Próstata ainda não registram volume no relatório de execução "
                      "de produção (a etapa entra com a ativação) e ficam fora do volume até lá."},
    3: {"linhas": ["ateromatose_coronariana", "transplante_pulmao", "cancer_estomago", "cancer_rim", "tumor_osseo"],
        "prd": "24/11/2026",
        "nomes": "Ateromatose, Transplante de pulmão, Câncer de estômago, Câncer de rim e Tumor ósseo",
        "sem_volume": "Câncer de estômago tem volume muito baixo (menos de 1 exame elegível por dia útil) e aparece "
                      "só como descritivo."},
}
AMBIENTE_SORTEIO = "hml"   # v2: prévia em homologação; trocar para "prd" na ativação
VERSAO = "v3"  # v3: gráfico dia a dia com eixo categórico (1 barra por dia) e legenda com título
Z2 = (1.96 + 0.84) ** 2
P, D = 0.58, 0.08   # referência da Onda 1: retorno de 58% para 66%


def sql_case(linhas):
    whens = " ".join(f"WHEN '{l}' THEN '{ROTULO[l]}'" for l in linhas)
    return f"CASE nm_especialidade {whens} ELSE initcap(nm_especialidade) END"


def datasets(n, cfg):
    ls = ", ".join(f"'{l}'" for l in cfg["linhas"])
    case = sql_case(cfg["linhas"])
    filtro = f"tp_ambiente = '{AMBIENTE_SORTEIO}' AND nm_especialidade IN ({ls})"
    vol = f"""
WITH r AS (
  SELECT nm_especialidade, to_date(dt_execucao_modelo) AS d, max_by(qt_exportados, dh_registro) AS elegiveis
  FROM tb_mov_randomizacao_relatorio
  WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({ls}) AND to_date(dt_execucao_modelo) >= DATE'2026-10-14'
  GROUP BY 1, 2
),
por_linha AS (SELECT nm_especialidade, avg(elegiveis) * 21 AS mes FROM r GROUP BY 1),
mes AS (SELECT coalesce(sum(mes), 0) AS pacientes_mes FROM por_linha)"""
    return {
        "pacientes": f"""
SELECT {case} AS linha,
       sha2(cast(num_cpf AS string), 256) AS id_paciente,
       CASE WHEN max(tp_grupo) = 'controle' THEN 'Grupo controle' ELSE 'Enviados à navegação' END AS grupo,
       CASE WHEN max(tp_grupo) = 'controle' THEN 1 ELSE 0 END AS fl_controle,
       CASE WHEN max(tp_grupo) = 'controle' THEN 0 ELSE 1 END AS fl_navegacao,
       min(to_date(dt_execucao_modelo)) AS dia,
       date_format(min(to_date(dt_execucao_modelo)), 'yyyy-MM-dd') AS dia_txt
FROM tb_mov_randomizacao_log WHERE {filtro}
GROUP BY 1, 2
""",
        "garantias": f"""
WITH l AS (SELECT * FROM tb_mov_randomizacao_log WHERE {filtro})
SELECT
  (SELECT count(*) FROM l WHERE tp_grupo = 'controle' AND fl_enviado) AS controle_enviado,
  (SELECT count(*) FROM (SELECT num_cpf FROM l GROUP BY num_cpf HAVING count(DISTINCT tp_grupo) > 1)) AS trocou_de_grupo,
  (SELECT count(*) FROM l LEFT JOIN tb_mov_randomizacao_controle b
     ON lpad(cast(l.num_cpf AS string), 11, '0') = lpad(cast(b.num_cpf AS string), 11, '0')
   WHERE b.tp_grupo IS NULL OR b.tp_grupo <> l.tp_grupo) AS diferente_do_sorteio
""",
        "distribuicao": f"""
WITH p AS (
  SELECT {case} AS linha, num_cpf, max(CASE WHEN tp_grupo = 'controle' THEN 1 ELSE 0 END) AS c
  FROM tb_mov_randomizacao_log WHERE {filtro} GROUP BY 1, 2
)
SELECT linha, count(*) AS pacientes, sum(c) AS no_controle, sum(c) / count(*) AS pct_controle, 0.05 AS meta
FROM p GROUP BY linha
UNION ALL
SELECT 'Onda {n} (total)', count(*), sum(c), sum(c) / count(*), 0.05 FROM p
""",
        "volume": vol + """,
c AS (SELECT * FROM VALUES ('Hoje: 5%', 0.05, 1), ('Com 10%', 0.10, 2) AS t(cenario, fracao, ordem))
SELECT c.cenario, c.ordem, 'Enviados à navegação' AS grupo, round(m.pacientes_mes * (1 - c.fracao)) AS pacientes_mes
FROM c CROSS JOIN mes m
UNION ALL
SELECT c.cenario, c.ordem, 'Grupo controle', round(m.pacientes_mes * c.fracao) FROM c CROSS JOIN mes m
ORDER BY ordem, grupo
""",
        # Meses de acúmulo = n de controle necessário / controle acumulado por mês (CPF casado ~95%).
        "prazo": vol + f""",
c AS (SELECT * FROM VALUES ('Com 10%', 0.10, 1), ('Se ficasse em 5%', 0.05, 2) AS t(cenario, fracao, ordem)),
acum AS (
  SELECT c.cenario, c.ordem,
         round({Z2} * {P} * (1 - {P}) * (1 / c.fracao + 1 / (1 - c.fracao)) / ({D} * {D}) / nullif(m.pacientes_mes * 0.95, 0), 1) AS meses
  FROM c CROSS JOIN mes m
)
SELECT cenario, ordem, '1. Juntar pacientes no controle' AS etapa, meses FROM acum
UNION ALL SELECT cenario, ordem, '2. Acompanhar cada paciente 90 dias', 3.0 FROM acum
UNION ALL SELECT cenario, ordem, '3. Base de consumo atualizar', 0.5 FROM acum
ORDER BY ordem, etapa
""",
    }


def textos(n, cfg):
    return {
        "titulo": [f"# Navegação da Captação — grupo controle da Onda {n}\n", "\n",
                   f"# {cfg['nomes']} · PRD prevista para {cfg['prd']} · atualização diária"],
        "s1": [f"## 1. Como está — prévia em homologação, controle com a fração vigente"],
        "s2": [f"## 2. A fração de controle (**a Onda {n} entra com a fração vigente.**) A validação do sorteio já foi "
               "feita na Onda 1, em produção: o grupo é sorteado ao acaso, o paciente nunca troca de grupo e **nenhum "
               "paciente do controle é enviado por engano**. A Onda " + str(n) + " usa o mesmo sorteio e a mesma base, então "
               "o mesmo paciente fica no mesmo grupo em qualquer linha. Escalar para **10% tem a finalidade de obter o "
               "resultado na metade do tempo**; subir não muda ninguém de lugar — só entram mais pacientes novos no "
               "controle. (Subir é possível a qualquer momento; descer, não.)"],
        "leitura3": [f"O volume mensal da Onda {n} está no gráfico ao lado: **exames elegíveis para a navegação em "
                     "produção desde 14/10**, projetados para um mês, que se atualiza todo dia.\n", "\n",
                     "**Com 5%:** 1 em cada 20 fica no controle.\n", "\n",
                     "**Com 10%:** 1 em cada 10 fica no controle.\n", "\n",
                     f"⚠️ {cfg['sem_volume']}"],
        "leitura4": ["**Os meses do gráfico são calculados com o volume da própria onda**, com a mesma referência da "
                     "Onda 1 (enxergar a taxa de retorno subir de 58% para 66%).\n", "\n",
                     "Onda com menos volume leva mais tempo: **a leitura de uplift é feita pela soma das ondas**, e cada "
                     "onda nova encurta o prazo do conjunto.\n", "\n",
                     "**Por que demora:** além de juntar pacientes no controle, cada paciente precisa idealmente de "
                     "**90 dias** de acompanhamento para sabermos se voltou à Rede, e também consideramos que a base de "
                     "consumo chega com \\~2 semanas de atraso.\n", "\n",
                     "**O que será medido:** a **taxa de retorno** — % de pacientes que voltam a consumir na Rede em até "
                     "90 dias, medida **do mesmo jeito nos dois grupos**. Não é a \"taxa de captação\" da Central, que só "
                     "existe para quem foi navegado. O valor em R$ por paciente vem como indicador secundário."],
        "s6": [f"## 6. O que precisamos do negócio:\n", "\n",
               f"## 1. **De acordo** com a operação da Onda {n}.\n", "\n",
               "## 2. **Confirmar o indicador principal:** taxa de retorno à Rede em até 90 dias."],
    }


def gerar(n, cfg):
    s = json.loads(json.dumps(BASE))
    sql = datasets(n, cfg)
    for ds in s["datasets"]:
        if ds["name"] in sql:
            ds["queryLines"] = [l + "\n" for l in sql[ds["name"]].strip().splitlines()]
    tx = textos(n, cfg)
    for p in s["pages"]:
        for it in p["layout"]:
            if it["position"]["y"] >= 2:
                it["position"]["y"] += 2
            w = it["widget"]
            if "multilineTextboxSpec" in w and w["name"] in tx:
                w["multilineTextboxSpec"]["lines"] = tx[w["name"]]
        p["layout"].insert(1, {"widget": {"name": "aviso_ativacao", "multilineTextboxSpec": {"lines": [
            f"**🧪 PRÉVIA EM HOMOLOGAÇÃO.** A randomização da Onda {n} entra em produção em {cfg['prd']} (data em ajuste). "
            "Até lá, as seções **1** (sorteio) e **5** (garantias) mostram os sorteios de **homologação** — poucos dias e "
            "poucos pacientes, para ver o formato funcionando, não para ler resultado. Volume e prazo já usam os exames "
            "elegíveis medidos em **produção**."]}}, "position": {"x": 0, "y": 2, "width": 12, "height": 2}})
    for p in s["pages"]:
        for it in p["layout"]:
            w = it["widget"]
            if w["name"] == "g_dia":
                w["queries"][0]["query"]["fields"][0] = {"name": "dia_txt", "expression": "`dia_txt`"}
                enc = w["spec"]["encodings"]
                enc["x"] = {"fieldName": "dia_txt", "scale": {"type": "categorical"},
                            "axis": {"title": ""}}
                enc["color"]["displayName"] = "Grupo"
                enc["label"] = {"show": True}
    txt = json.dumps(s, ensure_ascii=False)
    txt = txt.replace("Onda 1 (total)", f"Onda {n} (total)")
    txt = txt.replace("Volume real da Onda 1 desde 14/10 (103 unidades com navegação), projetado para um mês",
                      f"Exames elegíveis da Onda {n} em produção desde 14/10, projetados para um mês")
    txt = txt.replace("Pacientes sorteados por dia — o controle é a fatia laranja",
                      "Pacientes sorteados por dia em homologação (prévia) — o controle é a fatia laranja")
    txt = txt.replace("Pacientes por mês nos dois cenários", "Exames elegíveis por mês nos dois cenários")
    txt = txt.replace("Para enxergar um aumento de 8 pontos na taxa de retorno (de 58% para 66%), Onda 1 somada",
                      f"Para enxergar um aumento de 8 pontos na taxa de retorno (de 58% para 66%), Onda {n} sozinha")
    out = AQUI / f"painel-negocio-onda{n}-{VERSAO}.lvdash.json"
    out.write_text(json.dumps(json.loads(txt), ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name)
    return out


if __name__ == "__main__":
    for n, cfg in ONDAS.items():
        gerar(n, cfg)
