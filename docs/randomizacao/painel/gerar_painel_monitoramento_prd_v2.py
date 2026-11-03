"""Painel de MONITORAMENTO da randomização em PRD — v2: três camadas de garantia de que o controle não é navegado.

Uso: python gerar_painel_monitoramento_prd_v2.py  ->  painel-monitoramento-randomizacao-prd-v2.lvdash.json
Reaproveita a v1 (gerar_painel_monitoramento_prd.py: datas de segregação, consultas diária/resumo/garantias/pacientes).

Camadas, da mais próxima à mais distante do envio:
  1. CPF do controle dentro da tabela de intervenção (alimenta OneDrive e API) — paciente a paciente;
  2. exame enviado no OneDrive fora da intervenção do mesmo dia;
  3. paciente do controle com caso aberto na Central de Navegação (desfecho) — por id_inferencia.
Depois da segregação, as três têm de ser zero.
"""
import json
from pathlib import Path

import gerar_painel_monitoramento_prd as v1

AQUI = Path(__file__).parent
CENTRAL = "bronze_fabrica_ia_dev.navegacao.central_caso_navegacao"  # réplica com dado de produção (confirmar SLA)

CAMADAS = v1.BASE + f""",
lg AS (SELECT DISTINCT nm_especialidade AS esp, id_inferencia, lpad(CAST(num_cpf AS STRING), 11, '0') AS cpf, tp_grupo,
         to_date(dt_execucao_modelo) AS dia
       FROM {v1.OP}.tb_mov_randomizacao_log WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({v1.LINHAS})),
itv_cpf AS (SELECT DISTINCT nm_especialidade AS esp, lpad(regexp_replace(CAST(cpf_paciente AS STRING), '\\D', ''), 11, '0') AS cpf
            FROM {v1.OP}.tb_mov_randomizacao_intervencao WHERE tp_ambiente = 'prd' AND nm_especialidade IN ({v1.LINHAS})),
cs AS (SELECT DISTINCT id_inferencia FROM {CENTRAL}),
marc AS (SELECT lg.*, s.dt_segregacao, (s.dt_segregacao IS NOT NULL AND lg.dia >= s.dt_segregacao) AS pos,
           (i.cpf IS NOT NULL) AS na_intervencao, (c.id_inferencia IS NOT NULL) AS com_caso
         FROM lg JOIN seg s ON s.esp = lg.esp
         LEFT JOIN itv_cpf i ON i.esp = lg.esp AND i.cpf = lg.cpf
         LEFT JOIN cs c ON c.id_inferencia = lg.id_inferencia),
c13 AS (SELECT esp,
          count(DISTINCT CASE WHEN tp_grupo = 'controle' AND pos THEN cpf END) AS controles_pos,
          count(DISTINCT CASE WHEN tp_grupo = 'controle' AND pos AND na_intervencao THEN cpf END) AS c1_controle_na_intervencao,
          count(DISTINCT CASE WHEN tp_grupo = 'controle' AND pos AND com_caso THEN id_inferencia END) AS c3_controle_com_caso,
          count(DISTINCT CASE WHEN tp_grupo = 'controle' AND NOT pos AND com_caso THEN id_inferencia END) AS controle_com_caso_antes,
          count(DISTINCT CASE WHEN tp_grupo = 'intervencao' AND na_intervencao THEN cpf END) AS ok_intervencao_na_tabela,
          count(DISTINCT CASE WHEN tp_grupo = 'intervencao' AND com_caso THEN id_inferencia END) AS ok_intervencao_com_caso
        FROM marc GROUP BY 1),
c2 AS (SELECT esp, sum(CASE WHEN fase = 'segregado' THEN enviados_fora_intervencao ELSE 0 END) AS c2_envio_fora_intervencao FROM d GROUP BY 1),
atual AS (SELECT max(to_date(data_execucao_modelo)) AS central_ate FROM {CENTRAL})
SELECT {v1.CASE.replace('CASE esp', 'CASE s.esp')} AS linha,
  CASE WHEN s.dt_segregacao IS NULL THEN '⏳ aguardando' ELSE concat('✅ desde ', date_format(s.dt_segregacao, 'dd/MM')) END AS segregacao,
  coalesce(a.controles_pos, 0) AS controles_pos,
  coalesce(a.c1_controle_na_intervencao, 0) AS c1_controle_na_intervencao,
  coalesce(b.c2_envio_fora_intervencao, 0) AS c2_envio_fora_intervencao,
  coalesce(a.c3_controle_com_caso, 0) AS c3_controle_com_caso,
  coalesce(a.controle_com_caso_antes, 0) AS controle_com_caso_antes,
  coalesce(a.ok_intervencao_na_tabela, 0) AS ok_intervencao_na_tabela,
  coalesce(a.ok_intervencao_com_caso, 0) AS ok_intervencao_com_caso,
  date_format(t.central_ate, 'dd/MM') AS central_atualizada_ate,
  datediff(current_date(), t.central_ate) AS dias_desde_central,
  CASE WHEN s.dt_segregacao IS NULL THEN '⏳ ligar a segregação'
       WHEN coalesce(a.c1_controle_na_intervencao, 0) + coalesce(b.c2_envio_fora_intervencao, 0) + coalesce(a.c3_controle_com_caso, 0) > 0 THEN '🔴 controle pode ter sido navegado'
       WHEN t.central_ate < s.dt_segregacao THEN '🟢 ok · 🟡 Central ainda sem dado pós-segregação'
       ELSE '🟢 ok' END AS situacao
FROM seg s LEFT JOIN c13 a ON a.esp = s.esp LEFT JOIN c2 b ON b.esp = s.esp CROSS JOIN atual t
ORDER BY s.dt_segregacao NULLS LAST, linha"""


def main():
    sql = dict(v1.SQL, camadas=CAMADAS)
    descr = dict(v1.DESCR, camadas="Três camadas de garantia por linha")
    datasets = [{"name": k, "displayName": descr[k], "queryLines": [l + "\n" for l in v.strip().splitlines()]} for k, v in sql.items()]
    L = [
        v1.txt("titulo", ["# Randomização em PRD — monitoramento da segregação (v2)\n", "\n",
                          "Três camadas de garantia de que o **grupo controle não é navegado** · Tireoide e Reumatologia segregam desde **30/10/2026** · "
                          "DII e Ateromatose entram na semana de 03/11"], 0, 0, 12, 2),
        v1.counter("k_c1", "camadas", "SUM(`c1_controle_na_intervencao`)", "1 · Controle na intervenção",
                   "CPFs do controle dentro da tabela que alimenta OneDrive e API, desde a segregação. Tem de ser 0", 0, 2),
        v1.counter("k_c2", "camadas", "SUM(`c2_envio_fora_intervencao`)", "2 · Envio fora da intervenção",
                   "Exames no arquivo do OneDrive que não são da intervenção, desde a segregação. Tem de ser 0", 3, 2),
        v1.counter("k_c3", "camadas", "SUM(`c3_controle_com_caso`)", "3 · Controle com caso na Central",
                   "Pacientes do controle que viraram caso de navegação, desde a segregação. Tem de ser 0", 6, 2),
        v1.counter("k_central", "camadas", "MAX(`dias_desde_central`)", "Atraso da Central (dias)",
                   "Dias desde a última atualização da tabela de casos da Central — a camada 3 só vale até essa data", 9, 2),
        v1.table("t_camadas", "camadas", ["linha", "segregacao", "situacao", "controles_pos", "c1_controle_na_intervencao", "c2_envio_fora_intervencao",
                                          "c3_controle_com_caso", "controle_com_caso_antes", "ok_intervencao_na_tabela", "ok_intervencao_com_caso",
                                          "central_atualizada_ate"], "Três camadas por linha",
                 "c1–c3 desde a segregação (têm de ser 0) · controle_com_caso_antes = navegados antes da segregação, a excluir da análise · "
                 "ok_* = controle positivo: a intervenção aparece na tabela e na Central", 0, 5, 12, 4),
        v1.table("t_resumo", "resumo", ["linha", "segregacao", "situacao", "sorteados", "controle", "pct_controle", "intervencao_nao_enviada",
                                        "ultima_execucao_ok"], "Sorteio e envio desde a segregação",
                 "pct_controle esperado perto de 5% · intervenção não enviada = falha de envio", 0, 9, 7, 4),
        v1.counter("k_troca", "garantias", "SUM(`trocou_de_grupo` + `diferente_do_sorteio`)", "Inconsistências do sorteio",
                   "Paciente que trocou de grupo ou diverge da base de controle. Tem de ser 0", 7, 9, 2, 4),
        v1.counter("k_reg", "garantias", "SUM(`registros_verificados`)", "Registros verificados",
                   "Sorteios desde a segregação usados nas garantias (controle de que a checagem não está vazia)", 9, 9, 3, 4),
        v1.barras("g_dia", 0, 13, 12, 5),
        v1.table("t_diario", "diario", ["linha", "dia", "fase", "execucao", "alerta", "exportados", "controle", "intervencao", "enviados",
                                        "enviados_fora_intervencao", "intervencao_nao_enviada", "sem_chave"], "Dia a dia",
                 "fase: aguardando = segregação ainda não ligada · antes = sorteio sem segregação · segregado = envio só da intervenção", 0, 18, 12, 8),
        v1.txt("leitura", ["**Como ler as três camadas**\n", "\n",
                           "1. **Controle na intervenção** — a tabela de intervenção é a fonte da view que o OneDrive **e** a API leem. Se nenhum CPF do controle está nela, nenhum dos dois canais pode enviá-lo. Checagem paciente a paciente.\n",
                           "2. **Envio fora da intervenção** — cruza o arquivo de fato enviado (log do OneDrive, existe desde 26/10) com a intervenção do mesmo dia. Pega qualquer exame que não devia ter saído.\n",
                           "3. **Controle com caso na Central** — o desfecho: o paciente do controle não pode virar caso de navegação. Lê a tabela de casos da Central (réplica com dado de produção, `bronze_fabrica_ia_dev.navegacao`); só vale até a data de atualização dela.\n",
                           "\n",
                           "- **Controle com caso antes da segregação** são pacientes navegados enquanto o envio ainda levava o controle (até 29/10): ficam fora da análise de uplift.\n",
                           "- O envio pela **API** não tem log em PRD; a camada 1 o cobre pela origem.\n",
                           "- **Ao ligar DII ou Ateromatose:** preencher a data de segregação em `SEGREGACAO` (gerador da v1) e regerar."],
               0, 26, 12, 5),
    ]
    painel = {"datasets": datasets, "pages": [{"name": "monitoramento", "displayName": "Monitoramento PRD", "layout": L}]}
    out = AQUI / "painel-monitoramento-randomizacao-prd-v2.lvdash.json"
    out.write_text(json.dumps(painel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(out.name, "|", len(datasets), "datasets |", len(L), "widgets")


if __name__ == "__main__":
    main()
