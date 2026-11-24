"""Notas curtas v3: cada desvio, lacuna ou outlier com a FONTE que o explica.

Base: levantamento/levantamento-{repos,docs,workspaces,board}.md (19/11/2026). Nada e construido:
os numeros sao os do consolidado v2; a nota so explica. Sem fonte -> "causa nao registrada".
Gera mensal-consolidado-linha-v2-notas-v3.json (acrescenta nota_curta e fonte_nota) e
notas-curtas-v3.md (tabela legivel).
"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent
M = [f"2026-{i:02d}" for i in range(1, 11)]

# (linha, meses) -> (nota, fonte)
N = [
    # --- Ateromatose
    ("ateromatose", M[:5], "Linha iniciada em 23–24/06/2026.", "PR 6655 (fabrica-ia-plataforma); 1ª gravação 24/06"),
    ("ateromatose", ["2026-06"], "Estreia em 24/06 (mês parcial).", "PR 6655; tabela de saída do legado"),
    ("ateromatose", ["2026-07"], "Régua ajustada: faixa 45–70 anos (03/07) e exigência de lesão coronariana difusa/importante, com calcificação fora do escopo (15–20/07), por decisão da Cardiologia. Relevantes caem de 1.285 (jun) para 132–170/mês (ago–set).",
     "PRs 6747 e 6844; ntb_ia_ateromatose_algoritmo_log; cards 182295 — [Ateromatose Coronariana] Melhorias Algoritmo (especificidade), 194937 — [Ateromatose Coronariana] Filtro de idade - 45 a 70 anos, 197189 — [Ateromatose Coronariana] Ajuste Findings TC de Torax"),
    ("ateromatose", ["2026-07"], "Por hospital, envios acima dos relevantes neste mês: o envio foi registrado na época com a régua anterior, e a tabela de saída guarda a marcação da régua nova (15–20/07).", "monitoramento do job (envio) × tabela de saída do legado"),
    ("ateromatose", ["2026-08"], "Envios interrompidos de 03 a 13/08 (job no cluster errado).", "cards 279307 — [Ateromatose Coronariana] Interrupção dos envios desde 3.8 e 279331 — Erro ateromatose - 13/08"),
    ("ateromatose", ["2026-09"], "Último mês do algoritmo legado (até 01/10).", "histórico de gravação de diamond_fabrica_ia.legado…ateromatose_coronariana_saida"),
    ("ateromatose", ["2026-10"], "Plataforma desde 29/09, com a régua migrada (config própria); relevantes acima do legado.", "deploy de 28/09 (PRs 7444/7432/7431); 1ª execução 29/09"),
    # --- Ca de Cólon
    ("cancer_colon", ["2026-01"], "Linha iniciada em 19–20/02/2026.", "PRs 5707 e 5711"),
    ("cancer_colon", ["2026-02"], "Estreia em 20/02, só laudos de imagem (mês parcial).", "PRs 5707/5711; 1ª gravação 20/02"),
    ("cancer_colon", ["2026-03"], "Colonoscopia passa a ser processada em 26/03.", "PR 5972"),
    ("cancer_colon", ["2026-09"], "Último mês do algoritmo legado (até 01/10).", "job ia-colon, último run 01/10"),
    ("cancer_colon", ["2026-10"], "Troca para a plataforma (régua migrada).", "deploy de 28/09; 1ª execução 29/09"),
    # --- DII
    ("dii", ["2026-01", "2026-02"], "Régua anterior: 5,9% (jan) e 3,6% (fev) de relevantes. Régua revista em 20–24/02 (limiar de similaridade 0,82 → 0,90) e oficializada em 12/03; ~0,6% a partir de mar.",
     "PRs 5684 e 5750; queda na execução de 20/02; cards 144769 — [DII] Desenvolvimento Algoritmo TC e RM - Novo escopo acordado com negócio e 143856 — [DII] Ajustes algoritmo TC e RM - Cenário 4"),
    ("dii", ["2026-03"], "Colonoscopia passa a ser processada entre 14 e 19/03.", "histórico git (fabrica-ia-plataforma)"),
    ("dii", ["2026-09"], "Último mês do algoritmo legado (até 28/09; execuções de 29/09 a 01/10 falharam por falta de cota de VM no Azure).", "runs do job legado DII"),
    ("dii", ["2026-10"], "Troca para a plataforma (régua migrada).", "deploy de 28/09; 1ª execução 29/09"),
    # --- Reumatologia
    ("reumatologia", ["2026-01"], "Linha iniciada em 28/01, com janela retroativa de 30 dias.", "histórico git do legado; Data Card (deploy 02/02)"),
    ("reumatologia", ["2026-03", "2026-04"], "Filtro de entrada ampliado em 16/03 (1 → 3 colunas) e corrigido em 25/03 (exames com campo nulo deixam de ser descartados): volume cresce até abr.",
     "PRs 5899/5901 e 5970 (IAAzureDatabricksReumatologia)"),
    ("reumatologia", ["2026-08"], "Região craniana retirada do escopo em 23/07: entrada diária cai de ~6,4 mil para ~3,9 mil.", "PR 6893; tabela de saída a partir de 24/07"),
    ("reumatologia", ["2026-09"], "Troca para a plataforma: legado até 16/09, plataforma desde 11/09.", "último run do job legado 16/09; tabela de prd criada 11/09"),
    # --- Tireoide
    ("tirads", M[:5], "Exames liberados nos 180 dias anteriores, processados na estreia do algoritmo (15/06); sem envio no mês.",
     "janela de entrada do legado (dt_liberacao_laudo ≥ hoje − 180 dias); tabela criada 15/06"),
    ("tirads", ["2026-06"], "Estreia do algoritmo em 15/06.", "tabela de saída criada 15/06"),
    ("tirads", ["2026-08"], "Último mês do algoritmo legado (até 02/09); plataforma em prd desde 21/08.", "tabelas de saída do legado e da plataforma"),
    ("tirads", ["2026-09"], "Plataforma: o filtro de entrada seleciona pelo nome do exame (tireoide ou pescoço) sem exigir modalidade; volume ~3,5x o do legado.",
     "config TI-RADS da plataforma; filtro do legado exigia modalidade e região"),
    # --- Próstata
    ("pirads", M[:3], "Captação manual no PACS (enfermeira navegadora); algoritmo a partir de 27/05.", "hive_metastore.ia.enave_df_historico_pirads; PR 6377"),
    ("pirads", ["2026-04"], "Só exames do fim de abr processados na estreia do algoritmo (parcial).", "PR 6377; tabela criada 27/05"),
    ("pirads", ["2026-10"], "Plataforma desde 12/10: filtro de entrada sem restrição de sexo e modalidade (decisão de 29/09); volume ~3x, o excedente sem relevantes.",
     "cabeçalho da config 0.1.1-pirads; E2E em dev (6.521 × 2.228 laudos); legado desativado após envio em dobro em ~14/10 (card 373805 — Envios Retroativos)"),
    # --- Doenças biliares
    ("doencas_biliares", M[:3], "Linha iniciada em 02–05/04/2026.", "PR 6038; tabela piloto criada 05/04"),
    ("doencas_biliares", ["2026-04"], "Piloto (1ª versão até 13/04, depois V2).", "PR 6038; histórico git"),
    ("doencas_biliares", ["2026-05"], "Exames de pronto-socorro passam a entrar em 04/05.", "PR 6207"),
    ("doencas_biliares", ["2026-05", "2026-06", "2026-07"], "Contagem do próprio job (pode repetir exame); sem abertura por hospital.", "monitoramento do job legado"),
    ("doencas_biliares", ["2026-08"], "Contagem do job passa a incluir todas as regionais (antes só SP/RJ) em 28/07.", "PR 6919"),
    ("doencas_biliares", ["2026-09"], "Contagem do job legado (pode repetir exame); falhas por falta de cota de VM no Azure a partir de 22/09.",
     "runs do job legado; tabela de prd criada 12/10"),
    ("doencas_biliares", ["2026-10"], "Plataforma desde 12/10; mês completo pelo processamento inicial.", "deploy de 09/10; 1ª execução 12/10"),
    # --- Neuroimunologia
    ("neuroimunologia", M[:2], "Linha iniciada em abr/2026.", "notebooks e monitoramento criados 16–30/04 (histórico git)"),
    ("neuroimunologia", ["2026-03"], "Só exames do fim de mar processados na estreia (parcial).", "tabela de saída (1ª gravação 29/04)"),
    ("neuroimunologia", ["2026-08"], "Sem envios de 24/08 a 04/09.", "card 298100 — [Neuroimune] Não há envios desde o dia 24/08"),
    ("neuroimunologia", ["2026-10"], "Troca para a plataforma: legado até 01/10, plataforma desde 29/09.", "último run do legado 01/10; deploy de 28/09"),
    # --- Nefrologia: mudanças de régua
    ("nefrologia", ["2026-02", "2026-03"], "O monitoramento do job contava a mais neste período (data de execução sobrescrita no reprocessamento; fev: 8.624 contados × 3.584 reais), corrigido em 31/03. O número oficial é o real.",
     "card 154348 — [Rim] Monitoria trazendo volume maior que o real; PR 6019"),
    ("nefrologia", ["2026-04"], "Régua ajustada: idade até 80 anos (08/04) e exclusão de 44 unidades (24/04).", "PRs 6067 e 6166 (IAAzureDatabricksRim)"),
    # --- Relevante incompleto na origem (enviado oficial > relevante medido)
    ("pulmao_nodulo", ["2026-01"], "Relevante sem registro completo: a tabela do job começa em 27/01 (linha em produção desde 16/01).", "hive_metastore.ia.tb_diamond_mod_pulmao (1ª data 27/01)"),
    ("doencas_biliares", ["2026-03"], "Relevante sem registro: linha em produção desde 27/03, mas a tabela do legado só guarda dados desde 03/04.", "tabelas de saída do legado (1ª gravação 03–05/04)"),
    ("transplante_pulmao", ["2026-08"], "Em agosto o envio saiu do job de HML (18 a 30/08; 188 relevantes); PRD falhou em 26/08 (LLM 403) e enviou a partir de 31/08. O mês soma HML (18–30/08) e PRD (31/08).", "saídas de HML e PRD (diamond_fabrica_ia_hml / diamond_fabrica_ia); card 246669"),
    # --- Linhas só da plataforma
    ("cancer_estomago", M[:6], "Linha iniciada na plataforma em 26/08/2026.", "job na main em 24/08 (PR 7110); 1ª execução 26/08"),
    ("cancer_estomago", ["2026-07"], "Só exames do fim de jul processados na estreia (parcial).", "1ª execução 26/08"),
    ("transplante_pulmao", M[:6], "Linha iniciada na plataforma em 26/08/2026.", "tabela de prd criada 26/08"),
    ("transplante_pulmao", ["2026-07"], "Só exames do fim de jul processados na estreia (parcial).", "1ª execução 26/08"),
    ("cancer_rim", M[:7], "Linha iniciada na plataforma em 10/09/2026.", "PRs 7251/7252; tabela de prd criada 10/09"),
    ("cancer_rim", ["2026-08"], "Só exames de ago processados na estreia (parcial).", "1ª execução 10/09"),
    ("cancer_rim", ["2026-10"], "Hospitais com exame sobem de 99 (set) para 117 (out): revisão das unidades da linha (inclusão de PE e unidades satélite) em produção em 13/10.", "card 365142 — [Captação] Revisão das unidades enviadas para time Giovanni Targa (confiança média)"),
    ("tumor_osseo", M[:7], "Linha iniciada na plataforma em 29/09/2026.", "PR 7432; deploy de 28/09"),
    ("tumor_osseo", ["2026-08"], "Só exames do fim de ago processados na estreia (parcial).", "1ª execução 29/09"),
]
# Notas fixas por linha (critério), quando o mês não tem nota própria.
FIXAS = {
    "nefrologia": ("Não é NLP: cálculo de clearance. Relevante = enviado = clearance ≤ 35 (filtro do Salesforce); números oficiais da planilha do portfólio. Por hospital: recontagem com a régua atual do job.",
                   "planilha do portfólio (aba 'Envios IAs Captação'); rim_monitoramento (qtd_relevante)"),
    "mama": ("Relevante = BI-RADS 4 ou 5 (regra do job); o envio do legado inclui todas as categorias.",
             "birads_monitoramento (qtd_relevante); view vw_diamond_mod_birads_complemento"),
    "pulmao_nodulo": ("Relevante = indicação de encaminhamento; o envio se restringe a 21 unidades de SP com navegação (desde 19–20/03) e idade 20–85 (desde 22–23/02).",
                      "PR 5919; cards 149972 — [Pulmão][SP] Enviar apenas 21 unidades com navegação de pacientes e 145518 — [Pulmão] Ajustes algoritmo Pulmão"),
    "hepatologia": ("Algoritmo legado. A plataforma rodou em paralelo de 21/08 a 14/10, sem entrega; job pausado em 14/10, motivo não registrado.",
                    "PRs 7605/7606 (prod.json PAUSED); tabelas de prd"),
}
GERAL = [
    ("Enviados", "Números oficiais da planilha do portfólio (aba 'Envios IAs Captação'): para as linhas com envio por e-mail, contagem do enviado no dia; Mama, Nefro e Nódulo pulmonar com fluxo próprio. Nefro: enviado = clearance <= 35 (filtro do Salesforce).",
     "Portfólio Forum de IAs RDOR.xlsx, aba 'Envios IAs Captação' (lida em 23/11/2026)"),
    ("Unidades", "Código de unidade migrou para o ID do MDM (26/01/2026; na gold, exames desde 08/01, hospitais TASY e WPD); códigos antigos convertidos pelo nome do hospital.",
     "card 135635 — 26/01: Migração do ID das Unidades MDM na tabela Observation Exames (Engenharia); PRs de 28–29/01; gold_corporativo_ia.corporativo.tb_gold_mov_exame"),
    ("Envios, out", "Envios da plataforma indisponíveis em 12–14/10 (Logic App sem segredo; trocado em 14/10).", "card 371813 — [Captação] Todas as linhas indisponíveis"),
]


def main():
    d = json.loads((OUT / "mensal-consolidado-linha-v2.json").read_text(encoding="utf-8"))
    idx = {}
    for linha, meses, nota, fonte in N:
        for m in meses:
            idx.setdefault((linha, m), []).append((nota, fonte))
    for r in d["linhas"]:
        itens = idx.get((r["linha"], r["mes"])) or ([FIXAS[r["linha"]]] if r["linha"] in FIXAS and r["exames"] is not None else [])
        r["nota_curta"] = " ".join(n for n, _ in itens) or None
        r["fonte_nota"] = "; ".join(f for _, f in itens) or None
    d["meta"]["notas_gerais"] = [{"tema": t, "nota": n, "fonte": f} for t, n, f in GERAL]
    (OUT / "mensal-consolidado-linha-v2-notas-v3.json").write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    md = ["# Notas curtas v3 — histórico mensal jan–out/2026", "",
          "> Cada nota explica um desvio, lacuna ou outlier observado nos dados, com a fonte. Números: consolidado v2.", "",
          "## Gerais", "", "| tema | nota | fonte |", "|---|---|---|"]
    md += [f"| {t} | {n} | {f} |" for t, n, f in GERAL]
    md += ["", "## Por linha e mês", "", "| linha | mês | nota | fonte |", "|---|---|---|---|"]
    md += [f"| {r['linha']} | {r['mes']} | {r['nota_curta']} | {r['fonte_nota']} |" for r in d["linhas"] if r["nota_curta"]]
    (OUT / "notas-curtas-v3.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(sum(1 for r in d["linhas"] if r["nota_curta"]), "registros com nota")


if __name__ == "__main__":
    main()
