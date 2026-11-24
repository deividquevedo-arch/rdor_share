# Histórico mensal v2: correções de precisão sobre a v1

> 19/11/2026. A v1 (`inventario-fontes.md`, `mensal-consolidado-linha.json`) é mantida. A v2 está em
> `mensal-por-linha-unidade-v2.json` (por exame, `extrair_historico_v2.py`) e
> `mensal-consolidado-linha-v2.json` (linha × mês, `consolidar_v2.py`). Só agregados.

| # | problema na v1 | correção na v2 | efeito medido |
|---|---|---|---|
| 1 | Mês = 1ª execução do modelo; carga retroativa inflava o mês da carga | mês = **data do exame** | TI-RADS jun: 55.460 → 9.315 · Ca Rim set: 182.130 → 112.690 · Reumato mar: 94.090 → 92.143 |
| 2 | Ateromatose lida de `diamond_fabrica_ia_hml.legado` (para em 02/09), lacuna falsa em set | espelho `diamond_fabrica_ia.legado` (execução até 01/10, 62 dias) | set: lacuna → 18.532 exames |
| 3 | Fonte do mês escolhida pelo 1º mês completo na plataforma | fonte que **operou** o mês: plataforma se no ar até o dia 2; senão legado, se rodou até o fim do mês (tolerância 3 dias); senão a plataforma, se a janela retroativa cobriu o mês | Reumato set, PI-RADS out e Biliares out passam a mês completo pela plataforma |
| 4 | Captação manual do PACS (enave) com prioridade sobre a plataforma | enave só quando não há motor; marcado como não-algoritmo | PI-RADS out: enave 452 → plataforma 7.388 |
| 5 | Nefro sem relevante | **clearance < 30 por exame**; acompanha o `qtd_relevante` do job (jan 2.948 × 2.848) | ~3,0–3,9 mil/mês |
| 6 | Unidades com código antigo (até jan/26) contadas como hospitais distintos | `mapa-unidade-codigo-antigo.json` (código antigo → atual, pelo nome do hospital na gold) | hospitais em jan: Mama 110 → 68, Fígado 111 → 68, DII 100 → 60, Nefro 90 → 54 |

## Fatos verificados no caminho

- **Nefro, fev–mar:** não houve reprocessamento. Em 09/02 o monitoramento do job passou de ~6 para ~62
  registros por dia e triplicou `qtd_saida`/`qtd_relevante`; em 02/04 voltou. Por exame (deduplicado),
  o volume é estável no ano.
- **Envio da plataforma entre 29/09 e 23/10:** sem registro em prd. `tb_mov_log_execucao` só tem dev
  e hml (desde 03/11); `tb_mov_onedrive_saida` começa em 23–26/10. Lacuna confirmada.
- **Biliares antes da plataforma:** o espelho não tem mais histórico por exame que a cópia hml; segue
  a contagem do job (não deduplicada).

## Pendências de decisão

- **Tireoide jan–mai:** exames processados **retroativamente** pelo motor legado em 15/06 (marca `r`).
  Mesma régua do motor, mas não havia envio nesses meses. Alternativa: captação manual (enave).
- **Códigos sem sucessor inequívoco:** SÃO LUCAS (200144), HOSPITAL ASSUNCAO (100970), HOSPITAL JUTTA
  BATISTA (200149), mantidos com o código antigo.
- **Mudança de universo na troca legado → plataforma:** TI-RADS ~10 mil/mês no legado contra ~35 mil na
  plataforma (filtro de entrada mais amplo); Ca de Cólon perde a TC a partir de out.
