# Base de estabilidade comportamental — Hepato (Fase 0)

> **Não é base ouro vs médico** (o board hepato usa findings/seeds antigos → mediria mal a
> qualidade e o match_rate não bate porque o motor está MELHOR que o legado). Aqui a "verdade" é o
> **output do motor atual aceito** (config 0.1.13 + LLM), congelado sobre uma coorte fixa. Mede
> **regressão** (não qualidade): mudança futura que altere o fl de laudos é detectada. Análogo ao
> golden byte-compat, mas sobre a coorte real do hepato (com LLM).

## Baseline congelado
- **Fonte**: run do lake `diamond_hepatologia.workarea.dev_tb_diamond_mod_hepatologia_motor_saida`,
  `dt_execucao=2026-06-10`, `config_version=0.1.13-hep-emb-volume`, com LLM (claude-haiku).
- **Coorte**: 5735 linhas → **5690 ids únicos** (45 exames processados 2×; 2 com fl divergente =
  não-determinismo do LLM na banda de incerteza, temp=0 não é 100%). Dedup por id_exame.
- **Snapshot**: `baseline-comportamental-hepato-0.1.13-2026-06-10.csv` (id_exame, fl, decision_source).
- **Congelado**: `n=5690` · `relevantes=871` · `sha=c6061919a602b27be6b56a30e72b3757`.
- **decision_source**: llm_router_llm_negative 3482 · hybrid_calibrated 1432 · llm_router_llm_positive
  820 · abstain_invalid_json 1 → **LLM real** (a config 0.1.13 está comprovadamente funcional).

## Por que estabilidade, não qualidade
O board médico foi montado sobre retornos de dados **antigos** (findings/seeds anteriores). Comparar
o motor atual (findings novos) contra ele mistura "erro do motor" com "evolução de critério" — por
isso as duas frentes de homologação (board histórico prec 0,21 · Carol 73 casos prec 0,79). Para o
gate das Fases 3/4 o que importa é **não regredir** o comportamento aceito, e isso o snapshot mede
sem depender do gabarito contaminado.

## Harness Fase 0
`Desktop/Rede D'Or/_ferramentas/baseohro_hepato_estabilidade.py` — **fora do git**, junto
dos demais artefatos que tocam gabarito.

⚠️ Estava em `.claude/jobs/<id>/tmp/`, diretório temporário que é apagado com o job. Movido
em 2026-09-02. O destino definitivo é o **lake**, com alinhamento pendente sobre schema e
formato — o mesmo destino previsto para reuso posterior e treinamento de modelo proprietário.

Interface:
- sem args: re-valida o snapshot local vs os números congelados (sha/n/rel).
- `--fl-csv <path>`: compara um novo run (id_exame, fl) sobre a MESMA coorte vs o baseline →
  reporta flips (0↔1). `NOISE_TOLERANCE=15`: flips ≤ 15 = ruído de não-determinismo LLM (estável);
  acima = regressão OU mudança intencional/opt-in a revisar.

## Uso nas próximas fases
Antes e depois de uma mudança comportamental (F3 semântica→finding, F4 reordenar cascata), rodar o
motor sobre a mesma coorte (5690) e comparar vs este baseline. Poucos flips = mudança contida;
muitos = impacto real a validar clinicamente.
