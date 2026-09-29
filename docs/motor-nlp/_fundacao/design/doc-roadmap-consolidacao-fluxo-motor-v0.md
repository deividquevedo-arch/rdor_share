# Roadmap — Consolidação da cascata de decisão do motor NLP (fluxo + árbitro final)

**Data:** 2026-07-10 · **Status:** proposta (fase pós-homologação; NÃO iniciar no meio da validação em curso)
**Origem:** avaliação do diagrama "Componentes Motor NLP" (desenho do head) × implementação real, **confirmada empiricamente** pela homologação do E2E v21.2.

---

## Problema (o fluxo hoje não é um funil de decisão)

O `fl_relevante` de exame é montado por **mutação independente camada a camada** (`engine.process`), numa ordem em que **camadas posteriores não vetam as anteriores**:

```
rule → embeddings(promove) → calibração(anota) → judge(band-gate) → RADS(promove) → quantitativa(gate só dimensional) → fl
```

Consequência: **qualquer camada afirma relevância e chega a `fl=1` sem um juízo final coerente.** A homologação v21.2 provou 3 vazamentos estruturais:

1. **Semântica promove e o juiz nunca vê.** "bócio 0.883" em tireoide NORMAL → hybrid `fl=1`; score fora da banda `[0.35,0.65]` → **judge_llm não roda** → sem backstop. (ex.: `OBSTASYHSEE…147724`)
2. **Achado negado dispara e não há vet.** "Não se identificam nódulos ou outras lesões focais" → "lesões focais" vira positivo (negação coordenada escapa da janela).
3. **O gate só veta o dimensional.** tireoidite/linfonodo/massa/bócio (léxicos) e negados-que-escaparam **não têm gate** que os derrube.

## Alvo (o TO-BE do desenho — agora justificado por dados)

```
sinais como EVIDÊNCIA (regra+proveniência, semântica, RADS, quantitativo)
    → relevância por ACHADO (decision_trail, lib 0.3.12)
    → UM árbitro final que vê TUDO + score  → fl_relevante
```

- **Dois níveis** (achado → exame) em vez de um `fl` mutável — casa com o `decision_trail`.
- **Árbitro/vet por último**, com poder de **rebaixar** promoções fracas/contraditórias, usando a trilha como evidência.

## Insight-chave (o que realmente melhora o resultado)

**Reordenar sozinho NÃO corta FP** — o `fl` sairia igual. O ganho vem do **vet final que a trilha habilita** (poder + informação para demover). A trilha coerente é o *enabler*; o vet é a cura. Vet final pode ser **determinístico** (derruba contradição óbvia: "exame sem alterações", negado, promoção só-semântica em normal) e escalar ao LLM só na dúvida — mantém custo/429 sob controle.

## Dois horizontes (não misturar)

### Agora — fixes mecânicos (config, sem refactor, baixo risco)
Estancam o FP **na origem**; independentes do fluxo:
- **Threshold semântico** (0.88 promove normal — subir/recalibrar).
- **Negação coordenada** ("X ou outras Y" escapa da janela; fix durável = negação só-à-esquerda na lib).
- **Cobertura medida (homologação v21.2, 68 FP):** as 2 assinaturas explicam **~20/68 (29%)**. Os outros **48/68** são majoritariamente **tireoidite/linfonodo com gold-concordância possivelmente desatualizado** → resolvem na **triagem médica** (correção do gold), não no motor. **⇒ config ≈ 29%; triagem médica é o vetor maior.**

### Depois — consolidação do fluxo (refactor de orquestração do `process()`)
- Reordenar para **evidência → relevância por achado → árbitro final**; adicionar o **vet final** (determinístico + LLM na dúvida) usando o `decision_trail`.
- É o "god-method `process()`" já apontado na revisão da lib. **Não é feature nova** — é organizar o fluxo das peças existentes.
- **Restrições (load-bearing):** opt-in + **byte-compat validado** para TODAS as especialidades (BI-RADS/PI-RADS não podem mudar); agnóstico e global; incremental com rede de testes.

## Sequência recomendada
1. Fechar a **base ouro** com a triagem médica (CSV `revisao-medica-v21.2`) — dá a precisão real e corrige o gold-stale.
2. **Fixes mecânicos** (threshold + negação) — sobem precisão sem refactor.
3. **Consolidação do fluxo + vet final** — fase própria, pós-homologação, com validação byte-compat.

## Desenho TO-BE — `DecisionState` + pipeline de steps (o "grafo simplificado")

**NÃO é engine de grafo (LangGraph fica p/ a camada conversacional).** É um **pipeline de steps puros
sobre um estado compartilhado** — DAG linear com desvios condicionais. Zero dep nova, cada step
testável, byte-compat via ordem configurável. O `decision_trail` (0.3.12) é o log de travessia.

### `DecisionState` (evidência acumulada; steps leem/escrevem)
```
DecisionState:
  # entrada
  treated: str
  blocks: [ {text, organ} ]
  exm_tipo, exm_mod: str
  # EVIDÊNCIA (coletada sem decidir fl)
  findings: [ {cat, trecho, via(lexical|regex|matcher), negado:bool, excluido:(bool,motivo)} ]
  measures: { finding -> (valor, unidade, evidencia) }     # dimensional (nódulo/cisto ≥1cm)
  rads: { max_by_system, mentions }
  semantic: { score, termo, trecho }                        # SINAL, não decide sozinho
  scores: { rule, semantic, calibrated }
  # DECISÃO (preenchida pelos steps de decisão)
  finding_verdicts: [ {cat, relevante:bool|None, motivo, via} ]   # nível ACHADO
  fl: 0|1|None                                                    # nível EXAME
  decision_source: str
  uncertainty: bool                                               # aciona o juiz
  decision_trail: [ eventos ]                                     # auditoria = travessia
```

### Steps (ordem = do BARATO/DETERMINÍSTICO ao CARO/LLM; juiz por último)
| # | Step | Tipo | Lê | Decide? |
|---|---|---|---|---|
| 1 | `preparar` (to_plain + segmentar) | det. | texto | não (prepara) |
| 2 | `coletar_findings` (léxico+regex+matcher, com proveniência) | det. | blocos | não (evidência) |
| 3 | `marcar_negacao` (negação **left**) | det. | findings | marca `negado` |
| 4 | `marcar_exclusao` (qualificador: reacional/reativo) | det. | findings | marca `excluido` |
| 5 | `coletar_rads` + `coletar_semantica` + `coletar_medidas` | det.(+LLM row p/ medida) | texto/findings | não (evidência) |
| 6 | `relevancia_por_achado` | det. | findings+medidas+rads | **verdicts por achado** (claros) |
| 7 | `resolver_ambiguo_por_achado` (LLM qualitativo) | **LLM cond.** | achado ambíguo | resolve só o borderline |
| 8 | `agregar_fl` (OR dos achados relevantes) | det. | finding_verdicts | **fl da maioria** |
| 9 | `vet_final / juiz` (LLM) | **LLM cond.** | **TUDO** + score | decide **só a incerteza** |

### Quem decide o quê (o ponto central)
- **Determinístico decide a MAIORIA** (steps 6+8): achado inequívoco (nódulo/cisto ≥1cm, massa/tumor/
  bócio, TR4/5/6, linfonodo suspeito) → relevante; exame normal/negado/excluído → não-relevante.
- **LLM qualitativo (step 7) — só no achado AMBÍGUO**: linfonodo que não é claramente suspeito nem
  claramente reacional. Não roda para achado já resolvido (custo/429). Pode **promover OU rebaixar**.
- **LLM-juiz (step 9) — SÓ no exame incerto, e por ÚLTIMO**: sinais fracos/conflitantes (ex.: semântica
  alta em tireoide normal; score na banda) → o juiz vê a **evidência montada inteira** e dá a palavra
  final. **Fora da incerteza, nem é chamado.** Isso mata o bug atual "semântica promove e o juiz nunca
  vê" — agora o juiz é o último a falar quando há dúvida.

### Rotas possíveis (routing)
| Situação | Caminho | Custo |
|---|---|---|
| Achado claro relevante (≥1cm, TR4-6, bócio, linfonodomegalia) | steps 2-6-8 → fl=1 | 0 LLM |
| Exame claramente normal/negado/excluído | steps 2-3-4-6-8 → fl=0 | 0 LLM |
| Achado borderline (linfonodo suspeito?) | + step 7 (LLM qualitativo) | 1 LLM local |
| Exame incerto (sinal fraco, sem achado claro) | + step 9 (juiz) | 1 LLM juiz |
| Borderline **e** incerto | steps 7 **e** 9 | até 2 LLM (raro) |

### Por que essa ordem (e por que o juiz por último faz sentido)
- **Barato → caro:** determinístico resolve ~a maioria a custo zero; LLM entra só onde agrega (borderline/
  incerto). Controla 429.
- **Evidência antes de decidir:** nenhum step promove `fl` "no meio" sem que o agregador/juiz veja o todo
  → coerência + auditabilidade (a trilha É a decisão).
- **Bug-by-construction eliminado:** o vazamento do anchor (v21.6), semântica-em-normal e negado-que-
  escapou não acontecem, porque a relevância é decidida sobre a evidência montada, não por mutação
  independente com bookkeeping frágil de gate.

### Compat / migração
- `decision_flow: "cascade_v1" (default, byte-compat) | "funnel_v2"`. O v2 é opt-in por especialidade;
  o default reproduz o comportamento atual (BI-RADS/PI-RADS intocados) até validação byte-compat.
- Implementação incremental: extrair os steps do `process()` atual **sem mudar decisão** (refactor puro,
  testes verdes) → depois **inserir o vet final** e a ordem funnel como v2.

## Referências
- Diagrama: `.alt.doc/Mapa do Sistema Rede D'Or NLP Base.drawio` (página "Componentes Motor NLP", notas AS-IS).
- Evidência: E2E v21.2 (`ntb_ia_motor_e2e_full_v21.2.csv`) — precisão 63,4% / recall 98,3%; 68 FP.
- Trilha: `decision_trail` (lib 0.3.12), opt-in via `emit_decision_trail`.
- Fluxo real (linhas): `engine.py:295-379` (embeddings→calibração→judge→RADS→quantitativa).
