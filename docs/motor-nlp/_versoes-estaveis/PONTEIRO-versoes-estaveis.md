# Ponteiro — Versões estáveis homologadas (configs-piloto)

> Backup pessoal das configs de negócio **homologadas e validadas contra base ouro**. Cada entrada
> aponta a versão estável + métricas + fonte de validação. Snapshot das configs `.py` nesta pasta.
> **Atualizado: 2026-07-21.** Requer `nlp_engine >= 0.5.8` (arquitetura alvo F1–F4).

## Estado
As 3 pilotos foram consolidadas na `branch-from-versao-alpha` (branch canônica de negócio da
plataforma). Este backup preserva o snapshot + as métricas de homologação. **Nenhuma em produção**
(routing clínico) ainda — validadas em pilotos contra base ouro.

---

## 🫁 Transplante de Pulmão — `0.1.1-pulmao-v1-target`
- **Arquivo:** `ntb_ia_transplante_pulmao_config.py`
- **Perfil:** quantitativo puro (VEF1/CVF/DLCO por limiar; `on_met: promote`). Sem embeddings/juiz.
- **Arquitetura:** F4 `pipeline_order: target` (adotada, validada 1:1). F3 N/A (sem semântica).
- **Base ouro:** Carol (388) + recall amostral (1470) → gabarito humano.
- **Métricas (base ouro):** **TP=126 · FP=1 · FN=0 · TN=1727** — Recall **1,000** · Precisão **0,9921** · **MCC 0,996**.
- **Único erro:** 1 FP (VEF1 0,68% = razão lida como %; FP-01, guard opcional pendente).
- **Validação:** run E2E jun/2026 (1852 laudos) + F4 confirmada 1:1 (2026-07-20).
- **Doc:** `docs/motor-nlp/pulmao/homologacao-pulmao-v1-metricas.md`.

## 🦋 Tireoide / TI-RADS — `0.1.0-tirads-rads-v22.7-target`
- **Arquivo:** `ntb_ia_tirads_config.py`
- **Perfil:** TI-RADS (rads_extraction) + achados clínicos + embeddings(hybrid) + llm_router(haiku) + quantitative + document_vet.
- **Arquitetura:** F4 `pipeline_order: target` (adotada, validada 1:1). **F3 `emit_as_finding` OFF** (gerava +10 FP: embedding "bócio"~tireoide-normal).
- **Base ouro:** `base-ouro-tirads-2026-07-11.csv` — **586 confirmados** (300 pendentes fora de escopo).
- **Métricas (base ouro, produção c/ LLM):** **TP=133 · FP=8 · FN=1 · TN=444** — Recall **0,9925** · Precisão **0,9433** · **MCC 0,958**.
- **Validação:** run E2E (895) lib 0.5.5→0.5.8; F4 confirmada 1:1 (2026-07-20).
- **Doc:** `docs/motor-nlp/tireoide/base-ouro-tirads-metricas.md` + `relatorio-final-tirads-v1-2026-07-12.md`.

## 🩺 Hepatologia — `0.1.13-hep-emb-volume`
- **Arquivo:** `ntb_ia_hepatologia_config.py`
- **Perfil:** LLM-driven (embeddings + llm_router; o juiz decide a maioria). Legacy (F4 não testada aqui).
- **Base de referência:** **estabilidade comportamental** (não qualidade vs médico — o board médico
  usa findings/seeds antigos; o motor evoluiu, daí match_rate baixo não é erro).
- **Snapshot congelado:** run 0.1.13 c/ LLM (2026-06-10), coorte 5690 ids únicos, **871 relevantes**,
  **sha `c6061919a602b27be6b56a30e72b3757`** — baseline de não-regressão p/ mudanças futuras.
- **Nota:** medição vs board (board_cohort 2000) dá prec ~0,21 (contaminada por findings antigos);
  homologação Carol (73, findings novos) dá prec ~0,79. Número real entre as frentes.
- **Doc:** `docs/motor-nlp/hepatologia/baseline-comportamental-hepato-metricas.md`.

---

## Notas de rastreabilidade
- **Snapshot vs vivo:** as `.py` aqui são cópias congeladas. As versões vivas estão em
  `branch-from-versao-alpha` (plataforma). Ao promover uma nova versão estável, atualizar este ponteiro
  + copiar a config + registrar as métricas da base ouro.
- **Base ouro:** os gabaritos humanos (com PHI) NÃO ficam aqui — só as métricas agregadas. Fontes nos
  docs de homologação citados.
- **Harness de validação:** `.claude/jobs/56d76a3e/tmp/baseohro_{pulmao,tirads}.py` +
  `baseohro_hepato_estabilidade.py` (reproduzem as matrizes contra a base ouro/estabilidade).
- **Arquitetura alvo (lib):** F1 (LLM conexão/modelo) + F2 (config v2 ordinal/validação) + F3
  (semântica→finding, opt-in) + F4 (ordem coerente) — todas em produção na hml (nlp_engine v0.5.8).
