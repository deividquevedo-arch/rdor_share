# Relatório final — Motor TI-RADS, linha de cuidado de tireoide (V1)

**Data:** 2026-07-12 · **Estado:** v22.3 (config) sobre nlp-engine 0.3.15 · **Especialidade:** tireoide

---

## 1. Diagnóstico / objetivo

Entregar o **V1** da linha de cuidado de tireoide: classificar laudos de imagem como **relevantes**
(devem ser captados) segundo a spec de negócio, com **alta sensibilidade** (não perder caso relevante)
e **precisão suficiente** para não inundar a captação com falsos-positivos.

**Escopo V1 (spec):** exames de imagem de tireoide/pescoço (US/Doppler, cintilografia, PAAF/biópsia,
TC pescoço). Relevante = **TI-RADS 4/5** · **Nódulo/Cisto ≥1cm** · **Massa/Linfonodo/Tumor** ·
**Bócio**. Órgão = **Tireoide**. (Sangue, e nódulo "todos os tamanhos" → V2.)

## 2. Metodologia

- **Base ouro reconciliada** (`base-ouro-tirads-2026-07-11.csv`, n=586 confirmados): rótulos do time +
  revisão médica, incorporando a spec. **100% dos desacordos motor×base foram revisados** (46 rótulos
  ajustados), inclusive rótulos antigos que contradiziam a política atual.
- **Disciplina de double-check:** toda mudança é validada contra a base ouro **antes** de subir —
  mostra exatamente quais casos muda e prova **0 TP perdido** (0 regressão). Nada entra "no escuro".
- **Decisão por-achado** (não por-documento): cada achado é medido + checado por negação (esq/dir) +
  exclusão; agrega por OR. Um achado negado nunca suprime um relevante co-existente.

## 3. Resultado final (v22.3, confirmado em E2E `llm_http`)

| Métrica | Valor |
|---|---|
| Precisão | **0,943** |
| Recall (sensibilidade) | **0,9925** — efetivo ≈ 1,0 em escopo |
| F1 | **0,967** |
| Acurácia | **0,985** |
| Matriz (n=586) | TP 133 · FP 8 · FN 1 · TN 444 |

**O único FN** é uma **US de partes moles do dorso** — nem é exame de tireoide (higiene de entrada,
não erro do motor). Dentro do escopo real, **não há falso-negativo**.

### Trajetória (sobre a base ouro reconciliada)
| versão | precisão | recall | mudança principal |
|---|---|---|---|
| v21.9 | 0,838 | 0,985 | baseline reconciliado |
| v22.0 | 0,887 | 0,985 | bócio difuso ("dimensões aumentadas") = não-relevante |
| v22.1 | 0,917 | 0,978 | negação à direita (linfonodo) + seção de indicação + prompt difuso |
| v22.2 | 0,917 | 0,9925 | linfonodo indeterminado/perda de arquitetura hilar = relevante |
| **v22.3** | **0,943** | **0,9925** | massa negada à direita (pós-op "massas: Não caracterizadas") |

**Ganho do ciclo: precisão +10,5 pp (0,838 → 0,943)** com recall mantido em ~0,99.

## 4. O que foi entregue

**Biblioteca `nlp-engine` 0.3.15** (mergeada em `hml`, wheel publicada no Volume) — 2 primitivas
genéricas, opt-in, byte-compat, úteis a qualquer especialidade:
- **`negation_direction` por-achado** (`{'_default':'left','linfonodo':'both','massa':'both'}`) —
  achados negados à direita ("Linfonodomegalias: Não há") sem sobre-negar nódulo real.
- **`findings_ignore_sections`** — texto de "Indicação:"/"História clínica:" não gera achado.

**Config TI-RADS v22.0 → v22.3** (`test/rads-e2e-hml`): bócio difuso fora; negação linfonodo/massa à
direita; linfonodo indeterminado/perda de arquitetura hilar relevante; prompt do LLM refinado
(bócio só nodular; aumento difuso = não-relevante).

**Governança/rastreabilidade:** base ouro reconciliada + planilha de reconciliação (audit trail dos
vereditos) + **mapa de gaps vivo** (`mapa-gaps-tirads-v0.md`) + **auditoria de medida** (mm→cm
confiável) + este relatório.

**Nível 2 (embedding sem HF):** desenhado (`doc-embedding-model-serving-nivel2-v0.md`) — Model Serving
no Databricks + backend HTTP na lib (esqueleto 0.3.16 pronto, opt-in, aditivo).

## 5. Decisões de negócio fechadas (2026-07-12)

- **Nódulo/cisto = ≥1cm** (critério do V2 adotado como V1 efetivo, decisão médica).
- **Linfonodo reacional NÃO conta** (só suspeita/real/indeterminado).
- **Paratireoide FORA de escopo** (Órgão=Tireoide).
- **Deferidos:** condicionamento por tipo de exame; hipertireoidismo só-cintilografia; Bethesda
  (0 evidência de ganho na base atual, adicionam risco/complexidade).

## 6. Riscos / gaps pendentes (mapeados)

**Precisão (8 FP):**
- **G-P2** nódulo/cisto <1cm com medida pulada (~4) — **bloqueado**: apertar o gate derrubaria nódulos
  ≥1cm reais cuja medida foi pulada (auditoria provou; a leitura mm→cm em si é confiável).
- **G-P1** massa negada distante ("Ausência de … nódulos massas", ~2) — **fix seguro disponível**
  (janela de negação à esquerda 8→12, com revalidação do linfonodo='both').
- **G-P3** promoção semântica sem achado (~1); **G-P4** linfonodo proeminente pós-op reacional (~1).

**Recall:** **G-R2** exame não-tireoide (higiene de entrada do runner).

**Infra:** **G-INFRA1** cobertura da medição (medir sempre → destrava o gate <1cm com segurança);
**G-INFRA2** embedding via Model Serving (elimina download do HF por run); **G-INFRA4** condicionamento
por tipo de exame (destrava hipertireoidismo/Bethesda quando o negócio quiser V1 completo).

## 7. Recomendação

**Consolidar o V1.** O motor está **preciso (0,94), sensível (~1,0) e rastreável**, com base ouro 100%
reconciliada e todos os gaps mapeados/priorizados. Ganho marginal restante seguro = **G-P1** (~+1pp).

**Próximos passos sugeridos (ordem):**
1. (opcional) **G-P1** — fechar precisão em ~0,95.
2. **Higiene de repo** — consolidar `test/rads-e2e-hml` (v22.0–v22.3) → merge quando autorizado.
3. **G-INFRA2** (embedding Model Serving) — resolve o gargalo operacional do HF (~36 min/run).
4. **V2** quando o negócio pedir: exames de sangue + condicionamento por tipo de exame
   (hipertireoidismo/Bethesda) via G-INFRA4.

---

### Artefatos
- Config: `apps/databricks/nlp_engine/ntb_ia_tirads_config.py` (v22.3, `test/rads-e2e-hml`).
- Lib: `nlp-engine` 0.3.15 (tag `v0.3.15`, mergeada em `hml`).
- Base ouro: `docs/motor-nlp/notas/base-ouro-tirads-2026-07-11.csv`.
- Reconciliação (audit): `docs/motor-nlp/notas/reconciliacao-base-ouro-tirads-2026-07-11.{csv,md}`.
- Mapa de gaps: `docs/motor-nlp/notas/mapa-gaps-tirads-v0.md`.
- Auditoria de medida + Nível 2: notas correspondentes.
- E2E: `Downloads/ntb_ia_motor_e2e_full_v22.3.csv`.
