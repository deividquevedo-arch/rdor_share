# Mapa de gaps — motor TI-RADS (linha de cuidado tireoide V1)

Registro vivo do que sabemos e ainda NÃO corrigimos, com o porquê e o que destrava cada item.
Atualizar a cada iteração. Base de avaliação: **base ouro reconciliada** `base-ouro-tirads-2026-07-11.csv`
(n=586 confirmados, 46 rótulos revisados). Métrica atual **v22.3** (projeção): prec ~0,943 · recall 0,985.

## Estado das versões (base ouro reconciliada)
| versão | precisão | recall | mudança |
|---|---|---|---|
| v21.9 | 0,838 | 0,985 | baseline reconciliado |
| v22.0 | 0,887 | 0,985 | bócio difuso fora |
| v22.1 | 0,917 | 0,978 | negação-direita (linfonodo) + indicação + prompt difuso |
| v22.2 | 0,917 | 0,985 | linfonodo indeterminado/perda arquitetura hilar = relevante |
| **v22.3** | **0,9433** | **0,9925** | massa negada à direita (pós-op "massas: Não caracterizadas") — CONFIRMADO E2E |
| **v22.4** | (E2E pend.) | — | "ausente(s)" = negação ("Linfonodos atípicos: ausentes"); rule-layer −8 FP, 0 TP perdido |

Todos exigem **wheel nlp_engine >= 0.3.15**. **v22.3 = estado atual** (TP133/FP8/FN1/TN444; recall
efetivo ≈1,0 — o único FN é G-R2, exame não-tireoide). **8 FP restantes:** ~2 massa distante (G-P1),
~4 nódulo/cisto <1cm met=None (G-P2, bloqueado), ~1 linfonodo (G-P4), ~1 semântico (G-P3).

---

## SPEC DE NEGÓCIO (referência) — reconciliação
Exames de imagem em escopo: **US tireoide (doppler), US pescoço+tireoide, Doppler tireoide,
cintilografia tireoide, PAAF/punção tireoide, biópsia tireoide, TC pescoço** (partes moles/laringe/
tireóide/faringe — "não específico"). **Órgão: Tireoide.** Exame de sangue = **V2**.
Palavras-chave: **TI-RADS 4/5** (imagem) · **Nódulo/Cisto** (V1 TODOS / V2 >1cm) · **Massa/Linfonodo/
Tumor** (imagem) · **Bócio/mergulhante** (imagem) · **Hipertireoidismo** (só cintilografia) ·
**Bethesda** (só punção/biópsia). V1 = TI-RADS4/5 + Bócio + Nódulo/cisto (todos) + Hipertireoidismo
(cintilo) + Bethesda (punção/biópsia). V2 = nódulo só >1cm + sangue.

**Decisões FECHADAS (usuário/negócio, 2026-07-12):**
- **Nódulo/cisto = >=1cm** (critério V2 adotado como V1 efetivo) — CONFIRMADO. Diverge do V1-literal
  "todos", intencional (decisão médica).
- **Linfonodo reacional NÃO conta** (suspeita/real conta). Config alinhado (exclusion + negação).
- **Paratireoide FORA de escopo** (spec Organ=Tireoide) → G-R1 RESOLVIDO: base ouro flipada
  (`OBSMVHSR…475` → 0), deixa de ser FN. Recall v22.2 0,985 → **0,9925** (FN=1).
- **Condicionar por tipo de exame (G-INFRA4) — DEFERIDO** (sem ganho na base atual, adiciona risco).
- **Hipertireoidismo cintilo-only (G-P5) — DEFERIDO** (depende de G-INFRA4).
- **Bethesda (G-R3) — FORA por ora**: 0 FN de Bethesda na base ouro, ganho especulativo, exige o
  condicionamento adiado. Não é o ganho mais próximo nem claramente seguro.

## GAPS DE PRECISÃO (FP restantes ~8 após v22.3)

### G-P1 · Negação DISTANTE "Ausência de … nódulos massas" (~2 FP) — ABERTO, baixo risco
Pós-tireoidectomia: `"Ausência de sinais ecográficos sugestivos de parênquima… nódulos massas"`.
A negação ("Ausência de") está a **>8 tokens** do achado → `negation_window=8` não alcança.
- **Fix (Lever 2):** aumentar janela à ESQUERDA (8→~12). Seguro para nódulo/massa (já são 'left').
- **Risco:** `linfonodo='both'` na janela maior pode sobre-negar → **revalidar linfonodo** na janela 12
  antes de subir. Só fazer com double-check na base ouro.

### G-P2 · Nódulo/cisto <1cm com medida PULADA (met=None) (~2-3 FP) — BLOQUEADO
Nódulo <1cm que promove porque o gate **não mediu** (`source=skipped_*`, `met=None`).
- **Por que bloqueado:** auditoria de medida (2026-07-12) provou que **apertar o gate em "sem medida
  ≥1cm confirmada" derrubaria nódulos ≥1cm REAIS que a medição pulou** (achados: 1,1 / 1,2 / 1,2 cm,
  verdade=1). A leitura mm→cm em si é **confiável** (0 erros de conversão); o problema é COBERTURA.
- **O que destrava:** ver G-INFRA1 (fazer o LLM medir sempre em vez de pular). Só então o Lever 3 é seguro.

### G-P3 · Promoção semântica/LLM sem achado de regra (~1-2 FP) — ABERTO
Ex.: `"INDICAÇÃO: Bócio"` (summary vazio) promovido por embedding; "imagens nodulares" de uma
*observação/recomendação* ("correlacionar com estudo específico").
- **Fix candidato:** subir `similarity_threshold`, ou prompt, ou tratar seção "Observação/Recomendação"
  como não-definitiva. **Risco:** mexer no threshold afeta recall semântico — validar.

### G-P4 · Linfonodo proeminente em pós-operatório (~1 FP) — RESOLVIDO pela spec/reconciliação
TC de controle pós-op com "linfonodos cervicais proeminentes" (reacional ao procedimento).
- **Decisão:** reacional NÃO conta (só suspeita/real). Proeminente pós-op = reacional = **não-relevante**.
  Falta o motor reconhecer o contexto pós-op como reacional (a exclusão pega "reacional" literal; pós-op
  "proeminente" sem a palavra reacional escapa). Candidato: tratar "proeminente" em contexto pós-op como
  reacional, OU exigir qualificador suspeito p/ proeminente. Validar na base ouro.

### G-P5 · Hipertireoidismo GLOBAL (deveria ser só cintilografia) — ABERTO (spec V1)
O finding `hipertireoidismo` promove em QUALQUER exame; a spec restringe a **cintilografia**. Pode gerar
FP (hipertireoidismo citado em US/TC). Depende de **G-INFRA4** (condicionar por tipo de exame).

---

## GAPS DE RECALL (FN = 2 edge)

### G-R1 · Nódulo ≥1cm EXTRA-tireoide (paratireoide) — RESOLVIDO (fora de escopo)
`OBSMVHSR…475`: nódulo 1,0cm em paratireoide; tireoidianos todos <1cm. **Decisão 2026-07-12: FORA**
(spec Organ=Tireoide). Base ouro flipada → 0; motor (fl=0) agora CORRETO (TN, não FN).

### G-R3 · Bethesda em punção/biópsia — NÃO IMPLEMENTADO (spec V1)
A spec (V1) manda contar **Bethesda** em **punção/biópsia**. Não há match de "Bethesda" no config →
um resultado citológico Bethesda numa PAAF pode não ser marcado (FN potencial). Depende de G-INFRA4
(condicionar a punção/biópsia). Adicionar léxico Bethesda (categorias I–VI) escopado ao tipo de exame.

### G-R2 · Exame FORA de escopo (não-tireoide) — HIGIENE DE ENTRADA
`OBSTASYHRP…205`: **US de partes moles do dorso** (nódulo subcutâneo 3,3cm) — nem é exame de tireoide.
Motor acerta em não marcar relevância tireoidiana; revisor marcou 1 (há nódulo real, mas fora de escopo).
- **Fix:** filtrar exames não-tireoide ANTES do motor (higiene do runner), não é correção do motor.

---

## GAPS DE INFRA / LIB

### G-INFRA1 · Cobertura da medição (met=None em ~82 casos) — ABERTO
O gate quantitativo pula a medição em ~1/3 dos casos com nódulo/cisto (`skipped_no_anchor`/
`skipped_not_relevant`). Enquanto pula, o gate <1cm não age (G-P2 depende disto).
- **Fix:** fazer o LLM medir sempre que houver nódulo/cisto (não pular). Mudança de lib + custo de LLM.
- **Ganho:** destrava o Lever 3 (gate <1cm confiável) sem o risco de derrubar ≥1cm.

### G-INFRA2 · Embedding baixa do HF a cada run (~36 min) — DEFERIDO (Nível 2)
`sentence_transformers` baixa o modelo do HF a cada restart do cluster (cache efêmero) → trava.
- **Solução:** Model Serving do modelo multilingual no Databricks + backend HTTP na lib (0.3.16).
- **Estado:** modelo registrado (UC `diamond_ia_hml.nlp_engine.st_paraphrase_multilingual_minilm`);
  endpoint `nlp-embed-multilingual-minilm` READY **mas** query falha (`Unsupported input type: Series`
  — assinatura do modelo logado). **Falta:** re-logar com pyfunc que aceite Series / sem input_example,
  então confirmar o shape do `predictions` e finalizar o parser. Backend 0.3.16 = **esqueleto pronto
  local** (`semantic_expand._evidence_with_serving_endpoint`, opt-in, NÃO commitado). Doc:
  `doc-embedding-model-serving-nivel2-v0.md`.
- **NÃO usar** embedding FM em inglês (gte/bge-en) — corpus é PT.

### G-INFRA4 · Condicionamento por TIPO DE EXAME — ABERTO (spec V1)
A spec condiciona 2 achados ao exame: **hipertireoidismo → cintilografia** (G-P5), **Bethesda →
punção/biópsia** (G-R3). O `column_map` já expõe `tipoexame`/`modalidade` e há a noção
`applies_to_exam_type` (hoje inutilizada, tudo global). Implementar o filtro por tipo de exame
destrava G-P5 e G-R3 de uma vez. Mudança de lib (ler applies_to_exam_type por finding) + config.

### G-INFRA3 · Negação só-à-esquerda distante na lib — DEFERIDO
Relacionado a G-P1. A primitiva já tem `direction` por-achado (0.3.15); falta apenas subir a janela
com revalidação. Sem mudança estrutural nova.

---

## PRINCÍPIO (não violar)
Decisão é **por-achado**, não por-documento: cada achado é medido + checado por negação (esq/dir) +
exclusão, e a agregação é OR (fl=1 se QUALQUER achado relevante sobreviver). Um achado negado
("massas: não caracterizadas") **nunca** suprime um achado relevante co-existente no mesmo laudo.
O único demoter de documento é o `document_vet`, conservador (só quando todos os achados são soft +
frase de normalidade). Correções de precisão devem preservar esse balanço — corrigir **só o que for
seguro** (validado contra a base ouro: derruba exatamente o alvo, 0 TP perdido).
