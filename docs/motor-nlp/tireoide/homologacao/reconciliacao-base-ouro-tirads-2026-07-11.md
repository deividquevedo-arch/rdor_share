# Reconciliação da base ouro — TI-RADS (v21.9)

**Data:** 2026-07-11
**Para:** revisão médica
**Arquivo de trabalho:** `reconciliacao-base-ouro-tirads-2026-07-11.csv` (abrir no Excel — acentos OK)

## O que é isto

Rodamos o motor v21.9 contra a **base ouro** (586 laudos com verdade confirmada). O motor concordou com a base em **544** casos. Restam **42 desacordos**, listados no CSV. Precisamos do seu veredito para fechar a base ouro — assim não revalidamos os 893 laudos manualmente de novo.

**Como preencher (só duas colunas):**
- `verdade_medica` → `1` (relevante p/ linha de cuidado) ou `0` (não relevante)
- `observacao_medica` → comentário curto (opcional)

As demais colunas são contexto: `laudo` (texto limpo), `motor_achou_resumo`, `ti_rads_motor`, `criterio_quantitativo` (o que o motor mediu), `rotulo_atual_base` (rótulo hoje) e `fonte_base`.

---

## PRIORIDADE ALTA — 5 casos (rótulo em dúvida)

Aqui a **base diz relevante (1)** e o **motor discordou (0)**. São os que mais importam: definem se são erros do motor ou rótulos velhos.

| # | id (curto) | base (fonte) | motor viu | pergunta clínica |
|---|---|---|---|---|
| 1 | OBSMVHSR…005725 | 1 (concordancia_FN) | nódulos TR3, **maiores <1cm** (0,6–0,7cm) | Nódulos TI-RADS 3 **< 1 cm** contam como relevantes? |
| 2 | OBSTASYHSL…049201 | 1 (medico) | nódulo istmo **0,8cm**, TR3 | Nódulo **0,8 cm** (istmo) TR3 é relevante? |
| 3 | OBSTASYHSLC…07315 | 1 (concordancia_FN) | textura **difusamente heterogênea** (tireoidopatia), sem nódulo | Padrão **difuso/tireoidopatia** sem nódulo é relevante? |
| 4 | OBSWPDHQD…43152 | 1 (concordancia_FN) | hipotireoidismo, contorno irregular, **sem nódulo** | Alteração **difusa** sem nódulo/medida é relevante? |
| 5 | OBSTASYHRP…40664 | 1 (medico_manual) | **US de partes moles do dorso** (nódulo subcutâneo 3,3cm) — **não é exame de tireoide** | Exame **fora de escopo** (não-tireoide) deveria estar na base? |

**Contexto para 1–4:** a regra que você definiu — *nódulo/cisto contam só se ≥ 1 cm* e *padrão difuso/tireoidopatia fora* — faz o motor rebaixar esses casos. Se confirmarmos essa política, os rótulos 1→0 aqui **não são erro do motor**, e o recall sobe de 0,961 para ~0,984.
**Caso 5** é um exame que nem é de tireoide (US de partes moles) — provavelmente entrou por engano; sugerimos tirar da base / marcar fora de escopo.

---

## PRIORIDADE OPCIONAL — 37 casos (confirmação de política)

Aqui a **base já diz não-relevante (0)** e o **motor marcou 1** (falso-positivo do motor). A base já está correta — não precisa reconciliar; é só validar a política para nós corrigirmos o motor.

- **33 são linfonodo** (reacional / habitual / proeminente / hilo preservado). É a maior alavanca de precisão que resta. Se você confirmar "linfonodo benigno **não** é relevante", implementamos a exclusão e removemos ~33 falsos-positivos.
- **4 são outros** falsos-positivos — dar uma olhada rápida se algum na verdade deveria ser 1.

Basta uma marcação em bloco (ex.: "todos os linfonodos benignos = 0, exceto se suspeito/necrose/atipia") — não precisa caso a caso.

---

## RESULTADO DA REVISÃO (2026-07-11, revisor: usuário/DS) — 42 casos fechados

Vereditos gravados nas colunas `verdade_medica`/`observacao_medica` do CSV. Base ouro atualizada em `base-ouro-tirads-2026-07-11.csv` (preserva a de 07-10).

**Matriz v21.9 (n=586, base TOTALMENTE reconciliada = 45 rótulos):** TP=134 · FP=26 · FN=2 · TN=424 → **precisão 0,838 · recall 0,985 · acc 0,952.** (Além dos 42 disagreements, +3 rótulos stale de bócio difuso — `…927012`, `…786079`, `…743308` — que eram AGREEMENTS v21.9 motor=base=1, corrigidos p/ 0 pela regra de bócio; revisor confirmou lendo os laudos completos.)

**Projeção v22.0 (config bócio):** remove os 13 FP "dimensões aumentadas" (0 TP com achado real perdido, validado na camada de regra) → **FP 26→13, precisão ~0,91, recall 0,985**. Número final de produção vem do re-run E2E llm_http.

**Descoberta que inverte a leitura anterior:** dos 37 "FP", **13 eram TP** — o motor estava certo e a **base ouro subestimava linfonodomegalias REAIS** (TC pescoço: linfoma/Lugano, PAAF de linfonodo atípico, linfonodomegalia cervical medida, nódulo de parótida). **Excluir linfonodo em bloco destruiria esses 13 TP** — o lever NÃO é "excluir linfonodo benigno".

**24 FP confirmados, por causa-raiz (esses são erro real do motor):**
1. **bócio "dimensões aumentadas" (10)** — glândula difusamente aumentada / inespecífico / tireoidopatia difusa marcada como relevante; deveria ser não-relevante (padrão difuso). Refina a regra anterior ("dimensões aumentadas = bócio"): só **bócio nodular / específico** é relevante, não aumento difuso inespecífico.
2. **nódulo/cisto <1cm, negado ou da indicação (7)** — <1cm vazando quando há co-achado (gate coordenado não rebaixa), achado casado na LINHA DE INDICAÇÃO ("Nódulos?"), "Ausência de formações nodulares".
3. **linfonodo NEGADO / pós-op (7)** — "Linfonodomegalias cervicais: **Não há**" (negação à DIREITA do termo; `negation_direction='left'` não pega), pós-tireoidectomia "não suspeitos".

**2 FN residuais:** (a) paratireoide 1,0cm extra-tireoide (escopo); (b) US partes moles não-tireoide (higiene de entrada).

**Inconsistência `…818925` RESOLVIDA:** revisor confirmou **relevante** (nódulo 17x18x21mm = 2,1cm ≥1cm TR3) → TP.

**Regra de bócio (definida pelo usuário 2026-07-11):** se o laudo contém aumento da glândula MAS o texto indica que é apenas inflamatório/sem foco cirúrgico → descarta. **Termos bloqueados se vierem SOZINHOS:** "dimensões aumentadas", "volume aumentado", "inespecífico", "tireoidopatia difusa", "tireoidopatia parenquimatosa", "bócio difuso homogêneo". Justificativa: tratamento 100% medicamentoso com endocrinologista. Só **bócio nodular/específico** promove.

## Levers derivados (ordem por impacto)
1. **Negação à direita p/ achados enumerados** ("X cervicais: Não há", "Ausência de X") — resolve ~9 FP (linfonodo/nódulo/massa negados). Lever estrutural (config/lib).
2. **Bócio: distinguir difuso-inespecífico de bócio relevante** (~10 FP) — decisão clínica pendente do usuário.
3. **Excluir linha de INDICAÇÃO da extração de achados** (~2-3 FP).
4. **Gate coordenado: rebaixar nódulo/cisto <1cm mesmo com co-achado** (~poucos).
5. Escopo (higiene de entrada) + paratireoide (política) = 2 FN, baixa frequência.
