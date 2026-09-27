# PR 7275 — terceira revisão, 17/09/2026

> Branch `ateromatose_coronariana/feature/migracao-plataforma`, ponta `7901021` (17/09 10:14),
> config `0.2.3-ateromatose_coronariana-modelo-uc`. Parecer local, não postado.

**Veredito: a medição que faltava existe, e é de qualidade alta.** A entrega passou de "editada sem
evidência" para "três versões medidas, uma delas reprovada pelo próprio autor".

---

# 1. Os cinco ajustes anteriores, todos mantidos

| # | item | estado |
|---|---|---|
| 1 | `nlp_engine_version` literal | ✅ `"0.12.3"` |
| 2 | bloco `runtime` removido | ✅ |
| 3 | perfil e widget coerentes | ✅ `use_embeddings: False` **e** `embedding_enable: "false"` |
| 4 | `descriptografia` fora das 3 navegações | ✅ zero ocorrências nos três |
| 5 | e-mail de prd | ✅ negócio no `emailTo`, técnico no `emailCc` |

`pause_status: PAUSED` preservado.

# 2. A sequência 0.2.0 → 0.2.2 é o que uma calibração deve parecer

## 2.1 `0.2.0` — o perfil completo reprovou, e ele mediu por quê

7.500 laudos em dev, janela 15/08 a 13/09, lib `0.12.3`:

- banda `[0.35, 0.65]` chamou o juiz em **6.111 de 7.500 (81,5%) — todos SEM achado da régua**;
- o juiz **aprovou 655**; nos 2.332 pareados com o legado, **197 dos 198 aprovados** são laudos que o
  legado não navega;
- `match_rate` nos mesmos ids: **96,2% em `rule_only` contra 87,8% no perfil completo**;
- F1 contra o legado: 0,331 → 0,139.

🔴 **E ele achou a causa, que não era a banda:** o `specialty_context` mandava navegar
*"importante, grave ou extensa"* — termos que a régua desta linha **não navega** (mortos, na seção de
divergências). O prompt contradizia a régua, e foi isso que virou 654 laudos de 0 para 1.

ℹ️ É a armadilha registrada em memória: **o prompt só FILTRA**. Inclusão de escopo tem de virar
achado de regra, nunca instrução no prompt.

## 2.2 `0.2.1` — banda corrigida, e a semântica apareceu como o problema real

Banda `[0.75, 1.0]` e prompt alinhado à régua. O juiz passou a ser chamado **só nos 408 positivos da
régua e em nenhum outro**, zero `FALLBACK`, zero erro.

🔴 **Mas a precisão contra a médica foi 0,143** — e a causa está medida: **a semântica promoveu 33
dos 44 laudos sem achado para relevante SEM passar pelo juiz**. Com a banda em `[0.75, 1.0]`, o
score da promoção semântica (0,35 a 0,56) **nunca alcança o árbitro**.

**Reprovada pelo próprio autor, e substituída.** Registrar uma versão medida e descartada no
changelog é o que torna a série auditável.

## 2.3 `0.2.2` — semântica desligada, e a decisão está sustentada

Três mudanças coerentes entre si: `use_embeddings = False`, `embedding_enable = "false"` no job, e
banda recalculada para `[0.45, 1.0]` sobre o score sem semântica (positivos 0,508 a 0,678).

**Contra o lote rotulado de 493** (bancada, `0.12.3`, 408 chamadas, zero erro, 311.998 tokens de
entrada e 10.610 de saída):

| | régua | régua + juiz |
|---|---|---|
| precisão contra a médica (44) | 0,556 | **0,625** |
| recall | 0,833 | 0,833 |
| F1 | 0,667 | **0,714** |
| F2 | 0,758 | **0,781** |
| MCC | 0,619 | **0,671** |

E os **109 conservadores derrubados de 300** foram lidos **um a um, por termo**: grau leve ou
esparsa 36, negação antes do termo 30, termo genérico sem sítio 20, só aorta 9 — **nenhum é erro de
prompt**. É a régua falhando onde já se sabia que falha.

✅ **Runner em dev, 500 laudos:** `semantic_score` ausente em **500 de 500** (confirma a camada
desligada de fato), juiz chamado em **34 — exatamente os positivos da régua**, zero `llm_error`.
Decisão idêntica ao `rule_only` de 15/09 nos 178 pareados.

✅ **Custo medido, e a redução é de duas ordens:** `0.2.0` gastou 3.501.806 tokens de entrada em
6.111 chamadas; a `0.2.2` no runner, 29.351 em 34.

# 3. 🔴 Dois achados dele confirmam, por medição independente, defeitos da LIB

Isto é o que esta revisão tem de mais importante, e não é sobre a ateromatose.

## 3.1 Banda com piso abaixo do teto de laudo sem achado

O `0.2.0` mostra o juiz sendo chamado em 81,5% dos laudos, **todos sem achado**, e aprovando 655.
**É o mesmo fenômeno medido em 16/09 na hepatologia** — card `283648` `[P0-29]`, 36 laudos correntes
entregues com `n_positive_spans = 0`.

Duas linhas, dois medidores independentes, mesma causa: o piso da banda abaixo do teto analítico de
um laudo sem achado.

## 3.2 🔴 A promoção semântica não tem árbitro — e nenhuma banda resolve

O `0.2.1` é a prova mais forte disso que existe no projeto. Com `[0.75, 1.0]`, **33 de 44** laudos
foram promovidos pela semântica sem passar pelo juiz.

**E o dilema não tem saída por calibração:**

- banda **larga** → o juiz vê tudo (81,5% dos laudos), o custo é proibitivo e o `match_rate` despenca;
- banda **estreita** → a promoção semântica passa livre, sem árbitro nenhum.

ℹ️ O código marca essa promoção como *"pendente de arbitragem — ver `step_decide_llm`"*, mas a
arbitragem só ocorre dentro da banda. **Fora dela a pendência nunca é resolvida.**

ℹ️ O mesmo fenômeno foi medido hoje no run do `cancer_rim` do João: 2 laudos em 10.000 promovidos
pela semântica com `n_positive_spans = 0` e `llm_called = false`. **A medição do Lucas é muito mais
forte** — 33 de 44 contra 2 de 10.000 — porque a ateromatose tem similaridade alta em todo laudo de
TC de tórax (média 0,72).

🔴 **Consequência para o `283648`:** o card está escrito como *"impedir que o juiz LLM promova sem
evidência de regra"*. O escopo é maior — **a semântica promove sem evidência e sem juiz**, e essa via
não é alcançável pela banda. Os dois precisam do mesmo invariante na lib.

# 4. Sobre desligar a semântica

A régua de entrega diz que o alvo é o fluxo completo e que *"camada que piora o resultado é
refinamento, não veredito"*.

✅ **Ele refinou duas vezes antes de desligar** — banda e prompt na `0.2.1`, banda de novo na `0.2.2`.
O que restou não é problema de calibração: é a limitação estrutural da §3.2. **Desligar é a decisão
certa hoje**, e o que fica pendente é o defeito da lib, não a config.

⚠️ Quando o invariante existir na lib, a semântica volta a ser candidata nesta linha — e o insumo
para decidir já está gravado.

# 5. O que ainda fica aberto

## 5.1 🔴 `embedding_model` do UC num bloco desligado, dependendo de PR não mergeado

A `0.2.3` troca o path do Volume por `mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm`.

- **Inerte hoje** — `use_embeddings: False`, o step retorna antes de olhar o valor. Confirmado.
- 🔴 **Mas quem resolve esse nome é o `ConfigLoader` da branch do João (PR `7321`), que não está
  mergeado.** Se o 7275 entrar antes, a config declara um identificador que o loader da `hml` não
  sabe resolver. Inerte enquanto `use_embeddings` for `False`; armadilha no dia em que alguém ligar.
- 🟡 E `decision_mode: 'hybrid'` com `use_embeddings: False` é **bloco declarado e não consumido** —
  a régua de 03/09 reprova.

**Sugestão:** ou declarar a dependência do 7321 na descrição do PR, ou voltar o `embedding_model`
para o path do Volume até o loader existir na `hml`.

## 5.2 🔴 A branch está 32 commits atrás da `hml`

Mesmo ponto da revisão anterior. Sincronizar antes de pedir aprovação.

## 5.3 🟡 `match_rate` da janela inteira segue pendente

Ele declara isso, e marca a projeção (97,3% → ~98,2%) explicitamente como **ESTIMATIVA**. ✅ Correto
não apresentar estimativa como medição — mas o número que sustenta o merge ainda não existe.

## 5.4 🟡 Duas navegações do legado derrubadas pelo juiz

`ateromatose difusa` sem sítio coronariano: a régua escrita exige coronária, o juiz seguiu a médica.
Ele registra como pergunta clínica aberta. Precisa de leitura com a médica antes de despausar.

---

# 6. Recomendação

**Aprovar depois de dois itens**, os dois pequenos:

1. **Sincronizar com a `hml`** (32 commits).
2. **Resolver o `embedding_model`** — declarar a dependência do PR 7321 na descrição, ou voltar ao
   path do Volume.

O `match_rate` da janela inteira e a leitura clínica das duas navegações ficam como condição para
**despausar o job**, não para o merge — o mesmo critério aplicado nas linhas anteriores.

⚠️ E a precisão de **0,625** contra o gabarito dos 44 continua sendo o número que decide o
despause, não o merge.
