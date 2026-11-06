**Revisão — impacto na captação (DS)**

Ok para merge **como infraestrutura**: os 12 jobs entram `PAUSED` e a N5 (único ponto que escreve na `tb_gold_mov_exame`) está com `modo_simulacao: "true"` no job — nada é gravado na fonte. Condições para **ligar** em PRD:

**1. Destino da N5 em PRD.** Depois do `b011565`, o destino vira `${catalog_gold}.corporativo.tb_gold_mov_exame` = `gold_fabrica_ia.corporativo.tb_gold_mov_exame`, que **não existe** (`TABLE_OR_VIEW_NOT_FOUND`; a única visível é `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, EXTERNAL). Se a intenção é gravar na `gold_corporativo_ia`: é a tabela da carga da engenharia e a **fonte das 13 linhas de captação** — precisa de acordo com o dono da carga (a próxima carga pode sobrescrever o que o OCR gravou) antes de desligar a simulação.

**2. Campo.** A N5 grava `laudo_transformado`; o runner da plataforma lê `laudo_original` (`fabrica-ia-nlp-platform`, `plataform/pipeline_e2e/nlp_ia_02_input.py:511`, na `main`). Ligado como está, o texto do OCR **não chega** às linhas de captação. Alinhar com a plataforma: o runner passa a usar `coalesce(laudo_original, laudo_transformado)` ou o OCR grava no campo que o runner lê.

**3. Validação em hml.** O CD foi validado contra `gold_fabrica_ia_hml.legado.tb_gold_mov_exame` (cópia gerenciada, sem dado nos últimos 3 dias), não contra a fonte corporativa. Vale um run em simulação apontando para a fonte real e conferir a trilha (`exame_sem_laudo_atualizacao`) antes de gravar.

**4. `run_as` de PRD.** `jobs/ambientes/prd.json` está com `"run_as": "PREENCHER_SP_PRD"` — o próprio comentário do arquivo avisa que o CI fica verde e a falha aparece no upsert da Jobs API.

**5. Escopo do PR.** O título fala do OCR, mas a promoção leva a `hml` inteira (706 arquivos: FlowHub bronze/silver/gold, repositório clínico, navegação, diamond, qualidade). Sugiro ajustar título/descrição para quem aprovar saber o que vai para a `main`.

Cobertura (informativo, não bloqueia): o OCR cobre a família "acesse o viewer"; os ~1,5 mil "laudo em pdf"/mês das unidades TASY seguem fora (sem AN de 16 caracteres).
