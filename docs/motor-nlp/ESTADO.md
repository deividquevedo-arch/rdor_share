# Estado das frentes

> **Documento vivo.** É a única fonte de "onde cada coisa está agora". Carregado em toda sessão
> via `@` no `CLAUDE.md`, então sobrevive à compactação.
>
> **Regra:** aqui vai **estado** (o que está feito, o que falta, de quem depende). Fato durável e
> lição aprendida vão para a memória (`/memory`). Se uma linha aqui não muda há meses, ela é fato —
> mova para lá. Se uma memória tem data e "estado atual", ela é estado — mova para cá.
>
> Atualizado em **2026-09-03**.

---

## nlp_engine (lib) — `nlp-engine-lib`

**`0.11.1` na `hml`**, tags `v0.9.0`–`v0.11.1` publicadas. Gate: ruff, format, mypy, **628 testes**,
`release-check`.

- `0.10.0` **observabilidade do LLM e empacotamento** — `llm_router_mode` distingue os 3 motivos ·
  `llm_input_chars` · `llm_api_key_origin` · `py.typed` · `setup.py` removido. **Fecha os 4 P1.**
- `0.10.1` **legenda ordinal descendente** deixa de promover. Defeito 1 do card `285305`.
  Critério passou a aceitar corrida monotônica de ±1 que **toque** a categoria mínima.
  Medido: 53 de 314 entregas removidas (16,9%) em run de dev de 20.648 laudos.
- `0.11.0` **falha de infraestrutura não vira decisão clínica** — `require_measure` não rebaixa
  quando a medida faltou por erro de LLM · promoção semântica sem arbitragem do juiz não entrega ·
  a queda para `token_overlap` deixa de ser silenciosa.
- `0.11.1` **a camada semântica deixa de promover trecho negado.** A régua negava e a semântica
  promovia o mesmo texto. Reusa `is_negated_in_sentence_plain`; a âncora que faltava é construída
  com `tokenize_sentence_norm`. Direção default **`left`**, não `both`. Card `298597`.

🔴 **`main` está DUAS versões atrás** — `0.10.1`. A `0.11.0` e a `0.11.1` só estão na `hml`.

🟡 **Validação da `0.11.1` em curso.** Coorte de 4.508 laudos de hepatologia (18/08) contra o run de
referência de 01/09. Parcial em 2.495: **443 → 439 relevantes, 4 perdidos, 0 ganhos**, 77 com score
semântico reduzido, 43 mudaram de `decision_source`. Zero score subiu — a mudança só remove.
⚠️ A nota de release afirma impacto zero e **isso não se sustenta**; será corrigida com o número
final.

🟡 **`0.12.0` EM CURSO** — branch `feat/0.12.0-higiene-consolidada`, 5 commits, **8 dos 15 cards**:
`P2-07` extra vazio · `P2-09` contrato vinculante · `P2-10` hierarquia de exceções · `P2-11` fim do
engolir silencioso · `P2-12` ruff `B`/`BLE`/`C901` e mypy `strict` · `P2-17` SQL parametrizado ·
`P2-19` `__all__` · `P2-22` cobertura com gate.
Gates: ruff, format, mypy strict, **745 testes**, cobertura **86,66%** por ramo, release-check.
Faltam `P2-08`, `P2-20`, `P2-21`, `P2-23`, `P2-24`, `P3-26`, `P3-28`.

- ✅ **A lib passou a emitir log.** Não emitia em lugar nenhum — `NullHandler` no pacote e `logger`
  em `to_plain` e `html_plain`. Dos 13 `except Exception`, 4 estreitados, 7 passam a registrar a
  classe, 1 segue amplo de propósito e documentado.
- ✅ **O contrato de saída deixou de ser decorativo.** `contracts.py` estava em **0%** porque
  ninguém o importava; o motor emitia `findings`, `findings_match`, `findings_spans` e
  `decision_trail` sem declaração. Agora `process()` devolve `EngineOutputRow` e um teste compara
  emitido contra declarado nos dois sentidos.

🟡 **Plano de bumps registrado** — card `298598` e `docs/plano-acao-backlog-lib-2026-09.md`.
`0.12.0` higiene (15 cards, nenhum toca `fl_relevante`, um golden prova o lote) → `0.13.0` estrutura
(6 cards, `process()` em streaming **quebra a API**) → `0.14.0` juiz sem evidência → `0.15.0` vínculo
lesão↔medida. SPECs da `0.12.0` e `0.13.0` escritas.

🔴 **P0-29 segue aberto** — o juiz pode promover sem evidência de regra. Card `283648`, alocado na
`0.14.0`. Exige medição prévia por linha.

⚠️ **Branch `docs/plano-e-specs-ops` sem push** — plano e SPECs.

## Tireoide V2 — ✅ ENTREGUE EM HML

**PR aprovado e mergeado** (PR 7071, `005735e`). Config **`0.8.0-tirads`** na `hml`, exige
`nlp_engine >= 0.9.3`. Branch de origem: `tirads/feature/v2-sem-sangue`.

- **Escopo entregue:** só TR5 e TR4 ≥1 cm (`ordinal_only`), sangue **fora da captação e da
  promoção**, juiz LLM desligado, `findings` no formato `TR4 - Nódulo (1,8 cm)`.
- **Impacto medido:** 8,03% → 2,90% de apontamentos no mesmo dia (~64% menos). Volumetria de
  ordem de grandeza: ~94 laudos e ~62 pacientes/dia. ⚠️ **medida sobre UM dia** — se o Natan
  precisar de número firme, rodar a janela 17/06–11/07.
- ✅ **A dedup da VIEW foi corrigida** (PR 7075): ela reprojetava o histórico inteiro a cada
  `CREATE OR REPLACE` e reenviava laudo já entregue. Agora filtra por `dt_execucao_modelo`.
  Isso reduz muito a necessidade de filtrar `config_version` na view do João — sobra só o dia
  da transição, se o job noturno rodar a `0.1.0` e a `0.8.0` no mesmo dia.
- ✅ **O job noturno passou para a `0.8.0-tirads`.** Verificado: produção em 21/08 com
  `0.8.0-tirads` + engine `0.9.4` — 31.418 laudos, 997 relevantes.
- 🔴 **Pipeline quebrou na etapa `nlp_config` (20/08, João):** `nlp.findings values must be
  list[str]`. **Config e wheel já DESCARTADAS como causa** — a `0.8.0-tirads` do `origin/hml`
  passa validada contra o código de dentro da wheel `latest` (0.9.4) baixada do Volume. Espera o
  **log da etapa `install`**, que mostra a versão efetivamente instalada.
  ⚠️ **NÃO achatar o `findings` para `list[str]`** — destruiria `regex`, `exclude`, `unless` e
  `skip_organ_gate`, e o pipeline voltaria a rodar **errando em silêncio**.
- 🔴 **Aguardando Carol:** planilha de 500 laudos, desde 13/08.
- 🔴 **CORRIGIDO O ENTENDIMENTO (27/08): sair só com a categoria NÃO é questão de exibição.**
  Os **4 casos que saem como `TR5` puro são 4/4 falso positivo** — o TR veio da **legenda do ACR**
  no rodapé, não de achado. Os `TR4` puros são legítimos. A nota anterior ("melhoria de qualidade,
  não de recall") estava errada.
- 🟡 **Card `285305`** (Defect, P1) — dois defeitos consolidados, ambos na lib.
  ✅ **Defeito 1 CORRIGIDO** na `0.10.1` (tag publicada, na `main`). Produção rodou `0.10.0` em
  02/09 por ser a versão publicada no momento da execução; o próximo noturno pega a `0.10.1`.
  ✅ **Defeito 1 MEDIDO em 03/09, por A/B real** — coorte de 2.177 laudos (02–03/07), dois motores
  na mesma execução: **156 → 133 entregas, 23 removidas (14,7%), zero acrescentadas**. Causalidade
  provada: **23 de 23 mencionam ACR e PAAF**, nenhum removido sem legenda. Convergiu com os 16,9%
  da medição simulada de 01/09. Produção pegou a `0.10.1` em 03/09.
  ⚠️ A medição foi sobre 2 dias, não sobre 17/06–11/07: a baseline daquela janela existia em dev e
  **se perdeu quando as tabelas do TI-RADS foram dropadas em 03/09 às 12:36** e recriadas.
  Recuperável por `UNDROP` (7 dias), pendente de decisão com o João.
  🔴 Falta o **defeito 2** (medida associada ao nódulo errado) — é a `0.15.0`.
  Detalhe original dos dois:
  **(1) legenda ACR** — `_legend_exclude_ids` só reconhece corrida **ascendente por +1 começando no
  rank 0**; a legenda desse emissor é `TR5→TR1`, descendente. 5 categorias, passa o `min_run=4`, e
  escapa. Medido: **6 de 18 pacientes (33%)** no arquivo RJ de 27/08. Provado ponta a ponta com a
  config de produção.
  **(2) medida do nódulo errado** — o critério pede o **MAIOR nódulo do laudo**, não o que é TR4.
  `gate_mets` é por critério, não por menção: não existe vínculo lesão↔medida. Entregamos
  `TR4 - Nódulo (2.1 cm)` num laudo cujo TI-RADS 4 media 0,4 cm. **2 dos 7 TR4** do arquivo.
- ⚠️ **Antes de corrigir, rodar a janela 17/06–11/07** para dimensionar. Fazer **depois da `0.10.0`**,
  para o antes/depois ter uma variável só.
- ℹ️ **O arquivo que o Natan monitora vem de HML, não de produção** — conferido exame a exame.
- ✅ Backup das 3 variantes de escopo em `_versoes-estaveis/` + matriz no cabeçalho da config.
- ℹ️ **V3 (sangue) segue especificada e guardada** na branch `tirads/feature/v3-sangue`.
  Reativar = duas chaves + devolver as palavras-chave de captação.

## Transplante de pulmão

V1 **entregue** (2026-08-06, card 246669). V2 **especificado e parado**.

🔴 **Em produção a linha entrega ZERO** (26/08: 2.699 laudos, 0 relevantes). A config tem
`findings: {}` — não existe caminho léxico, toda relevância vem de `on_met: promote` nos critérios
quantitativos, que dependem do LLM. Com o 403, nada promove e o run fecha em sucesso.
🔴 **`dt_agendada` VAZIO** — não está no agendamento noturno; a única execução em prd foi manual.

🔴 **V2 NÃO autorizada — em backlog até liberação do Natan.** Não trabalhar nela sem esse aval.

- 🟡 Quando for liberada: o bloqueio "promoção sempre vence o gate" foi **parcialmente resolvido**
  na `0.8.5` (`gates_ordinal_promotion`), mas ali a blindagem levantada é a **ordinal**. Falta
  avaliar se o pulmão precisa também de `on_met: demote` para expressar contraindicação — hoje um
  paciente com VEF1 < 30% **e** FEVE < 40% seria encaminhado sendo contraindicado.

## Hepatologia

🔴 **Única especialidade em `segmentation.mode: auto`** — provavelmente descarta IMPRESSÃO/CONCLUSÃO
de todo laudo. **Nunca medido.** Mensurável desde a 0.8.3 (`segmentation_coverage`).
No ca-rim, a mesma correção recuperou **+25 laudos em 6 dias**.

## Câncer de estômago — 🔴 PR ABERTO EM HML, AGUARDA REVOTE

Branch `cancer_estomago/feature/migracao-plataforma`, commit `e371036`, pushado.
Exige `nlp_engine >= 0.9.4`. SPEC: `cancer_estomago/spec-negocio-cancer-estomago-v1.md`.

**Medido em 10.783 laudos / 61 dias (01/05–30/06):** recall **0,600** e precisão **1,000** no lote
de 37 do negócio · **75 relevantes = 1,2/dia** · **zero** entregue sem achado (eram 36) · juiz
chamado 345 vezes (eram 3.199) · run de 45 min (eram 169).

- ✅ **Cascata regra → expansão → juiz** implementada pela banda `[0.60, 0.97]`. O teto de score de
  laudo sem achado é **analítico** (0,597; medido 0,5876 idêntico em dois runs), então o corte é
  garantia, não estimativa. ⚠️ Revalidar se mudarem pesos, política de score ou régua.
- ⚠️ **O recall de 0,600 é o TETO contra esse gabarito, não limitação da régua.** A anotação do
  Targa é **anterior** à decisão sobre úlceras: cinco dos 15 relevantes dele ficam fora por decisão
  posterior, e um era promoção do juiz sem evidência.
- 🔴 **Aguardando Carol** — 133 laudos (75 entregues + 58 recusados com achado).
- 🔴 **Aguardando Targa** — 120 casos para homologar, mais duas questões: MALT em seguimento conta
  como progressão? achado maligno fora do estômago entra?
- 🔴 **Schema `cancer_estomago` NÃO existe em hml** — só em dev. É do time da Fábrica e é o
  **caminho crítico**: não depende de nenhuma validação.
- Arquivos gerados em `Desktop/Rede D'Or/` (fora do git, têm texto de laudo).


### PR 7102 — estado em 21/08

Branch `cancer_estomago/config-0.6.1-hml`, **config `0.6.2`**, commits `598fb04` → `fe4cd55` →
`08f9c7b`. **Um arquivo, adição pura.**

- 🔴 **Diego votou `Rejected`** e só ele altera. A revisão dele referenciava a `0.6.1`; a `0.6.2`
  e a `08f9c7b` fecharam **todos** os itens de código.
- Único aberto: **compliance/DPO**, que vale para as três especialidades.
- ⚠️ Nome da branch diz `0.6.1` e o conteúdo é `0.6.2` — não renomear, quebra o PR.
- Card do PR: `283567` (**sem dono atribuído**).

## Câncer de rim — Leandro

✅ **Ativou o juiz LLM e subiu para >91%** (2026-08-18). Já tinha resolvido a segmentação com
`full_doc`; os 3 avisos dele viraram 0.8.3/0.8.4, e o da versão do motor se resolve com a migração
(branch `cancer_rim/feature/migracao-config-motor`).

- ❓ **Confirmar qual métrica subiu.** O juiz estava desligado porque **derrubava 3 a 8 pacientes
  confirmados** — precisão já era ~92% com ele. Se os >91% forem precisão, a pergunta que decide é
  se o **recall** se manteve em 1,000. Em rastreio, precisão comprada com paciente perdido é
  regressão, não ganho.
- O diagnóstico escrito para ele (`docs/motor-nlp/cancer_rim/diagnostico-config-cancer-rim.md`)
  ainda não foi enviado; se ele alinhou o prompt à v0.5 por conta própria, parte dele já venceu.

## Câncer de cólon — 🟡 LEVANTAMENTO PARA MIGRAÇÃO

Roda no **legado**: `fabrica-ia-plataforma/apps/databricks/colon/`, no **workspace antigo**, sobre
**`hive_metastore.ia`** — fora do Unity Catalog. **Sem juiz LLM.** Agenda convocada pelo Natan.

- ✅ A régua legada já tem um `CONFIG` **no mesmo formato que o `nlp_engine` espera** (`negation`,
  `organs.<x>.seeds/regex`, `findings`, `semantic`). Migração é **tradução de config**, não
  reescrita — e a régua real do cólon é pequena: ~53 termos, 44 seeds, 14 regex, mais o bloco DII.
- 🔴 **Não existe baseline.** O notebook de monitoramento não calcula nenhuma métrica de acerto.
- 🔴 **Gabarito desconhecido** — existe `dev_tb_diamond_mod_colon_saida_conferencia`; confirmar se é
  gabarito clínico ou fila operacional.
- 🔴 **Vocabulário estrangeiro embutido:** a régua geral carrega **reumatologia** (97 seeds) e a de
  colonoscopia carrega **hepatologia** (39 achados, LI-RADS). Remover muda resultado — medir.
- 🟡 **Duas réguas divergentes** (geral × colonoscopia): `polipo` (18 termos) só existe na geral.
- Schema `cancer_colon` **já existe em dev**. Convenção confirmada: linhas oncológicas levam o
  prefixo `cancer_`.
- ⚠️ Volumetria do legado **não medida** — o perfil do workspace antigo está expirado.

## Contexto do paciente — card 280008

Estudo e desenho **concluídos** (doc macro, drawio, nota de review, 2 comentários no card).
🔴 **Pendente: SPEC da fase 0** — exclusão/refutação no escopo do laudo. É o critério que fecha o card,
e não depende de nenhuma das 6 decisões em aberto.

## Revisão de config das especialidades — régua de 2026-09-03

🔴 **Config não passa com bloco morto.** Régua fixada, valendo para qualquer PR de especialidade.
Referência: `cancer_rim`. Regra canônica em `.claude/rules/motor-nlp.md`.

O runner lê `specialty_id`, `config_version`, `model_version`, `data`, `nlp` e **`runtime.llm_router`**.
Não lê `catalog`, `monitoring`, `distribution`, nem interpola `{catalog}`/`{run_id}`.

⚠️ **A SPEC 27 §6.1 está ERRADA** ao afirmar que nada lê `runtime` — `ntb_ia_loader.py:105-113`
faz `self.llm_router.update(runtime_llm)`, e o `runtime` **sobrescreve** o `nlp`. Foi o que
derrubou a primeira subida do ca-estômago. O guia `boas-praticas/02` Passo 6, em compensação, já
pede a coerência entre os dois blocos.

### PR `7159` — ca-rim (Leandro)

Config valida no `load()` da lib: juiz **ligado** (`enabled: True` explícito), 27 termos semânticos,
negação com 51 frases e `direction: left`, threshold `0.92`.
✅ Atendeu dois pedidos: comentário de calibragem e `embedding_model` no Volume do ambiente novo.
🟡 Falta remover `catalog`, `monitoring` e `distribution`.
ℹ️ O comentário dele documenta que `ambiguity_band` é **inerte** em `decision_mode: hybrid` —
confirmado em `decision_pipeline.py:555`, único uso.

### PR `7191` — ca-cólon (Lucas)

Config valida no `load()`: 13 findings, 148 termos, negação com 64 frases e direção por achado.
**Zero PHI** nos notebooks e no RUNBOOK — conferido.
🟡 Pedidos: `nlp.llm_router.enabled: False` explícito (o juiz está desligado e a config parece
ligada — prompt completo declarado); remover blocos mortos; cabeçalho (é o único dos seis configs
sem nenhum) e changelog no arquivo, incluindo o `match_rate` de paridade, que se perde com a
remoção dos notebooks.
ℹ️ **Sem LLM neste ciclo, por paridade com o legado.** O juiz entra no próximo.
ℹ️ Os dois `assert` do backtest dele — wheel instalada contra o pin e `config_version` carregada
contra a esperada — são o padrão que vale copiar.

## Plataforma / MLOps

🔴 **O LLM nunca funcionou em produção nesta plataforma** (medido 27/08). **8.058 tentativas, zero
sucessos**, nas 4 linhas, em toda a história da tabela (21/08 a 26/08). Erro:
`403 — Invalid access to Org: 7405607882166874`. Em HML, 100% de sucesso na mesma janela.
**Causa:** produção roda em `adb-7405605001346204` e o `base_url` estava fixo no outro workspace —
o token do contexto do notebook é sempre do workspace onde o job roda. João corrigiu no PR 7135
(URL por ambiente). ⚠️ **Ainda SEM TESTE**: o run de 27/08 às 04:00 falhou antes de processar.

🔴 **Noturno falhando por lote vazio** (27/08): `nlp_config`/`input`/`persisters` com sucesso e
`process` com `ValueError: Nenhum laudo recebido`, nas 3 linhas agendadas, nas duas tentativas.
Casa com a dedup por `id_exame` sem `config_version`. Comunicado ao João.

🔴 **A tabela de monitoramento não tem NENHUMA coluna de LLM** — só total, relevantes, taxa e
confiança. O `alert_threshold_relevance_drop` não pega falha de LLM: no TI-RADS a taxa ficou
3,17% → 3,21% enquanto 4.703 chamadas falhavam, porque a régua sustenta o número.

- ⚠️ **Dedup da entrada usa `id_exame` puro**, sem `config_version` — janela processada uma vez fica
  bloqueada. Falha em silêncio (lote vazio com sucesso). ✅ Resolvido na prática pelo widget
  **`reprocess_enable`** (só dev) — o contorno de `model_version` de bancada está obsoleto.
- `persist_input` é **código morto** — declarado, lido, nunca consumido.
- ✅ Schemas provisionados: `cancer_estomago`, `cancer_colon`, `cancer_rim`, `tumor_osseo` em **hml**
  (21/08). Produção segue com hepatologia, tirads e transplante_pulmao.

### Runs de bancada em dev estão caros — card `298596` (02/09)

🔴 **`limit_rows` não isola coorte.** O teto é aplicado **depois** da união da fila, cuja ordem é
inéditos → pendentes → **reprocessados por último**. `df_queue.limit(N)` pega as N primeiras, então
a coorte a remedir fica sempre fora do teto. O run fecha **com sucesso sem tocar a coorte**.

- Medido em 02/09, hepatologia: fila de **110.777** para medir 6.398; depois de limpar pendentes,
  fila de **62.266** com 55.868 inéditos. Os 1.000 primeiros gravados tiveram **zero id em comum**
  com a coorte.
- ✅ **Contorno:** janela de **um dia integralmente processado** zera os inéditos. Com 18/08,
  `gold=4508 | ineditos=0 | fila=4508` — **14× menos LLM**. ⚠️ Depende de coincidência: nos outros
  dias da mesma coorte sobrariam 1.203 e 2.765 inéditos.
- ✅ **Backlog de 104.379 pendentes em dev na hepatologia LIMPO** (02/09). Era resíduo de run
  interrompido; as outras três especialidades tinham **zero**.
- `include_pending` tem default `True` e **não tem widget** — não dá para desligar pela UI.

## Alinhamento com a plataforma — 2026-08-21

Duas agendas (42min + 2h24) depois da queda do TI-RADS em produção. **Mapa completo em**
[`_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md`](_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md)
— 15 gaps com dono, prioridade e solução.

🟡 **A instalação da lib passou de wheel para `pip`** (PR 7194, João, 03/09), com feed privado.
`latest` ou vazio instala sem pin; versão específica vira `nlp-engine==<versão>`. ⚠️ Muda a
conversa sobre fixar versão por especialidade — o mecanismo agora existe.

**Decisões que MUDARAM e valem a partir de agora:**

- 🔴 **Produção deixa de usar `latest`.** Cada especialidade declara qual versão da lib usa; `latest`
  fica para o time de DS.
- 🔴 **Fluxo de branch:** PR para `hml` publica a wheel no volume de **dev** (validação nossa);
  PR para **`main`**, com code review de Diego/João/Gabriel, publica em **hml e prd**.
- 🔴 **Todo PR na lib passa por code review**, com matriz de impacto na descrição.

**Cards criados em 21/08:** `283644` (juiz por contorno, em execução) · `283645` (fluxo, Diego) ·
`283646` (DPO, em execução) · `283647` (contrato) · `283648` (`[P0-29]` juiz sem evidência).
`282904` (o incidente) **encerrado**.

**P0/P1 sem card, e são nossos:** doc do consumidor + mensagem de erro · vazamento de memória no
`process()` · lib não emite log no caminho NLP.

## Dívidas transversais

- ✅ **`main` sincronizada com a `hml`** (21/08, ambas na `0.9.4`). Estava 167 commits atrás e
  **causou a queda do TI-RADS em produção** — a esteira publica prd a partir da `main`. Débito que
  estava catalogado como "decisão de repositório" e era, na verdade, risco de produção.
- 🔴 **9 CSVs com texto de laudo** nos commits locais da raiz — bloqueia push de `docs/`, que
  portanto está **sem backup**.
- 🔴 **A base ouro não tem lugar oficial.** Gabarito vive em planilha, e-mail e arquivo
  temporário — sem `spec_version`, sem `dt_anotacao`, sem dono. Custou uma conclusão errada em
  20/08. A sandbox do Datahub (Diego) **não cobre** isso: nosso caso é o inverso, artefato que já
  nasce oficial.
  ✅ **Destino decidido em 02/09: o LAKE, não o repositório.** Alinhamento com o Diego em curso
  sobre schema e formato, com reuso posterior e treinamento de modelo proprietário no horizonte.
  Enquanto isso os harnesses saíram do diretório temporário do job — que é apagado junto com ele —
  para `Desktop/Rede D'Or/_ferramentas/`, fora do git.
- 🟡 **`embedding_model` das configs aponta para `diamond_ia_hml`**, o Volume ANTIGO — **caminho
  literal, idêntico nos três ambientes**, então produção lê o modelo de um volume de HOMOLOGAÇÃO do
  workspace antigo. Hoje responde e o modelo está íntegro; o risco é latente. Se aquele Volume sair
  do ar, as linhas com embeddings caem para `token_overlap` — a `0.11.0` registra a queda na trilha,
  mas o resultado muda sem alarme na monitoria.
  ✅ **MiniLM já copiado** para `gold_fabrica_ia_hml/nlp_engine/nlp_engine_lib/st_models/` (01/09).
  🔴 **Produção não tem destino:** o catálogo `gold_fabrica_ia` não possui o schema `nlp_engine`.
  Criar schema é do time da Fábrica. Card **`298600`**, com o ponto em aberto: como resolver o
  caminho **por ambiente** — mesma classe do `base_url` do LLM, resolvido no PR 7135.
  ℹ️ O `mpnet-base-v2` fica **fora de escopo**: só existe no volume antigo e serve notebooks legados.
- `git-steward` ainda é 78 linhas sempre carregadas; candidato a virar regra curta + skill.
