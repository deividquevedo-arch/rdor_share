# Estado das frentes

> **Documento vivo.** É a única fonte de "onde cada coisa está agora". Carregado em toda sessão
> via `@` no `CLAUDE.md`, então sobrevive à compactação.
>
> **Regra:** aqui vai **estado** (o que está feito, o que falta, de quem depende). Fato durável e
> lição aprendida vão para a memória (`/memory`). Se uma linha aqui não muda há meses, ela é fato —
> mova para lá. Se uma memória tem data e "estado atual", ela é estado — mova para cá.
>
> Atualizado em **2026-08-27**.

---

## nlp_engine (lib) — `nlp-engine-lib`

**0.9.4 na `hml`**, tags `v0.9.0`–`v0.9.4` publicadas, wheels no Volume. Gate: ruff, format, mypy,
**484 testes**, `release-check`.

- `0.9.0` **`gates_ordinal_promotion`** (critério de gate declara QUAIS categorias condiciona) +
  **nomenclatura ordinal completa** (rename limpo) + **blob = núcleo mínimo + trilha sempre**.
- `0.9.1` laudo de uma linha deixou de ser apagado — ver [[laudo-de-uma-linha-e-apagado]].
- `0.9.2` `findings_policy.display` era engolido pelo carregador (a config declarava e sumia).
- `0.9.3` categoria ordinal sai **uma vez**, no achado que ela qualifica.
- `0.9.4` laudo que é SÓ aviso volta a sair vazio — a `0.9.1` deixava resíduo e quebrava o
  sinal `texto tratado vazio`, que vale para 41% do corpus do ca-estômago.

🟡 **`0.10.0` pronta e NÃO pushada** — branch `feat/0.10.0-observabilidade-e-empacotamento`, 8
commits, árvore limpa. Gates verdes: ruff, format, mypy, **593 testes**, `release-check` completo.
Escopo: `llm_router_mode` distingue os 3 motivos · `llm_input_chars` · **`llm_api_key_origin`** ·
`py.typed` · `setup.py` removido. Espera autorização para push + PR para `hml`.

✅ **CI de release corrigido** (2026-08-20). `nlp_engine-latest` no Volume **é a 0.9.4** — verificado
baixando a wheel e lendo o METADATA.

✅ **Changelog fechado** (27/08): 5 tags publicadas não tinham seção no `RELEASE.md` (`0.7.4` a
`0.8.1`; a `0.7.5` era a da entrega do transplante V1). Preenchidas, e o `check_release.py` ganhou
gate que falha nomeando a versão sem entrada.

🔴 **A falha de LLM é SILENCIOSA e a régua a mascara.** Provado em produção em 27/08. `met=None` por
endpoint morto vira "não atende" e o gate **rebaixa** — 2.947 laudos no TI-RADS em 21/08. Distinguir
"não pôde avaliar" de "não atendeu" **muda comportamento**: bump próprio, SPEC ainda não escrita.

🔴 **P0-0 novo: o juiz pode promover sem evidência de regra.** É quebra de invariante — já existe
`_tem_evidencia_dura` impedindo o juiz de **derrubar** evidência dura; falta a simétrica, impedindo
que ele **crie** relevância do nada. Hoje contornado por banda na config (ca-estômago), o que é
frágil: qualquer especialidade nova viola baixando o número. Medido: 2.861 de 3.199 laudos que iam
ao juiz não tinham evidência alguma.

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
- 🔴 **Card `285305`** (Defect, P1) — dois defeitos consolidados, ambos na lib:
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

## Alinhamento com a plataforma — 2026-08-21

Duas agendas (42min + 2h24) depois da queda do TI-RADS em produção. **Mapa completo em**
[`_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md`](_fundacao/mapa-gaps-lib-plataforma-2026-08-21.md)
— 15 gaps com dono, prioridade e solução.

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
- ⚠️ **`embedding_model` das 3 configs aponta para `diamond_ia_hml`**, o Volume ANTIGO. Hoje
  responde e o modelo está íntegro (verificado 20/08), então **não está degradando**. Mas o dia
  em que aquele Volume sair do ar, as três caem para `token_overlap` **sem erro e sem log**.
- `git-steward` ainda é 78 linhas sempre carregadas; candidato a virar regra curta + skill.
