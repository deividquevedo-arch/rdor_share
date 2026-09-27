# Revisão Técnica Rede D'Or — proposta do portfólio de modelos próprios (v0.1)

> **Alvo:** `docs/motor-nlp/_fundacao/propostas/proposta-portfolio-modelos-proprios-v0.md`, commit `341b0c4`.
> **Método:** skill `rededor-review`, sete agentes em modo leitura (Architect, Data Engineer, ML/NLP
> Engineer, QA Reviewer, Security & Governance, Documentation Keeper, Git Workflow Guardian), cada um
> contra a fonte da sua alçada: docs de fundação, código da `nlp-engine-lib`, código e configs da
> `fabrica-ia-nlp-platform`, POPs da Fábrica, catálogos do Databricks (perfil `nlp-platform`,
> só metadados e contagens) e o estado do git.
> **Data:** 2026-09-16. **Nada foi alterado na proposta por esta revisão.**

---

## 1. Resumo executivo

A tese da proposta sobrevive: modelos próprios por pergunta, com gabarito, versão e dono, e nunca
decisão sem evidência. **O que não sobrevive é parte do "por que agora" e a onda 0 como está escrita.**

| # | achado | severidade | dono do ajuste |
|---|---|---|---|
| 1 | O labeling do MLflow **não é** "o ciclo de rótulos inteiro, pronto e desligado por config". Está fiado ao runner, mas roda **só em dev e hml por código**, registra **um trace por run** e não por laudo, faz uma pergunta única sobre extração, nunca grava o Evaluation Dataset, tem revisor único hardcoded e não mascara nada. Ligá-lo em prd é alteração no motor da plataforma, alçada do Ops, e contradiz "nada muda antes do alinhamento". | **Crítico** | proposta §1, §5 onda 0 |
| 2 | A lib **não tem porta plugável**. O juiz chama HTTP direto; a extração de medida aceita um `caller` injetável que o pipeline não repassa. "Modelo próprio é outro adaptador da mesma porta" é falso hoje; exige um minor da lib com chave nova de saída, e portanto alinhamento com o Ops antes. | **Alto** | proposta §1, §3, §5 ondas 1–2 |
| 3 | O invariante "juiz nunca decide sem evidência de regra" **não está no código**. Só a banda por config o sustenta, e foi por isso que o P0-29 passou. A proposta o chama de "invariante da lib"; é invariante de desenho, e a `0.14.0` é o que o leva ao código. | **Alto** | proposta §3 |
| 4 | O diagrama da seção 3 faz o **estado alimentar os modelos**, o que inverte a premissa do contexto do paciente: o motor declara o que falta, e só o orquestrador lê o estado. M3 está na coluna dos modelos e pertence ao consumo. Faltam as três libs, a decisão "saída por paciente-condição ou por exame" e as fases 0 e 1 do contexto. | **Crítico** | proposta §3, §5, §6 |
| 5 | A **decisão 1 inverte o POP**: o ciclo de vida prescreve discovery e treino em `dev`, sobre coorte aprovada e sem cópia indiscriminada de PII, e re-treino no ambiente alvo pela esteira. A proposta pede "nunca em dev". | **Alto** | proposta §6 |
| 6 | O catálogo M1 e M2 afirma "dado existe: sim" sobre fontes que não sustentam: o domínio `prontuario` do `data_manager` é um **snapshot congelado desde 14/05** sem consumidor; a fonte viva é `gold_fabrica_ia_dev.fhir`, 24 tabelas achatadas, **só em dev**; a tabela de retorno citada como gabarito é **carga única de planilha de homologação** sem `id_exame`; o laboratório tem unidade vazia em 100%. | **Alto** | proposta §4, §4.2 |
| 7 | Não há **de-identificação antes do juiz** em lugar nenhum (lib nem plataforma), o SDD deixa o gate em aberto e o POP exige ciência formal do DPO para LLM em prd. Vale igual para o Review App e para o dataset de rótulos. Entra como pré-condição da onda 0. | **Alto** | proposta §0, §5 |
| 8 | Nomenclatura e regras do repo: "Central de Navegação" não existe no repositório (é **Central de Captação**), e o Portal Clínico de IA é outro produto; card `303791` só por número; decisões citadas só por data; "nosso" em primeira pessoa; seções 3 e 4.2 reescrevem o que deveria ser remissão. | **Médio** | proposta, toda |

**Achados fora da proposta que viraram pendência própria** (seção 4): PHI em claro numa cópia de
planilha no lake; segredo literal no envio ao OneDrive; o schema `legado` de prd é cópia manual de
05/09 e não mede execução do legado; o `data_manager` tem catálogo hardcoded com `TODO`.

Contagem do QA sobre as afirmações factuais da proposta: **42 confirmadas, 11 divergentes, 7 sem
fonte** (3 cobertas pelo Anexo B, 4 não).

---

## 2. Achados por agente

### 2.1 Architect

- **Crítico.** Diagrama §3: ESTADO → MODELOS. Contradiz a regra 3 da própria proposta e a inversão
  do desenho macro (`doc-desenho-macro-contexto-paciente-v0.md:103-105`; `nota-review:29-31`). O
  estado é escrito e lido pelo orquestrador para casar pendências; modelo recebe dict pronto.
- **Crítico.** "Estado agnóstico do hub" foi estendido a entrada de todos os modelos. O desenho macro
  (`:242-245`) recomenda contrato agnóstico **só para a pendência**. Como está, é a base de
  conhecimento paralela ao hub que a própria proposta nega.
- **Alto.** M3 listado entre os modelos especialistas; o desenho macro (`:139`, `:270`) põe
  elegibilidade fora do motor, com o consumidor.
- **Alto.** Omissões: as três libs `nlp_engine`, `data_manage` e `monitoring` (`anexo02:61-84`);
  o componente 4 "avaliação por paciente" (lib) distinto do componente 3 orquestrador (plataforma),
  "o erro mais fácil" (`macro:34-49`); a decisão 1 do desenho macro, saída por paciente-condição ou
  por exame, "a mais cara de reverter" (`nota-review:92`), ausente da §6; as fases 0 e 1 do contexto
  nas ondas.
- **Alto.** "Modelos por contrato atrás da mesma porta" é abstração vazada: os contratos são
  heterogêneos (texto → decisão, frase → QueryPlan, analitos → veredito, tabular).
- **Médio.** "Regra de ouro do SDD" citada com conteúdo que o SDD não tem. A regra
  (`05-sdd-arquitetura.md:42-43`) é "camada 3c/4 não muda; nenhuma regra clínica fora do motor"; o
  contrato é CONFIG + Jobs API + view/MLflow (`:174`). Models in UC não aparece no SDD.
- **Médio.** "LLM em dois lugares, e só neles" omite o row-LLM C5 da busca conversacional e o
  `llm_fallback` da extração ordinal.
- **Médio.** Régua "registrada em Models in UC" contradiz a diretriz das libs: variação de
  comportamento é `config_version` + `engine_version` no output, nunca artefato de modelo.
- **Médio.** "Fase 0" atribuída ao prontuário; a fase 0 é exclusão e refutação **no escopo do
  laudo**. Prontuário é a pergunta 3 do desenho macro, Research separado.
- **Baixo.** Gate "plano de consulta validado sem LLM": o interpretador C2 é LLM por definição; o
  que é sem LLM é o validador C3.

### 2.2 ML/NLP Engineer (código da lib, `0.12.3`)

- **Crítico para o texto.** Não existe Protocol nem backend plugável. Ponto único de saída:
  `call_openai_compatible_chat` (`llm_router_backend.py:793`, httpx direto). Juiz:
  `llm_router_step` → `decide_llm_http` → HTTP, sem injeção. Quantitativa: `Caller` existe
  (`quantitative.py:31-32`) e é aceito por `process_quantitative_criteria` (`:1551-1563`), mas
  `step_measure` (`decision_pipeline.py:817-828`) não o repassa e `EngineContext` não tem o campo.
  O que existe é "endpoint compatível com OpenAI configurável por `base_url` e `model`": modelo
  próprio em Model Serving entra sem mudar a lib; modelo em processo, não.
- **Crítico para o texto.** Invariante ausente. O juiz recebe o documento inteiro truncado a 8.000
  caracteres (`decision_pipeline.py:691-697`, `:967-970`), sem span, achado ou evidência; devolve
  `{"relevante": bool}`. Não há checagem de `n_positive_spans > 0`. Teto analítico sem span
  0,597 (`scoring.py:96-98`) contra piso 0,35 da hepatologia: a banda é a única barreira.
- **Confirmado.** Extração de medida é substituível: o LLM só localiza e lê; parse, conversão de
  unidade, comparação, `plausible_range`, `threshold_by` e gating por âncora já são código. Ressalva:
  `evidence` é texto livre, não offset; supervisão fraca exige alinhamento ao texto; o vínculo
  lesão↔medida (`0.15.0`) é documental e um encoder herda igual.
- **Médio.** `embedding_model` por URI `models:/` não funciona: `SentenceTransformer(model_name)`
  aceita id HF ou caminho local; URI cai em `OSError`, capturado, e degrada para `token_overlap` em
  silêncio, o mesmo modo quebrado das quatro linhas. A lib não importa mlflow.
- **Confirmado.** Tokens só no juiz (`llm_router_backend.py:913-919`, `:1077-1079`); quantitativa e
  ordinal não têm campo.
- **Viabilidade das ondas 1 e 2:** minor aditivo da lib: `caller` no `EngineContext` e repasse em
  `step_measure`; `decide_llm_http` aceitando `caller`; bloco de sombra na saída sem tocar `met`,
  `fl` e `decision_source` (**chave nova de saída, alinhar com o Ops antes**, no pacote da
  `0.14.0`/`0.15.0`); modelo fora do core, por extra ou callable do runner; teste de byte-identidade
  com desafiante ligado e desligado. Juiz próprio: janela de 512 tokens do BERTimbau exige receber
  janelas centradas nos `findings_spans`, o que fecha o P0-29 por construção.

### 2.3 Data Engineer (catálogos, perfil `nlp-platform`)

- **Alto.** Domínio `prontuario` (`ntb_ia_data_manager.py:594-660`) aponta para
  `gold_corporativo_ia.workarea.tb_gold_mov_paciente_prontuario_eletronico`: 2,26 bilhões de linhas,
  1,59 milhão de pacientes, **datas de 14/01 a 14/05/2026, sem atualização desde então**; catálogo
  hardcoded com `# TODO` (`:351`); **zero consumidores** fora do `data_manager`. Não existe domínio
  `encounter`; existe `encontro`, com datas sujas até o ano 8026.
- **Médio.** O catálogo `gold_fabrica_ia` **não existe**; existe `gold_fabrica_ia_dev` com schemas
  `fhir` e `finops`. `fhir` tem 24 tabelas achatadas em português (`encontro`, `procedimento`,
  `condicao`, `observacao_exames`, `declaracao_medicamento`, `laudo_diagnostico`, `paciente`...),
  criadas em 29/08 e gravadas até 11/09 por service principal. Não são resources FHIR crus; derivam
  de `gold_corporativo.<resource>.tb_gold_mov_*`. **Só dev.** É o produto do mapeamento em curso e
  a fonte que o M1 deve declarar.
- **Alto.** `diamond_ia_hml.fabrica_ia.tb_mod_monitoramento_retorno`: 9.248 linhas, carga única de
  planilha de homologação (10/06 a 20/07), `id_exame`, `dt_execucao_modelo` e `cd_status_revisao`
  **todos nulos**, `cd_fluxo_origem = 'homologacao'` em 100%. **Não é retorno da navegação nem serve
  de gabarito.** Candidato real: `diamond_fabrica_ia_dev.jornada.tb_mov_navegacao` (10.295 linhas,
  `fl_navegado` e `fl_captado` preenchidos, desfecho e linha de cuidado vazios, só dev, parado em
  19/08).
- **Crítico (governança, fora da proposta).** A cópia
  `diamond_ia_hml.workarea.fabrica_ia_tb_mod_monitoramento_retorno` é planilha crua com colunas
  `nome_paciente`, `cpf`, `telefone`, `crm`: **PHI em claro no lake**.
- **Médio.** Laboratório: `gold_fabrica_ia_dev.fhir.observacao_exames` tem 629,6 milhões de linhas,
  `vl_numerico` em 392,6 milhões e **`un_medida` vazia em 100%**. O `plausible_range` da camada
  quantitativa depende da unidade. No `data_manager` só há `paciente.exames_lab` sobre a tabela de
  pacientes, com **0 preenchidos em 16,15 milhões**. Sem domínio de laboratório.
- **Médio.** Estado paciente-condição não existe; `fl_modelo_birads` e `vl_modelo_FIB` estão em
  0 de 16,15 milhões. O mais próximo é
  `gold_corporativo.condicao_saude.tb_gold_mod_condicao_saude_historico` (3,03 milhões de linhas,
  536.885 pacientes, 19 condições, gravada até 14/09 por SP): chave `(codPaciente, nmeCondicao,
  dscPesquisa, dscResultadoPesquisa)`, sem critério nem prazo. Ponto de partida do contrato.
- **Médio.** `diamond_fabrica_ia.legado`: 17 tabelas criadas em **05/09 por usuário humano**; só a
  ateromatose foi reescrita depois (09 e 11/09, por SP). Zero ocorrências de `diamond_fabrica_ia.legado`
  nos três repositórios. O `max(dataExecucaoModelo)` mede a cópia, não a última execução do legado.
- **Transversal.** Tudo de M1 e M2 existe só em dev ou num catálogo único sem promoção; a decisão 1
  não tem dado em hml e prd. Quatro chaves de paciente sem de-para (`id_patient`, `id_paciente`,
  `codPaciente`, `subject`). Datas em 1900, 8026 e 9926. Sem gate de qualidade, gabarito nasce
  contaminado.

### 2.4 QA Reviewer (rastreio de evidência)

42 confirmadas, 11 divergentes, 7 sem fonte. Divergências:

| sev. | afirmação | fonte | correção |
|---|---|---|---|
| Alta | labeling "pronto", "desligado por config", "sem alinhamento" | `ntb_ia_motor_e2e.py:387, 441-456, 581-590, 721` | ver item 1 do resumo |
| Média | pin "desde 15/09" | `ESTADO.md:670` "verificado em 15/09"; diário: "já estava aplicado" | "verificado em 15/09" |
| Média | "história S15 de fine-tuning" | anexo03 vai até S12b; fine-tuning é Fase 3 (docs 02, 04, 05, 06) | "Fase 3 dos docs de fundação" |
| Média | M2 "gabarito parcial (142, 95)"; "só M0 e M2 têm gabarito" | `nota-review:13,119` é aceite **medido** por régua, sem dono clínico | "aceite medido, sem gabarito validado"; só M0 tem gabarito |
| Média | card `303791` só por número | regra `00-global-project-rules.md:19-35` | *Plano de Migração algoritmos final* |
| Baixa | citação "objetivo de evolução, não ponto de partida" | não é verbatim (`02:342`) | tirar aspas |
| Baixa | "POP-IA-001 passo 9" para gabarito congelado | é passo 5; 9 é avaliação | "passos 5 e 9" |
| Baixa | "doc macro §4" para "não faz I/O" | é §5 e §6 | corrigir |
| Baixa | pulmão e reumatologia "não declaram" embeddings | ambas declaram `use_embeddings: False` | "desligados" |
| Baixa | "MCC 0,58 virou 0,98" | `base-ouro-tirads-v2-metricas.md:39`: 0,596 → 0,9803 | "0,60 → 0,98" |
| Baixa | achado do TI-RADS "passou a ser entregue" | PR 7287 mergeado na `hml`; prd não registrado | "deploy em prd a confirmar" |

Sem fonte no repositório: a PoC de captação por prompt único (fonte é
`zOPs/fabrica-ia-plataforma-handover/apps/databricks/dr_captacao/README.md:4-10`, citar); "evolução
em texto" no M1 (spec 23 não lista); "BioBERTpt" (doc 04 cita BioBERT); doc 03 do brainstorm
ausente do Anexo A e é a única fonte de "RAG fora do escopo". ⚠️ **1.371** aparece no `ESTADO.md`
com dois sentidos (`:258` entregues pela falha; `:260` zero falhas em 1.371 chamadas): conferir
a fonte antes de publicar.

Gabaritos por linha (coluna do M0 está correta: parcial, fora do lake): tireoide v2 congelada em
24/07 com MCC 0,9803; ca-estômago consolidada em 28/07; pulmão 388 laudos com gold humano fora do
repo; hepatologia parcial, 73 laudos; ca-rim só comentário na config; reumatologia nenhum.

### 2.5 Security & Governance (POPs, SDD, código)

- **Alto.** Decisão 1 diverge do POP-IA-001: fases 1 e 2 em **dev** (L27, L191); a regra é "sem
  cópia indiscriminada de PII" (L166, L393), não "nunca dev"; passo 15 promove código e **re-treina
  no ambiente alvo pela esteira** (L321). POP-IA-07 L179 e L446: grupos editam só em dev e leem em
  hml e prd; guia da Fábrica §8: nenhuma escrita manual em hml ou prd. Coerentes: `diamond.<produto>`
  para adaptado, `gold.modelo` para base, coorte aprovada pelo especialista.
- **Alto.** PHI ao LLM: SDD RNF-09 de-identificação antes do LLM está **em aberto**
  (`05-sdd:120-123`); POP-IA-08 L116-118 e L319 exigem ciência formal de Compliance/DPO; L310 põe a
  máscara como demanda à plataforma. Lib: nenhuma de-identificação, único `mask` é de segredo
  (`observability.py:31,82`); juiz recebe até 8.000 caracteres do texto tratado. Plataforma só
  **decifra** PII (`nlp_ia_06_view.py:41`).
- **Alto.** Alçada: labeling vive em `plataform/ntb_ia_motor_e2e.py:441-458` (código do motor,
  POP-IA-08 L43), não na config; `active = mlflow_active and env in ["dev","hml"]` (`:443`).
  Extrator e juiz próprios como adaptador da lib são bump de `nlp_engine_version`, alçada do Dono
  do NLP Engine.
- **Médio.** POP-IA-09 L24: biblioteca por tag semver; modelo e config por `config_version` e
  alias. Régua não é "modelo" no sentido do POP; o corolário do §2 contradiz o critério 3.
- **Alto (pré-existente).** `plataform/tools/data_exchange/runs/ntb_ia_onedrive.py:121-124`: URL de
  trigger de Logic App com assinatura literal; nenhum `dbutils.secrets` no `data_exchange`; e-mails
  nominais em `jobs/ambientes/*.json` e revisor hardcoded no e2e. Não reproduzir valores.

### 2.6 Documentation Keeper

- **Alto.** "Central de Navegação" só existe na proposta. O repositório usa **Central de Captação
  (Navegação)** (`10-arquivo-de-navegacao.md:6-7`, `14-data-export-view.md:18`,
  `doc-busca-conversacional:3,25`). O Portal Clínico de IA (`03-sdd:10`) é outro produto: onde o
  clínico configura soluções, não o consumidor das listas.
- **Alto.** Card `303791` só por número (l.182), fora do `_processo/cards/indice-de-cards.md`.
- **Média.** Decisões citadas só por data (l.97, 98, 182, 189, 300); a de "02/09 sobre a base
  ouro" não tem documento, só o `ESTADO.md`. Reescrever como fato com remissão.
- **Média.** Primeira pessoa: "a nossa recomendação" (l.276), "no nosso schema", "sem trabalho
  nosso" (l.143, 283, 198); "modelo nosso" como termo em sete linhas.
- **Média.** Duplicação a virar remissão: §3 regras 1 a 5 reescrevem o desenho macro §5 a §7;
  §4.2 reescreve §8 e §9 do mesmo; "onde o LLM fica" reescreve a arquitetura alvo §2 e §6;
  P0-29 e `0.14.0` sem citar `nlp-engine-lib/docs/plano-acao-backlog-lib-2026-09.md`.
- **Média.** `tb_mod_monitoramento_retorno` no workspace antigo, preenchimento por definir
  (`framework-homologacao-hitl-v0.md` §3.2).
- **Baixa.** Título "(v0)" com nota "v0.1": padrão da fundação é nome de arquivo fixo e título com o
  bump. Recuperabilidade: `ESTADO.md` não cita a proposta; `docs/motor-nlp/README.md` parado em
  13/07. Os 19 caminhos do Anexo A existem.

### 2.7 Git Workflow Guardian

- **Baixo.** Branch `worktree-proposta-modelos-proprios` tem um arquivo, +352 linhas, merge-base
  igual ao `main`: **fast-forward limpo**. Cópia no checkout principal byte-idêntica ao blob. A branch
  não toca `ESTADO.md`, que tem +231/−14 linhas não commitadas de 16/09 no checkout.
- **Sem PHI** na proposta (grep por identificadores e trechos de laudo: nenhum).
- **Alto (pré-existente).** `main` está 97 commits à frente do GitHub de backup com **15 CSVs de
  laudo**; nenhum remoto serve para a proposta. Entrega ao time: docx pelo
  `.claude/scripts/md-para-docx.py` (existe, não versionado; converter e conferir o diagrama ASCII
  no Word) mais OneDrive e comentário no card.

---

## 3. Síntese cruzada

**Convergências entre agentes:**

- **O labeling é o ponto de falha da onda 0.** QA (fiado, um trace por run, pergunta única) e
  Segurança (dataset nunca gravado, sem máscara, revisor único, dev e hml por código) chegam ao mesmo
  lugar por caminhos distintos. A onda 0 "sem alinhamento novo" não existe: é pedido ao Ops.
- **"Porta" e "invariante" são desenho, não código.** ML/NLP Engineer prova no código; Architect
  chega ao mesmo pela arquitetura alvo (a porta é `llm.connection` para chat OpenAI-compatível;
  encoder local é step novo via registry).
- **Critério 3 precisa se dividir.** Architect (Models in UC não está no SDD; régua varia por
  `config_version` + `engine_version`) e Segurança (POP-IA-09 separa lib por semver de modelo por
  alias) convergem: pesos em Models in UC **ou** `config_version` + `engine_version` no output.
- **Retorno da navegação não é gabarito.** Data Engineer (carga única, colunas nulas) e Documentation
  Keeper (preenchimento por definir) convergem; QA acrescenta que só M0 tem gabarito.
- **Decisão 1 está invertida.** Segurança pelo POP; Data Engineer pela realidade dos dados, que só
  existem em dev.

**Conflito resolvido:** QA diz que o labeling "está fiado" e Segurança diz que "não roda". Os dois
estão certos: o trace do run é registrado, o dataset e a amostra por laudo não. O texto correto é
"esqueleto fiado ao runner, ativo só em dev e hml, sem laudo no trace, sem dataset gravado".

**Divergência entre esta revisão e a medição de 16/09 da proposta:** o Anexo B afirma que
`max(dataExecucaoModelo)` no schema `legado` é "última execução do legado". Data Engineer mostra que
o schema é cópia manual de 05/09; o número mede a cópia. O parágrafo "Legado ainda ativo" da §4.1
precisa ser reescrito.

---

## 4. Riscos principais

| risco | severidade | de onde vem |
|---|---|---|
| Levar ao Head uma proposta com "pronto" que não está pronto e "invariante" que não está no código; perde-se a credibilidade da tese, que é correta | **Crítico** | itens 1, 2, 3 do resumo |
| PHI em claro numa cópia de planilha no lake (`diamond_ia_hml.workarea.fabrica_ia_tb_mod_monitoramento_retorno`) | **Crítico** | Data Engineer; independe da proposta; **card e remoção** |
| Treinar sobre `workarea` congelado ou sobre `fhir` só de dev, sem de-para de chaves e sem gate de qualidade: gabarito contaminado na origem | **Alto** | Data Engineer |
| Texto de laudo em trace do MLflow e em dataset de rótulos sem máscara e sem ciência do DPO | **Alto** | Segurança |
| Segredo literal no envio ao OneDrive e destinatários nominais no código, num fluxo que a proposta quer ampliar | **Alto**, pré-existente | Segurança |
| Diagrama que legitima "estado alimenta modelo" vira arquitetura por inércia: o mesmo laudo passa a decidir diferente por dia | **Alto** | Architect |
| Interpretar o schema `legado` como execução: conclusão errada sobre linhas legadas paradas | **Médio** | Data Engineer |

---

## 5. Recomendações priorizadas

**P0 — antes de qualquer circulação da proposta**

1. Reescrever §1 e §5 onda 0 sobre o labeling: "esqueleto fiado ao runner, ativo só em dev e hml
   por código, um trace por run, sem laudo e sem dataset; levar `id_exame` e o trecho de evidência
   ao trace, gravar o dataset com a proteção da coorte e definir acesso é trabalho da plataforma,
   e a ativação em prd é decisão do Ops". Onda 0 passa a "pedir e especificar", não "ligar".
2. Reescrever §1 "porta" e §3 "invariante" com o que o código mostra, e acrescentar à onda 1 a
   pré-condição "minor da lib: `caller` no `EngineContext`, bloco de sombra na saída alinhado com o
   Ops no pacote da `0.14.0`". Juiz próprio condicionado aos spans de regra, janela de 512 tokens.
3. Redesenhar o diagrama §3 a partir do desenho macro §5 a §7: orquestrador entre modelos e estado;
   estado é saída dos modelos e entrada do orquestrador; M3 na coluna de consumo, sem o "M";
   contrato agnóstico restrito à fila de pendência. Acrescentar §3.1 "onde cada modelo mora": lib
   (M0, M2, M6), plataforma (orquestrador, estado), app (M5), consumo (M3).
4. Reescrever a decisão 1 conforme o POP: discovery e treino em `dev` sobre coorte aprovada e sem
   cópia indiscriminada de PII; registro e gate em `hml`; re-treino em `prd` pela esteira, nunca
   manual. Acrescentar decisão 8: saída por paciente-condição ou por exame. Acrescentar pré-condição
   da onda 0: ciência formal do DPO (RNF-09 em aberto) para LLM, Review App e dataset.
5. Abrir card para o PHI em claro na `workarea` de `diamond_ia_hml` e sinalizar remoção. Não é da
   proposta, mas foi achado por ela.

**P1 — para o catálogo ficar verdadeiro**

6. M1: fonte é `gold_fabrica_ia_dev.fhir` (24 tabelas, só dev, gravadas até 11/09) e o FHIR canônico
   de `gold_corporativo`; o domínio `prontuario` do `data_manager` é snapshot de jan–mai sem
   consumidor. Retirar "evolução em texto" até ter fonte. Trocar `tb_mod_monitoramento_retorno` por
   "candidatos: `jornada.tb_mov_navegacao` (só dev, desfecho vazio) e rotulagem por amostra".
7. M2: nomear `fhir.observacao_exames` e registrar `un_medida` vazia em 100% e a ausência de domínio
   no `data_manager`. Coluna "gabarito existe?" do M2 passa a "aceite medido, sem gabarito validado".
8. §4.1 legado: "`diamond_fabrica_ia.legado` é cópia pontual de 05/09; só a ateromatose recebe
   escrita recorrente; a data das demais mede a cópia".
9. Estado: citar `condicao_saude.tb_gold_mod_condicao_saude_historico` como ponto de partida do
   contrato e o de-para das quatro chaves de paciente como entrega da onda 0.
10. Fases 0 e 1 do contexto do paciente entram na onda 1 antes do orquestrador; corrigir "fase 0"
    da §4.2 para "pergunta 3 do desenho macro, Research separado".

**P2 — forma**

11. "Central de Captação (Navegação)" em todo o texto; regra de ouro citada de `05-sdd:42-43,174`
    e marcada como **estendida** à Central; Models in UC marcado como proposta nova.
12. Card `303791` — *Plano de Migração algoritmos final*, e no índice. Decisões por data viram
    remissão a documento. Varredura de "nosso/nossa". Critério 3 dividido (pesos **ou**
    `config_version` + `engine_version`). Passos 5 e 9 do POP. "Fase 3" em vez de "S15". Aspas fora
    da citação não verbatim. "0,60 → 0,98" com fonte. Embeddings de pulmão e reumatologia
    "desligados". PR 7287 "deploy em prd a confirmar". Citar `dr_captacao/README.md` para a PoC.
13. §3 regras e §4.2 viram remissão ao desenho macro; "onde o LLM fica" remete à arquitetura alvo;
    Anexo A ganha o doc 03 do brainstorm, o plano de ação do backlog da lib e
    `ntb_ia_motor_e2e.py:441-456` como fonte do estado real do labeling.
14. Título "(v0.1)" com linha de versão; uma linha em "A refinar" do `ESTADO.md` e uma no
    `docs/motor-nlp/README.md`.
15. Git: fast-forward da branch no `main`, remover worktree e branch, gerar docx pelo script,
    entregar por OneDrive e card. Nenhum push.

---

## 6. Próximos passos

1. Aplicar P0 e P1 como **v0.2** da proposta, medindo de novo o que mudar de fonte (M1, M2,
   legado), com a data.
2. Abrir os cards: PHI em claro na `workarea` (Crítico); labeling do MLflow inutilizável em prd e sem
   laudo no trace (Alto, plataforma); catálogo hardcoded no `data_manager` (Médio, plataforma).
   Segredo literal no OneDrive e e-mails nominais já constam no `ESTADO.md` sem card: abrir.
3. Conferir a coincidência do número 1.371 no `ESTADO.md:258,260` antes de citar em qualquer
   documento.
4. Só então: docx, OneDrive, card, e a agenda com o dono da Central de Captação.
