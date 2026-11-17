---
tags: [biopsias, motor, regras-estruturadas, entrega, ops, arquitetura]
---

# Motor de regras estruturadas — entrega ao Ops (5 linhas, pronto para avaliação)

> v4 · 17/11/2026 · substitui a [[entrega-ops-motor-regras-estruturadas-v3|v3]] (só Ca de Cólon).
> História `408743` — *[Plataforma NLP] Industrializar o motor de regras estruturadas a partir do protótipo (piloto
> biópsias Ca de Cólon)*. Protótipo: `407833` — *[Tabia][Captação] Motor de regras estruturadas: protótipo e
> arquitetura para industrializar as linhas de biópsia (piloto Ca de Cólon)*.

## 1. O que é

Segundo runner da plataforma, para linhas cuja relevância vem de **dado estruturado** do Motion LIS
(anatomopatológico e citologia), e não de leitura de laudo por NLP. Gera SQL a partir da config da linha e grava a
tabela de saída **no mesmo contrato** das linhas NLP. View de exportação, randomização, OneDrive e relatório são os
da plataforma, sem mudança. Sem `nlp_engine`, sem LLM, sem embeddings.

| | |
|---|---|
| repositório | `fabrica-ia-nlp-platform` |
| branch | **`biopsias/feature/motor-regras-estruturadas`** (publicada, commit `09c408d`, sem PR) |
| guia dos arquivos | `plataform/tools/structured_rules/README.md`, na branch |
| arquitetura | `arquitetura-biopsias-v2.png` / `.drawio` (anexo da história) |
| escopo validado | **dev**, até a tabela de saída e a view |

**Um job por linha, quatro tasks:**

| task | notebook | situação |
|---|---|---|
| 1 · `motor_regras_<linha>` | `plataform/ntb_ia_motor_regras_e2e` | **novo** |
| 2 · `randomizacao_<linha>` | `tools/randomization/runs/ntb_ia_randomizacao` | existente; sem efeito enquanto a linha não estiver na config global |
| 3 · `onedrive_<linha>` | `tools/data_exchange/runs/ntb_ia_onedrive` | existente; `disabled: ${disabled?}` |
| 4 · `relatorio_<linha>` | `tools/reporting/runs/ntb_ia_relatorio_execucao` | existente |

## 2. As cinco linhas

| linha (`specialty`) | fonte | relevante | navegação (`id_linha`) |
|---|---|---|---|
| `biopsia_cancer_colon` | AP | neoplasia maligna primária, cólon/reto, amostra não cirúrgica | `cap:lin:cancer-colon` |
| `biopsia_cancer_estomago` | AP | idem, estômago | `cap:lin:cancer-estomago` |
| `biopsia_cancer_prostata` | AP | idem, próstata (RTU como não cirúrgica) | `cap:lin:pirads` ⚠️ a confirmar |
| `biopsia_tireoide` | citologia (PAAF) | Bethesda V ou VI, lido da conclusão | `cap:lin:tirads` ⚠️ a confirmar |
| `biopsia_cancer_rim` | AP | idem, rim; nefrectomia como cirúrgica | `cap:lin:cancer-rim` |

Linha nova do mesmo formato é **só config**: estômago, próstata e rim não tocaram o motor.

## 3. Checklist de provisão (Ops / Administrador)

Nomes literais. O `<amb>` do catálogo é `diamond_fabrica_ia_hml` (hml) e `diamond_fabrica_ia` (prd).

### 3.1 Por linha

| linha | schema (hml, prd) | tabela de saída | variável de pausa em `jobs/ambientes` | cluster a revisar |
|---|---|---|---|---|
| cólon | `biopsia_cancer_colon` | `tb_mod_diamond_biopsia_cancer_colon_saida_v0` | `job_status_biopsia_cancer_colon_batch` | `jobs/clusters/biopsia-cancer-colon-batch.json` |
| estômago | `biopsia_cancer_estomago` | `tb_mod_diamond_biopsia_cancer_estomago_saida_v0` | `job_status_biopsia_cancer_estomago_batch` | `jobs/clusters/biopsia-cancer-estomago-batch.json` |
| próstata | `biopsia_cancer_prostata` | `tb_mod_diamond_biopsia_cancer_prostata_saida_v0` | `job_status_biopsia_cancer_prostata_batch` | `jobs/clusters/biopsia-cancer-prostata-batch.json` |
| tireoide | `biopsia_tireoide` | `tb_mod_diamond_biopsia_tireoide_saida_v0` | `job_status_biopsia_tireoide_batch` | `jobs/clusters/biopsia-tireoide-batch.json` |
| rim | `biopsia_cancer_rim` | `tb_mod_diamond_biopsia_cancer_rim_saida_v0` | `job_status_biopsia_cancer_rim_batch` | `jobs/clusters/biopsia-cancer-rim-batch.json` |

- **Schema:** dev já provisionado nas cinco. Falta hml e prd.
- **Tabela de saída:** criada pelo runner no 1º `append`, com o schema das saídas NLP. Pré-criar é opcional.
- **Variável de pausa:** em `hml.json` e `prod.json`, valor inicial `PAUSED`. O job declara `pause_status` **sem `?`**:
  com a variável ausente o deploy falha, em vez de criar o job despausado.
- **Cluster:** proposta de 1 worker, sem LLM; `node_type`, `policy` e tags são decisão do Administrador.
- **Cadastro de envio:** incluir cada linha em `mlops_fabrica_ia.operacao.tb_cad_pipeline_configuracao_envio_nlp_platform`
  (hml e prd). **Sem ele a view de exportação sai vazia**, mesmo com a tabela de saída preenchida.

### 3.2 Uma vez, para todas as linhas

| item | detalhe | ambiente |
|---|---|---|
| leitura do Motion | `SELECT` da identidade de serviço do job em `especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame` | hml, prd |
| decrypt na view | a identidade que lê a view precisa de `EXECUTE` em `security.prd.rdsl_decrypt` (mesmo grant das linhas NLP) | hml, prd |
| esteira | PR da branch para a `hml` e deploy; depois `hml → main` | hml, prd |

## 4. Decisões de desenho do Ops

1. **Motor como 2º runner da plataforma.** O código novo está em `plataform/tools/structured_rules/` (vocabulário,
   contrato da config, gerador de SQL, runner) e `plataform/ntb_ia_motor_regras_e2e.py`. Revisão no padrão da
   plataforma (§6).
2. **Controle de carga e monitoramento.** O runner ainda **não grava** em `mlops_fabrica_ia.operacao.controle_carga`,
   que a task de relatório lê para o card do Teams — hoje o card sai sem os volumes do motor. Opções: o runner usa o
   `LoadControlTracker` da plataforma, ou o relatório lê o funil da task 1. O POP-IA-08 pede três tabelas (entrada,
   saída, monitoramento); o motor grava só a saída — a entrada é a própria tabela do Motion.
3. **Fonte do Motion:** leitura direta de `especialistas.gold` (hoje) ou ingestão pelo Datahub.

## 5. Roteiro de ativação em hml

| # | passo | quem | pronto quando |
|---|---|---|---|
| 1 | provisões da §3 em hml (schema, grant, variável `PAUSED`, cluster, cadastro de envio) | Ops / Administrador | itens marcados |
| 2 | PR `biopsias/feature/motor-regras-estruturadas` → `hml` e deploy | Ciência de Dados abre, Ops aprova | os 5 jobs existem em hml, **pausados** |
| 3 | run manual de cada job, uma linha por vez, começando pelo cólon | Ciência de Dados | task 1 `SUCCESS`; funil impresso; `engine_version = structured_rules-0.2.0` na saída |
| 4 | conferir a view | Ciência de Dados | view com os relevantes da data do run; CPF e nome decriptados; zero nulo em paciente e exame |
| 5 | conferir a idempotência | Ciência de Dados | 2º run no mesmo dia grava **0** pedidos |
| 6 | despausar | Ops | `job_status_…` = `UNPAUSED` em hml |

Em hml o OneDrive fica **desligado** (`disabled`) e a randomização sem efeito: nada vai à navegação real.
**Ordem sugerida:** cólon primeiro (piloto homologado) e depois as demais; as quatro restantes só repetem os passos.

## 6. Garantias e evidência

| garantia | como |
|---|---|
| laudo reemitido não é reenviado | `id_predicao` = nº do pedido; pedido já gravado não volta |
| retroação | 3 dias pela emissão do laudo, janela **fechada** na data de referência |
| sem PII na saída do motor | só a view decripta, como nas linhas NLP |
| "por que este paciente não veio?" | funil por etapa na saída da task; `findings_spans` com órgão, recorte, amostra e regra |
| config com bloco morto não passa | `extra="forbid"`; vocabulário referenciado por nome |
| entrada não vira SQL livre | data, `run_id` e tabela validados; literais escapados |
| linha completa nos três ambientes | teste trava config × job × cluster × navegação dev/hml/prd; pausa sem `?`; OneDrive desligável |

| evidência | resultado |
|---|---|
| testes sem Spark | **147 de 147**, `ruff` limpo; validador de padrão da plataforma: 0 erro, 0 alerta |
| mutação | 20 mutantes mortos no motor; o teste de artefatos falha com pausa `${…?}` e com navegação ausente |
| paridade do cólon com a régua homologada (v3) | **123 = 123** e **125 = 125** pedidos, zero diferença, depois de cada linha nova |
| cólon em dev (`persist=true`) | 19 pedidos gravados, 10 relevantes; 2º run: 0 gravados; view validada com o filtro de data trocado |
| estômago e próstata em dev (30 dias até 11/11) | 63 e 68 enviáveis; funil idêntico depois da entrada da tireoide |
| tireoide em dev (30 dias) | 65 Bethesda V/VI, igual à medição independente na mesma janela; 53 com exame no RC |

⚠️ **Rim:** config e testes prontos, **sem run do motor em dev**. Fazer antes do PR (passo 2).

⚠️ **A view sai vazia em dev para qualquer linha**: o cadastro de envio não tem linhas de dev. Não é defeito da linha.

## 7. Fora do escopo do Ops (não bloqueia hml)

| item | responsável |
|---|---|
| homologação das listas de estômago, próstata, tireoide e rim | Navegação |
| `id_linha` de próstata e tireoide; destinatários de hml/prd (hoje os do Ca de Cólon) | Navegação |
| origem `biopsia_motion` em `origem_identificacao` na Tábia; projetos das linhas em `envio_tabia` | Central de Captação (`fabrica-ia-central-captacao-app`) |
| linhas de biópsia no experimento de randomização (paciente com câncer confirmado no grupo controle) | decisão de produto; se entrar, declarar também `randomizacao_envio_biopsia_<linha>` em `jobs/ambientes` |
| pedidos combinados (ex.: EDA + colonoscopia no mesmo pedido) contam nas duas linhas — atribuição por amostra | motor, antes de prd |
| paciente só do RC (~7% dos pedidos sem exame no RC) ou também do cadastro do Motion | decisão de produto |
