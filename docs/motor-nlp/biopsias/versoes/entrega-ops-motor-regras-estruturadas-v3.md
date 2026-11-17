---
tags: [biopsias, motor, regras-estruturadas, entrega, ops, arquitetura]
---

# Motor de regras estruturadas — entrega ao Ops (protótipo testado)

> v3 · 12/11/2026 · branch publicada no Azure, código revisado no padrão da plataforma, janela de emissão fechada na
> data de referência e paridade refeita. Anteriores: [[entrega-ops-motor-regras-estruturadas-v2|v2]] ·
> [[entrega-ops-motor-regras-estruturadas-v1|v1]].
> Primeira linha: `biopsia_cancer_colon` (AP do Motion LIS, cólon e reto). Proposta:
> [[proposta-motor-regras-estruturadas-v0]] · plano: [[plano-industrializacao-biopsias-v2]] · régua:
> `consulta-lista-colon-biopsias-v3.sql`. Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados do
> Motion LIS disponíveis no Repositório Clínico*.

## 1. Arquitetura

![[arquitetura-biopsias-v2.png]]

Editável: `arquitetura-biopsias-v2.drawio`.

**Um job por linha, no padrão das linhas NLP.** Só a task 1 é nova. Ela grava a tabela de saída **no mesmo contrato**
das linhas NLP (`tb_mod_diamond_<linha>_saida_v0`, 23 colunas), então view de exportação, randomização, OneDrive, API e
relatório funcionam sem mudança.

| task | notebook | situação |
|---|---|---|
| 1 · `motor_regras_<linha>` | `plataform/ntb_ia_motor_regras_e2e` | **novo** — config → funil → saída → view |
| 2 · `randomizacao_<linha>` | `tools/randomization/runs/ntb_ia_randomizacao` | existente; sai como `randomizacao_inativa` enquanto a linha não estiver na config global |
| 3 · `onedrive_<linha>` | `tools/data_exchange/runs/ntb_ia_onedrive` | existente; `disabled` em hml |
| 4 · `relatorio_<linha>` | `tools/reporting/runs/ntb_ia_relatorio_execucao` | existente |

**Sem `nlp_engine`, sem LLM, sem embeddings.** O motor gera SQL a partir da config e o executa no Spark. Cluster sem
token de LLM e sem driver grande.

## 2. O que está na branch

Repositório `fabrica-ia-nlp-platform`, branch **`biopsias/feature/motor-regras-estruturadas`** (publicada, commit `d035cbc`,
base `hml` em `70d426b`). **Sem PR** — o PR para a `hml` sai depois do alinhamento com o Ops. O guia dos arquivos está
na própria branch, em `plataform/tools/structured_rules/README.md`.

| arquivo | papel | alçada (POP-IA-08) |
|---|---|---|
| `plataform/tools/structured_rules/ntb_ia_rules_vocabulario.py` | vocabulário compartilhado: fontes, órgãos, classes de amostra | Ciência de Dados |
| `plataform/tools/structured_rules/ntb_ia_rules_config.py` | contrato da config (pydantic, chave desconhecida é erro) | plataforma |
| `plataform/tools/structured_rules/ntb_ia_rules_sql.py` | gerador de SQL (uma CTE por operação) e funil | plataforma |
| `plataform/tools/structured_rules/ntb_ia_rules_runner.py` | grava a saída no contrato e devolve o funil | plataforma |
| `plataform/ntb_ia_motor_regras_e2e.py` | notebook da task 1 (composition root, 14 células com título) | plataforma |
| `plataform/config/speciality/ntb_ia_biopsia_cancer_colon_config.py` | config da linha, com cabeçalho e changelog | Ciência de Dados |
| `jobs/definicoes/biopsia-cancer-colon-batch.json` | job (4 tasks, 04:00 seg–sex) | Ciência de Dados |
| `jobs/clusters/biopsia-cancer-colon-batch.json` | cluster da linha | **Administrador** |
| `plataform/tests/structured_rules/` | 59 testes, sem Spark | — |
| `scripts/structured_rules/paridade_v3.py` | paridade contra a consulta homologada (só contagens) | — |

Os módulos do motor seguem o padrão da plataforma: **notebooks** carregados pela task com `%run` em ordem
(vocabulário → config → sql → runner), sem `dbutils` no nível do módulo. Os testes os executam por caminho num
namespace só, como o `%run` (modelo de `tests/randomization/test_resolver.py`); um teste trava a ordem dos `%run`.

## 3. Garantias da linha

| garantia | como |
|---|---|
| laudo reemitido não é reenviado | `id_predicao` = nº do pedido; pedido já gravado não volta (`NOT EXISTS` na saída) |
| retroação | janela de 3 dias pela emissão do laudo (`janela.dias_emissao`) |
| sem PII na saída do motor | decripta só a view, como nas demais linhas |
| "por que este paciente não veio?" | funil por etapa na saída da task + `findings_spans` com órgão, recorte, classe e regra da amostra |
| config com bloco morto não passa | `extra="forbid"`; vocabulário referenciado por nome e conferido |
| entrada não vira SQL livre | data, `run_id` e tabela validados; literais escapados |

## 4. Evidência de teste

| teste | resultado |
|---|---|
| unitários (config, vocabulário, SQL, runner, ordem dos `%run`) | **64 de 64**, `ruff` limpo |
| mutação | **13 de 13 mutantes mortos** (envio, apêndice, ordem dos órgãos, escape, dedup, `forbid`, validação de classe, lookbehind de polipectomia, ordem segmento × fragmento, ordem dos `%run`, tabela ausente, dedup só com tabela existente, fim da janela) |
| paridade com a v3 homologada — 30 dias até 03/11, janela fechada nos dois lados | **123 = 123** pedidos; **zero** diferença em pedido, exame, recorte, amostra e envio; 70 enviáveis nos dois |
| paridade — 30 dias até 11/11 | **126 = 126**; zero diferença; 73 enviáveis nos dois |
| paridade anterior, sem fim de janela (v1/v2 deste documento) | 161 = 161 e 124 = 124; zero diferença |
| controle positivo da paridade (régua alterada) | sem o reto: 57 pedidos só na v3; enviando peça: 33 envios divergem — a comparação detecta diferença |
| execução no workspace de dev (serverless, `persist=false`) | `TERMINATED SUCCESS` · funil de 3 dias: 4.116 pedidos → 198 qualificados → 21 cólon/reto → 19 com RC → 10 enviáveis (todas as unidades) |

| gravação em dev (`persist=true`, schema `diamond_fabrica_ia_dev.biopsia_cancer_colon`) | tabela de saída criada no 1º append: **19 pedidos, 10 relevantes**, `engine_version = structured_rules-0.1.0` |
| idempotência (2ª execução no mesmo dia) | **0 gravados**; a tabela segue com 19 pedidos de 1 execução |
| view de exportação | criada pelo `nlp_ia_06_view` da plataforma; com o filtro de data do cadastro de envio trocado pela data do run: **9 linhas** (os relevantes de unidades navegadas), CPF e nome decriptados, categoria, laudo montado, `PAT`, unidade e projeto preenchidos em 9 de 9 |

⚠️ **A view sai vazia em dev para qualquer linha:** o filtro de data dela lê
`mlops_fabrica_ia.operacao.tb_cad_pipeline_configuracao_envio_nlp_platform` (datas liberadas por ambiente e projeto),
que tem linhas de `prd` e `hml` e **nenhuma de `dev`**. Não é defeito da linha.

### Revisão de padrão (v3)

| ponto | ajuste |
|---|---|
| módulos do motor | cabeçalho `# MAGIC %md` com título, alçada e ordem de carga, como os de `tools/` |
| runner | classe pydantic com `Field(description=…)`, como os gerenciadores do pipeline |
| funções públicas | docstrings `Args`/`Returns`/`Raises`/`Example` |
| notebook da task | 14 células com `DBTITLE`, como o `ntb_ia_motor_e2e` |
| `pause_status` do job | **obrigatório** (spec 31 §4.3): com `${…?}`, variável ausente remove a chave e o job nasceria despausado |
| janela de emissão | **fechada na data de referência**: reprocessamento de data passada não lê laudo emitido depois dela |

## 5. O que o Ops precisa prover

| # | item | ambiente |
|---|---|---|
| 1 | schema `biopsia_cancer_colon` em `diamond_fabrica_ia_dev`, `_hml` e `diamond_fabrica_ia` | dev, hml, prd |
| 2 | tabela `tb_mod_diamond_biopsia_cancer_colon_saida_v0` com o schema das saídas NLP (ou criada no 1º append) | dev, hml, prd |
| 3 | `SELECT` da identidade de serviço do job em `especialistas.gold.tbl_motionlis_motionlis_motion_observation_exame` | hml, prd |
| 4 | variável `job_status_biopsia_cancer_colon_batch` em `jobs/ambientes` (nasce `PAUSED`) | hml, prd |
| 5 | revisão do cluster da linha (sem LLM, 1 worker) | — |
| 6 | incluir `biopsia_cancer_colon` no processo que grava `tb_cad_pipeline_configuracao_envio_nlp_platform` (datas liberadas para envio) | hml, prd |
| 7 | relatório da execução (task 4): o motor ainda **não grava** em `mlops_fabrica_ia.operacao.controle_carga`, que o card do Teams lê — definir se o runner usa o `LoadControlTracker` da plataforma | — |

## 6. Escopo da Ciência de Dados e divergências com o POP-IA-08

**Escopo desta entrega: dev, até a tabela de saída e a view validadas.** hml e prd seguem depois da aprovação e do
alinhamento com o Ops.

| artefato (POP-IA-08: o que a Ciência de Dados edita) | estado em dev |
|---|---|
| config da linha (`ntb_ia_biopsia_cancer_colon_config`) | ✅ |
| definição do job (`biopsia-cancer-colon-batch.json`) | ✅ (o job só existe após a esteira) |
| arquivo de navegação (`config/exchange/dev/ntb_ia_biopsia_cancer_colon_navegacao`): mesmas 108 unidades e `cap:lin:cancer-colon` do Ca de Cólon, `projeto` próprio | ✅ |
| tabela de saída, com idempotência | ✅ |
| view de exportação, sem nulo em paciente e exame (critério de saída do POP) | ✅ validada com o filtro de data trocado |

| ponto do POP-IA-08 | divergência | encaminhamento |
|---|---|---|
| saída de dev: "as **três tabelas** e a view validadas" | o motor grava só a **saída**; a entrada é a própria tabela do Motion (o funil por etapa vai na saída da task) e não há tabela de **monitoramento** | Ops decide se o runner grava entrada e monitoramento (junto do controle de carga, item 7 da §5) |
| a view mostra o que o motor processou | desde 14/10/2026 (`2a93fb1`) a view só mostra as datas de `tb_cad_pipeline_configuracao_envio_nlp_platform`, alimentada fora do repositório; **não há linhas de dev para nenhum projeto**, então a view de dev de toda linha sai vazia. O POP não menciona esse cadastro | responsabilidade do Ops (item 6 da §5); registrar no POP e no guia de linha nova |
| especialidade = config do **motor NLP** | segundo runner não previsto em POP nem em guia de uso | decisão 1 da §7 |

## 7. Decisões para o alinhamento com o Ops

1. Motor de regras estruturadas como 2º runner da plataforma — este protótipo é a proposta concreta.
2. Origem Motion × NLP na Tábia: campo **`origem_identificacao`** do formulário do evento "Avaliação pertinência do
   laudo" (lista: `biopsia_motion` · `achado_exame`). É preenchido pelo job `central-captacao-envio-tabia`
   (`fabrica-ia-central-captacao-app`, `envio_tabia/config_tabia.py`), que lê a **view** de cada projeto e hoje grava
   `achado_exame` fixo para todos (`CAMPOS_ENUM_PADRAO`), só quando o campo está vazio. Para a linha de biópsias:
   projeto `biopsia_cancer_colon` em `projetos.py` (view da linha, `cap:lin:cancer-colon`), em `config_tabia.LINHAS`
   com o mesmo programa/pathway do Ca de Cólon (620 · "1. Captação cirúrgica") e o padrão `biopsia_motion` por linha
   (hoje o padrão é global). Mudança pequena, no repositório da Central.
3. "Já navegado → anotar": regra na Central ou campo novo no payload.
4. Paciente só do RC (perde ~7% dos pedidos) ou também do cadastro do Motion.
5. Randomização nesta linha.
6. Fonte do Motion: leitura direta de `especialistas.gold` (hoje) ou ingestão pelo Datahub.

## 8. Como rodar

```text
job:        jobs/definicoes/biopsia-cancer-colon-batch.json
manual:     plataform/ntb_ia_motor_regras_e2e  specialty=biopsia_cancer_colon  environment=dev  persist=false
paridade:   python scripts/structured_rules/paridade_v3.py <consulta-v3.sql> <AAAA-MM-DD>  → executar no warehouse
testes:     python -m pytest plataform/tests/structured_rules -q
```

Linha nova do mesmo formato (estômago, próstata) é **só config**. Se exigir código, o desenho volta à SPEC.
