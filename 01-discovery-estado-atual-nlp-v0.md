# Discovery do estado atual NLP v0

## Escopo e evidencias

Documento de discovery com base em analise estatica dos repositorios em `algoritmos`, com foco em:

- status operacional (ativo vs legado),
- lineage por especialidade (origem -> processamento -> saida -> consumo),
- governanca clinica e aprovacao,
- baseline provisoria de SLO/SLA e runtime para fase 0.

Evidencias principais:

- [algoritmos/hepatologia/azure-pipelines.yml](\hepatologia\azure-pipelines.yml)
- [algoritmos/DII/azure-pipelines.yml](\DII\azure-pipelines.yml)
- [algoritmos/birads/azure-pipelines.yml](\birads\azure-pipelines.yml)
- [algoritmos/hepatologia/model/ntb_ia_hepatologia_algoritmo.py](\hepatologia\model\ntb_ia_hepatologia_algoritmo.py)
- [algoritmos/hepatologia/setup/ntb_ia_hepatologia_setup.py](\hepatologia\setup\ntb_ia_hepatologia_setup.py)
- [algoritmos/colon/data/ntb_ia_colon_entrada_v2.ipynb](\colon\data\ntb_ia_colon_entrada_v2.ipynb.py)
- [algoritmos/birads/model/ntb_ia_predicao.py](\birads\model\ntb_ia_predicao.py)

---

## 1) Criterio operacional PRD/HML/Legado (definido para o discovery)

Como hoje existe o contexto de "HML=PRD" sem separacao formal, o criterio pratico adotado foi:

- **ativo_hml_prd**: possui pipeline de entrega (Azure yml com `rdmlops repo update`) e trilha de execucao com `data/model/serving` e escrita de tabela/merge.
- **legado**: arquivo/trilha explicitamente marcado como `legado`/`deprecated`, ou claramente substituido por versao mais nova.
- **indeterminado**: ha codigo de modelo/dados, mas sem evidencia suficiente de deploy/consumo operacional atual.

### Matriz de status por especialidade


| Especialidade   | Status                              | Evidencia tecnica                                                                                                                       |
| --------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| Hepatologia     | ativo_hml_prd                       | `azure-pipelines.yml` com trigger hml/main + fluxo completo `data/model/serving/monitoring` e tabelas `tb_diamond_mod_hepatologia_`*.   |
| Colon           | ativo_hml_prd                       | Cadeia `data/model/serving` com `CREATE TABLE` e extracao; presenca de versoes v2 e arquivo `deprecated` indica operacao com transicao. |
| DII             | ativo_hml_prd (com legado paralelo) | `azure-pipelines.yml` (release/hml/main, py3.9), arquivos `*_legado.ipynb` coexistindo com trilha atual.                                |
| Birads          | ativo_hml_prd (com legado paralelo) | `azure-pipelines.yml` + `model/data/serving/monitoring`, incluindo `ntb_ia_envio_api.py`; arquivo `ntb_ia_views_legado.py`.             |
| Pulmao          | ativo_hml_prd                       | `azure-pipelines.yml` + pipeline data/model/monitoring com `tb_diamond_mod_pulmao*`.                                                    |
| Rim             | ativo_hml_prd                       | `azure-pipelines.yml` + data/model/monitoring com tabelas `tb_diamond_creatinina_*` e `tb_diamond_nefrologia_clearance_saida`.          |
| Reumatologia    | ativo_hml_prd (com legado paralelo) | `azure-pipelines.yml` (release/hml/main, py3.9) + varios arquivos `deprecated`.                                                         |
| Endometriose    | **descontinuado**                   | Projeto descontinuado conforme alinhamento com lideranca. Pipeline existente sera desativado.                                            |
| Biliar          | **ativo_prd** (entrando nesta sprint) | Grupo 1b (spaCy + embeddings + negacao avancada). Promovido para PRD na sprint atual.                                                 |
| Neuroimunologia | **ativo_prd** (entrando nesta sprint) | Grupo 1b (spaCy + embeddings + negacao avancada). Promovido para PRD na sprint atual.                                                 |


---

## 2) Matriz de lineage por especialidade (v0)

Observacao: em varios notebooks o schema/catalogo e parametrico (`environment`, `catalog`, `schema`, `id_projeto`). A matriz abaixo registra o lineage extraivel com alta confianca do codigo.


| Especialidade   | Origem principal                                                                       | Processamento                                                        | Saida principal                                                                                                                                                                      | Consumo/adaptadores                                                                        |
| --------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| Hepatologia     | `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `tb_gold_mov_paciente`            | NLP regra/spaCy em `model`, extracao/retorno/navegacao em `serving`  | `tb_diamond_mod_hepatologia_entrada`, `tb_diamond_mod_hepatologia_saida`, `vw_diamond_mod_hepatologia`, `tb_diamond_mod_hepatologia_retorno`, `tb_diamond_mod_hepatologia_navegacao` | **Excel/SharePoint** (canal oficial), monitoramento `ia.tb_diamond_mod_monitoramento`, retorno operacional e navegacao |
| Colon           | `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `tb_gold_mov_paciente`            | embeddings + regras, trilhas `algoritmo`/`colonoscopia`, extracao v2 | `tb_diamond_mod_colon_entrada`, `tb_diamond_mod_colon_saida`, `tb_diamond_mod_colonoscopia_saida` (parametrico por env/projeto)                                                      | **Excel/SharePoint** (canal oficial), tabelas de conferencia                               |
| DII             | entradas em notebooks `data` (inclusive trilhas colono e legado)                       | trilha NLP (embeddings+regras) e resquicios de trilha supervisionada | `OUTPUT_TABLENAME` parametrico em `data/model`                                                                                                                                       | extracao legado e extracao atual (`serving`)                                               |
| Birads          | entradas em `ntb_ia_entrada.py` e `ntb_ia_entrada_pacs.py`                             | pattern extraction (regex/regras) em `model/ntb_ia_predicao.py`      | tabela de saida BI-RADS (parametrica), tabelas auxiliares de logs/complemento                                                                                                        | **API** (canal oficial birads), views (`ntb_ia_views.py`) e monitoramento/log analytics    |
| Pulmao          | `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `tb_gold_mov_paciente`            | embeddings + regras em `model/ntb_ia_predicao.ipynb`                 | `tb_diamond_mod_pulmao`, `tb_diamond_mod_pulmao_saida`, `tb_diamond_mod_pulmao_saida_salesforce`, `tb_diamond_mod_pulmao_metricas`                                                   | **Excel/SharePoint** + trilha Salesforce, monitoramento                                    |
| Rim             | `gold_corporativo.observation.tb_gold_mov_observation_exames` (e tabelas corporativas) | regras de negocio e calculo clearance                                | `ia.tb_diamond_creatinina_wrk_*`, `ia.tb_diamond_nefrologia_clearance_saida`                                                                                                         | monitoramento em `ia.tb_diamond_mod_monitoramento`                                         |
| Reumatologia    | entradas em notebooks `data` (incluindo `entrada_anterior`)                            | NLP em `model` com coexistencia de trilhas deprecated                | `OUTPUT_TABLENAME` parametrico + tabelas temporarias antigas (`ia.tbl_tmp_reumatologia_algoritmo`)                                                                                   | extracao atual e extracoes deprecated                                                      |
| Endometriose    | ~~descontinuado~~                                                                      | ~~descontinuado~~                                                    | ~~descontinuado~~                                                                                                                                                                     | ~~descontinuado~~                                                                          |
| Biliar          | entradas em notebook `data`                                                            | NLP Grupo 1b (spaCy + embeddings + negacao avancada)                 | `OUTPUT_TABLENAME` parametrico                                                                                                                                                       | extracao em `serving` — **entrando em PRD nesta sprint**                                   |
| Neuroimunologia | entradas em notebook `data`                                                            | NLP Grupo 1b (spaCy + embeddings + negacao avancada)                 | `OUTPUT_TABLENAME` parametrico                                                                                                                                                       | **entrando em PRD nesta sprint**                                                           |


---

## 3) Governanca clinica e aprovacao (estado atual)

### O que esta evidenciado

- Existem responsaveis tecnicos listados em alguns READMEs (ex.: colon, biliar, neuroimunologia, rim).
- Parte das regras clinicas esta embutida em codigo/notebooks (listas, regex, thresholds, filtros SQL, regras de relevancia).
- Ha monitoramento tecnico em varias linhas (`ia.tb_diamond_mod_monitoramento` e scripts de analise de logs).

### Lacunas criticas (sem evidencias formais no codigo)

- Dono clinico oficial por especialidade (RACI claro).
- Fluxo de aprovacao de mudanca clinica (quem aprova, em qual comite, periodicidade).
- Registro formal de versao de regra clinica por release (alem do Git tecnico).
- Criterios de aceite clinico padronizados por especialidade.

### Fluxo pragmatico recomendado

Em vez de RACI corporativo ou comite formal, adotar fluxo simples e executavel:

1. **DS propoe** mudanca de regra clinica (PR no Git com diff do YAML de config)
2. **Medico valida** (review funcional — pode ser a DS Jr medica do time)
3. **Deploy versionado** (Git tag + merge; campo `config_version` registrado no output de cada execucao)

Rastreabilidade garantida por: YAML versionado no Git + `config_version` no output + historico de PRs. Sem burocracia excessiva neste estagio.

---

## 4) Baseline provisoria SLO/SLA e politica de runtime (fase 0)

## 4.1 SLO/SLA minimo (provisorio)

Sem runbook operacional oficial no repo, baseline sugerido para nao travar a convergencia:

- **latencia lote diaria**: concluir execucao ate janela operacional acordada com navegacao.
- **reprocesso**: capacidade de rerun por data (`data_execucao_modelo`) sem corromper historico.
- **disponibilidade**: sucesso diario monitorado por tabela de monitoramento e alerta de falha.
- **qualidade tecnica**: schema valido e taxa de linhas processadas acima de limite acordado por linha.

## 4.2 Runtime/dependencias (evidencia atual)

- Python 3.12: hepatologia, birads, pulmao, rim.
- Python 3.9: DII, reumatologia, endometriose.
- Sem CI formal (azure-pipelines.yml ausente): colon, biliar, neuroimunologia.
- CI atual entrega conteudo ao Databricks via `rdmlops repo update`, sem gate de testes de regressao clinica na pipeline.

### Decisao recomendada para fase 0

- Congelar matriz de compatibilidade por especialidade e definir estrategia unica com EngML:
  - opcao A: convergir para runtime unico (preferencial),
  - opcao B: manter dual runtime temporario com data de deprecacao.
- Incluir gate minimo em CI da biblioteca comum (lint + testes unitarios de pipeline textual).

---

## 5) Respostas objetivas as perguntas pendentes

### Ja conhecemos profundamente o fluxo de cada algoritmo?

**Ainda nao 100%.** Temos boa cobertura tecnica de pipeline e tabelas, mas faltam evidencias oficiais para:

- status PRD formal por job,
- lineage operacional validado com Jobs Databricks,
- governanca clinica formalizada (aprovadores e criterios),
- SLO/SLA acordado por linha.

### Conseguimos extrair lineage a partir dos algoritmos?

**Sim, parcialmente e com boa base tecnica.** O codigo permite extrair boa parte de origem/processamento/saida, mas para "lineage oficial" ainda precisa cruzar com:

- Jobs reais ativos no Databricks,
- consumidores finais de cada tabela/view,
- ownership funcional e operacional.

### SLA/SLO, metrica de negocio e compliance sao fundamentais agora?

- **SLO/SLA minimo**: sim, necessario para nao gerar proposta sem viabilidade operacional.
- **metricas de negocio** (captacao/conversao/concordancia): sim, ao menos baseline inicial por especialidade para orientar o piloto.
- **compliance contratual de integracoes**: necessario mapear o minimo na fase 0 para evitar bloqueio posterior (especialmente API/FTP/export).

---

## 6) Prioridade executiva (alinhada ao Head)

Piloto oficial da convergencia:

- **Hepatologia legado vs Hepatologia motor novo**.

Justificativa:

- fluxo completo e bem evidenciado no codigo (entrada, modelo, extracao, retorno, navegacao, monitoramento),
- bom candidato para medir paridade funcional e risco clinico antes de expandir para outras especialidades.

