# Pontos de Alinhamento Pre-Codigo

**Natureza:** checklist vivo -- atualizar conforme decisoes forem tomadas
**Objetivo:** consolidar todos os pontos pendentes de definicao antes e durante o inicio do desenvolvimento
**Ultima atualizacao:** 2026-03-31

---

## 1. Bloqueantes Sprint 2

Estes pontos impedem o inicio efetivo do desenvolvimento. Precisam de resposta antes de comecar S00 (setup de repos).

### B1 -- Feed interno para publicacao de wheels

| Campo | Valor |
|-------|-------|
| **Descricao** | As 3 libs (`nlp_engine`, `data_manage`, `monitoring`) serao empacotadas como wheels. Onde publicar para que `%pip install` funcione nos notebooks Databricks? |
| **Opcoes** | Azure Artifacts (feed PyPI privado) / Databricks workspace files / PyPI interno corporativo / DBFS |
| **Recomendacao** | Azure Artifacts (feed PyPI) -- integra com Azure DevOps, suporta versionamento semver, compativel com `%pip install --index-url` |
| **Responsavel** | MLOps |
| **Impacto** | S00 inteira (T00.5, T00.6) |
| **Ref.** | `anexo03-historias-e-tasks-v0.md` (S00) |
| **Status** | `pendente` |

---

### B2 -- Gestao de dependencias (confirmar abordagem)

| Campo | Valor |
|-------|-------|
| **Descricao** | Como instalar as 3 libs nos notebooks: `requirements.txt` centralizado vs `%pip install` individual por notebook vs cluster library |
| **Opcoes** | A: `requirements.txt` centralizado / B: `%pip install` por notebook / C: cluster library |
| **Recomendacao** | Opcao A para Fase 1 (centralizado, DRY, canary release para cenarios transitorios). Analise completa no doc dedicado. |
| **Responsavel** | DS + MLOps |
| **Impacto** | Template do notebook (T05.1), padrao de todas as especialidades |
| **Ref.** | `doc-gestao-dependencias-libs-v0.md` |
| **Status** | `pendente` |

---

### B3 -- Formato de notebook

| Campo | Valor |
|-------|-------|
| **Descricao** | Os notebooks orquestradores no Databricks serao `.py` (export Databricks source) ou `.ipynb`? Afeta o template `_template/ntb_motor` e como o CI faz sync. |
| **Opcoes** | `.py` (Databricks source format, mais leve, diff amigavel) / `.ipynb` (celulas, mais visual no Databricks, diff ruidoso) |
| **Recomendacao** | `.py` (source format) -- diff limpo, CI simples, consistente com hepato e birads atuais |
| **Responsavel** | DS + MLOps |
| **Impacto** | Template (T05.1), CI (sync), padrao de novos projetos |
| **Ref.** | `07-relatorio-final-v0-plataforma-nlp-clinica.md` (decisao E) |
| **Status** | `pendente` |

---

### B4 -- Runtime Python (confirmar versao)

| Campo | Valor |
|-------|-------|
| **Descricao** | A decisao de runtime unificado esta tomada, mas a versao exata precisa de confirmacao. Se houver restricao de cluster (ex: imagem base so suporta ate 3.11), impacta as 3 libs. |
| **Opcoes** | 3.12 (recomendado, ja usado em hepato/birads/pulmao/rim) / 3.11 (se houver restricao) / 3.10 (fallback conservador) |
| **Recomendacao** | 3.12 -- ja e o runtime da maioria dos pipelines atuais |
| **Responsavel** | MLOps (confirmar compatibilidade de cluster) |
| **Impacto** | Todas as 3 libs (pyproject.toml / setup.cfg), CI, testes |
| **Ref.** | `07-relatorio-final-v0-plataforma-nlp-clinica.md` (decisao H), `05-roadmap-entregas-sprint-v0.md` (L22) |
| **Status** | `pendente` |

---

## 2. Paralelos (resolver na 1a semana)

Estes pontos podem ser definidos em paralelo com o inicio do desenvolvimento. Nao bloqueiam S00/S01/S02, mas bloqueiam tasks especificas.

### P1 -- CI/CD gates (ferramenta de lint)

| Campo | Valor |
|-------|-------|
| **Descricao** | S00 preve "CI minimo (lint)" nos 3 repos. Qual ferramenta? Como integrar no Azure Pipelines? |
| **Opcoes** | ruff (rapido, moderno, substitui flake8+isort+pyupgrade) / flake8 (classico) / pylint (completo mas lento) |
| **Recomendacao** | ruff -- rapido, configuravel, comunidade ativa, compativel com pre-commit |
| **Responsavel** | DS (escolha) + MLOps (integracao no pipeline) |
| **Impacto** | T00.1-T00.3 (CI dos repos), S11 (gates completos na Fase 2) |
| **Ref.** | `07-relatorio-final-v0-plataforma-nlp-clinica.md` (decisao D), `anexo03-historias-e-tasks-v0.md` (S11) |
| **Status** | `pendente` |

---

### P2 -- Unity Catalog naming (ADR)

| Campo | Valor |
|-------|-------|
| **Descricao** | Hoje coexistem `ia` e `diamond_*` como prefixos de tabelas. O motor novo vai criar tabelas (ex: `ia.tb_diamond_mod_metricas_qualidade`). Sem convencao, risco de retrabalho. |
| **O que precisa** | ADR (Architecture Decision Record) ou convencao informal: `{env}.{schema}.{prefixo}_{specialty}_{stage}` |
| **Responsavel** | DS + MLOps + Head |
| **Impacto** | T04.4 (tabela de metricas), T05.2 (integracao Databricks) |
| **Ref.** | `07-relatorio-final-v0-plataforma-nlp-clinica.md` (sec 12, risco "Nomenclatura fragmentada") |
| **Status** | `pendente` |

---

### P3 -- Dono clinico para hepato (governanca)

| Campo | Valor |
|-------|-------|
| **Descricao** | A governanca clinica (S08) define fluxo "DS propoe -> medico valida -> deploy". Precisa de pelo menos 1 responsavel clinico para hepato (piloto). |
| **O que precisa** | Nome do responsavel clinico que valida mudancas de regra/config para hepatologia |
| **Responsavel** | Head DS + lideranca clinica |
| **Impacto** | S06 (validacao paralela), S08 (governanca clinica) |
| **Ref.** | `07-relatorio-final-v0-plataforma-nlp-clinica.md` (sec 10), `04-visao-refinada-motor-nlp-unificado-v0.md` (configs: `owner_clinical: "a definir"`) |
| **Status** | `pendente` |

---

### P4 -- Schema de I/O versionado (contrato)

| Campo | Valor |
|-------|-------|
| **Descricao** | O relatorio menciona `clinical_nlp_io_v1` como contrato de dados, mas nao ha definicao formal. T02.1 (interface `ClinicalNlpEngine`) precisa saber quais campos sao obrigatorios no input/output. |
| **O que precisa** | Definir: campos de entrada (quais colunas do Delta?), campos de saida (`fl_relevante`, `confidence_score`, `exm_laudo_resultado`, metadados), tipos, campos opcionais por especialidade |
| **Responsavel** | DS (propor) + MLOps (validar compatibilidade com serving layer) |
| **Impacto** | T02.1 (interface), T02.4 (quality_guard), T05.1 (notebook) |
| **Ref.** | `07-relatorio-final-v0-plataforma-nlp-clinica.md` (sec 8.3), `anexo03-historias-e-tasks-v0.md` (S02 AC) |
| **Status** | `pendente` |

---

## 3. Futuro (sem urgencia)

Pontos ja mapeados que serao resolvidos em fases posteriores. Listados aqui para rastreabilidade.

| ID | Ponto | Fase | Responsavel | Ref. |
|----|-------|------|-------------|------|
| F1 | Cluster GPU (tipo VM, policy, scheduling) | 3 | MLOps | Decisao G no relatorio |
| F2 | MLflow experiment naming + model registry | 3 | DS + MLOps | Decisao D5 no roadmap |
| F3 | Imagem de cluster padronizada (init script / container) | 2+ | MLOps | Decisao D7 no roadmap |
| F4 | Orquestracao avancada (LangGraph / Prefect / Workflows) | 4 | DS + MLOps | Premissa operacional |
| F5 | LLM fallback seletivo (API, anonimizacao, custo) | 4 | DS + Head | S19 no anexo03 |
| F6 | NER clinico (entidades anatomicas, procedimentos) | 4 | DS | S20 no anexo03 |
| F7 | Monitoramento de drift (KS test, alertas) | 4 | DS | S18 no anexo03 |

---

## 4. Historico de decisoes

Registrar aqui cada ponto resolvido com data e decisao tomada.

| Data | ID | Decisao | Decidido por |
|------|----|---------|--------------|
| --- | --- | --- | --- |
| | | *(preencher conforme evolucao)* | |

---

## 5. Resumo de status

| Categoria | Total | Pendente | Em discussao | Resolvido |
|-----------|-------|----------|--------------|-----------|
| Bloqueantes (B) | 4 | 4 | 0 | 0 |
| Paralelos (P) | 4 | 4 | 0 | 0 |
| Futuro (F) | 7 | 7 | 0 | 0 |
| **Total** | **15** | **15** | **0** | **0** |

---

*Proximo passo: alinhar B1-B4 com MLOps para desbloquear Sprint 2.*
