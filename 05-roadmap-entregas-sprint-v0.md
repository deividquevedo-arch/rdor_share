# Plano de Acao — Motor NLP Unificado (v0.2)

**Data:** 2026-03-31
**Sprints:** 10 dias uteis cada
**Piloto:** Hepatologia (sem alterar PRD)
**Historia Sprint 1:** Estudo do esqueleto NLP + oportunidades de melhoria

---

## Premissas operacionais

| Aspecto | Posicao |
|---------|---------|
| **Canal de saida** | Excel / SharePoint / OneDrive e o canal oficial de consumo operacional atual e permanece inalterado. O motor alimenta Delta; o serving layer existente (extracao, API, FTP) continua consumindo normalmente. Evolucao para API/Delta como canal primario e progressiva e nao e pre-requisito. |
| **Orquestracao** | SQL + Jobs Databricks (pipeline linear agendada) e o padrao atual. Nao ha reescrita de orquestracao. Ferramentas como LangGraph, Prefect ou Airflow sao opcoes futuras, nenhuma e pre-requisito. |
| **Clusters** | Fases 1-2 rodam inteiramente em CPU nos clusters atuais. GPU so e necessaria a partir da Fase 3 (encoder). Definicao de cluster GPU a alinhar com EngML/MLOps quando pertinente. |
| **Posicionamento** | Esta proposta e uma **evolucao controlada com coexistencia**, nao substituicao. Pipelines atuais continuam rodando em PRD. O motor novo roda em paralelo para validacao. So substitui apos evidencia de paridade ou melhoria. |

---

## 1. Lacunas encontradas na auditoria do esqueleto

### 1.1 NLP e assertividade

| # | Lacuna | Onde | Evidencia | Risco |
|---|--------|------|-----------|-------|
| L1 | Negacao simplificada: apenas 2 expressoes ("ausencia", "nao ha"), janela de 3 tokens | Hepato | `ntb_ia_hepatologia_algoritmo.py` — `matcher_irrelevants` | Falso positivo: achado negado conta como positivo |
| L2 | Sem limpeza RTF/HTML robusta | Hepato | Nao usa `to_plain()`; Grupo 1b tem deteccao RTF/HTML/plain com pypandoc fallback, ftfy, NFKC | Ruido na entrada degrada matching |
| L3 | Processamento row-by-row (`.apply`) sem batch | Hepato | Fluxo inteiro em Pandas `.apply` | Performance degradada em volumes altos |
| L4 | Imports mortos: gensim, CountVectorizer, fuzzywuzzy | Hepato | Importados no topo, sem uso no fluxo principal | Codigo morto, confusao tecnica |
| L5 | Zero deteccao de negacao | Birads | Linha comentada `#.replace('negativo', ' 9 ')` — intencao nao implementada | "BI-RADS 4 descartado" conta como categoria 4 |
| L6 | Funcao `laudo_rtf_to_text` definida mas nao aplicada | Birads | Funcao existe no codigo, nao e chamada no pipeline | Laudos RTF nao sao limpos |
| L7 | `eval(str(...))` para flatten de listas | Birads | Logica fragil com risco de runtime error | Fragilidade em producao |
| L8 | CONFIG duplicado entre 3 notebooks | Biliar/Neuroimuno/Reumato | Mesma mega-CONFIG copiada; cada notebook ativa apenas `TARGET_ORGAN` | Divergencia quando um notebook e atualizado e os outros nao |
| L9 | Bugs: virgula faltante em listas, nomes herdados de DII/colon em helpers | Neuroimuno | Detectado na analise estatica | Resultados incorretos silenciosos |
| L10 | Treinamento sem holdout real (train=test=dataset inteiro) | DII SVM (legado) | `old(first)/DII/` — split comentado, `class_weight=None` comentado | Metricas reportadas com overfitting; legado ja abandonado |

### 1.2 Metricas e observabilidade

| # | Lacuna | Estado atual | Impacto |
|---|--------|-------------|---------|
| L11 | Nenhum pipeline calcula metricas de qualidade NLP automaticamente | Nao existe precision, recall, F1, FPR em nenhuma execucao | Impossivel medir assertividade objetivamente |
| L12 | Monitoramento registra execucao, nao qualidade | `ia.tb_diamond_mod_monitoramento` grava datas e contagens (MERGE) | Sabe que rodou, nao sabe se acertou |
| L13 | Sem baseline de metricas por especialidade | Nenhum documento com precision/recall de referencia | Sem baseline, impossivel medir melhoria |
| L14 | Sem monitoramento de drift | Nao ha comparacao de distribuicao de scores entre janelas temporais | Degradacao silenciosa ao longo do tempo |
| L15 | Confianca binaria (0.35 ou 0.90) | Grupo 1b atribui 0.9 se nao negado, 0.35 se negado | Sem granularidade para priorizacao operacional |

### 1.3 Logs e auditoria

| # | Lacuna | Estado atual | Impacto |
|---|--------|-------------|---------|
| L16 | Sem `config_version` ou `engine_version` nos outputs | Nenhum campo identifica qual versao de regras/engine gerou o resultado | Impossivel rastrear causa de mudanca de comportamento |
| L17 | Sem trilha de auditoria de regra clinica por release | Versao de regra so rastreavel pelo historico Git | Dificil auditar mudancas clinicas |
| L18 | Logs detalhados apenas no Birads | Birads tem `ntb_ia_log_analytics`; demais pipelines nao | Observabilidade desigual entre especialidades |

### 1.4 Organizacao de codigo

| # | Lacuna | Estado atual | Impacto |
|---|--------|-------------|---------|
| L19 | Tudo em notebooks Databricks | `.py` exportado ou `.ipynb`; logica de negocio misturada com orquestracao e DDL | Impossivel testar unitariamente; reutilizacao zero |
| L20 | CONFIG hardcoded dentro dos notebooks | Listas clinicas, thresholds, orgaos embutidos no codigo | Alterar regra exige mexer no codigo |
| L21 | CI faz apenas sync de codigo | Azure DevOps com `rdmlops repo update`; sem lint, sem testes, sem gate de qualidade | Qualquer push vai direto ao workspace |
| L22 | Runtime divergente | Python 3.9 (DII, reumatologia, endometriose) vs 3.12 (hepato, birads, pulmao, rim); colon, biliar e neuroimuno sem CI formal | Impede biblioteca unica sem alinhamento |

### 1.5 Governanca clinica

| # | Lacuna | Impacto |
|---|--------|---------|
| L23 | Sem dono clinico oficial por especialidade | Ninguem responde formalmente por mudanca de regra |
| L24 | Sem fluxo de aprovacao de mudanca clinica | Mudanca tecnica pode alterar comportamento clinico sem revisao |
| L25 | Sem criterios de aceite clinico padronizados | Cada especialidade valida de forma diferente (ou nao valida) |

---

## 2. Plano de acao

### A1 — Estruturar motor rule-based unificado (Fase 1 / MVP)

**O que:** criar biblioteca Python `clinical_nlp_engine/` extraindo o melhor codigo existente, **sem dependencia de embeddings ou GPU**.

**Estrategia NLP do MVP (apenas rule-based):**
- **Limpeza textual:** `to_plain()` com deteccao RTF/HTML/plain, pypandoc fallback, ftfy, normalizacao NFKC, remocao boilerplate OCR (extraido do Grupo 1b)
- **Tokenizacao/segmentacao:** spaCy `pt_core_news_lg` com sentencizer `\n` + segmentacao por headers/ancoras de orgao
- **Negacao avancada:** 23 expressoes, janela 7 tokens, frases multi-token, singleton ban (extraido do Grupo 1b)
- **Matching:** regex accent-tolerant com word boundaries + filtros de proximidade (finding <= 200 chars de orgao)

**O que NAO entra na Fase 1:** SentenceTransformer, embeddings, encoder, GPU, treinamento.

**Estrutura do pacote:**
```
clinical_nlp_engine/
  text_pipeline.py    — limpeza, normalizacao, segmentacao, negacao
  engine.py           — interface ClinicalNlpEngine + implementacao rule-based
  config_loader.py    — carregamento e validacao de YAML
  scoring.py          — score de confianca e priorizacao
  quality_guard.py    — validacao de output contra schema
```

**Resolve lacunas:** L1, L2, L3, L4, L6, L7, L8, L9, L19, L20

### A2 — Adicionar embeddings existentes como melhoria (Fase 2)

**O que:** encapsular SentenceTransformer `paraphrase-multilingual-MiniLM-L12-v2` (118M params) no motor como componente opcional de expansao semantica.

**Por que na Fase 2 e nao na Fase 1:** o MVP deve provar o padrao (lib + YAML + metricas) antes de adicionar dependencia de modelo pre-treinado. O valor da Fase 1 e a unificacao e padronizacao, nao a tecnica NLP avancada.

**Por que este modelo:** ja esta em uso nos Grupos 1b e 2 (biliar, neuroimuno, reumato, colon, DII, pulmao). Zero friction de adocao. Validado em producao.

**Como entra no motor:** `text_pipeline.py` carrega o modelo sob demanda; `semantic_expand()` usa para calcular similaridade entre seeds e vocabulario do laudo. Ativado via flag no YAML (`use_embeddings: true`).

**Objetivo de evolucao (Fase 3):** evoluir para BERTimbau (`neuralmind/bert-base-portuguese-cased`) com continued pre-training + fine-tuning como encoder compartilhado com specialty heads, condicionado a validacao das Fases 1-2.

**Resolve lacunas:** melhora assertividade do hepato (que hoje nao usa embeddings)

### A3 — Padronizar logs e metricas

**O que existe hoje:**
- `ia.tb_diamond_mod_monitoramento` — MERGE com datas e contagens de execucao
- Birads: `ntb_ia_log_analytics` com logs detalhados
- Demais: sem logs de qualidade

**O que implementar no motor (Fase 1):**

| Campo | Descricao | Resolve |
|-------|-----------|---------|
| `config_version` | Versao semver do YAML de config usado | L16, L17 |
| `engine_version` | Versao semver do pacote clinical_nlp_engine | L16 |
| `confidence_score` | Score continuo (0.0-1.0) ao inves de binario 0.35/0.90 | L15 |
| `total_laudos` | Contagem de laudos processados por execucao | L12 |
| `total_relevantes` | Contagem de fl_relevante=1 por execucao | L12 |
| `taxa_relevancia` | total_relevantes / total_laudos | L13 |
| `metricas_qualidade` | precision, recall, F1 (quando amostra gold disponivel) | L11, L13 |
| `dt_execucao` | Timestamp da execucao | ja existe |
| `specialty_id` | Identificador da especialidade | padronizacao |

**Tabela nova:** `ia.tb_diamond_mod_metricas_qualidade` com schema acima, gravada a cada execucao do motor.

**Resolve lacunas:** L11, L12, L13, L15, L16, L17, L18

### A4 — Externalizar configuracao clinica em YAML

**O que:** mover CONFIG (listas, thresholds, orgaos, findings) de dentro dos notebooks para arquivos YAML versionados no Git.

**Formato:** 1 arquivo por especialidade (`configs/hepatologia.yaml`, `configs/biliar.yaml`, etc.)

**Conteudo do YAML:**
- `specialty_id`, `version`, `status`
- `preprocessing`: segmentacao, orgaos-alvo, janela de negacao
- `clinical_taxonomy`: keywords, findings por categoria, exclusoes
- `postprocessing`: regras deterministicas, pesos de scoring

**Resolve lacunas:** L8, L20, L17 (versionamento de regra clinica via Git tag)

### A5 — Organizar codigo (notebooks -> lib + notebooks finos)

**Hoje:**
- Notebooks Databricks com 500-1500+ linhas misturando logica NLP, DDL, widgets, exports
- Azure DevOps faz `rdmlops repo update` (sync sem testes)

**Proposta:**
- Logica NLP na biblioteca `clinical_nlp_engine/` (testavel com pytest, sem Spark)
- Notebooks Databricks ficam com ~50 linhas: widgets -> carregar config -> chamar motor -> gravar Delta
- Serving layer existente (extracao Excel/SharePoint/API) continua inalterado
- CI: adicionar lint + testes unitarios da lib como gate antes do sync

**Decisao pendente com EngML:** lib no mesmo repo (pasta) ou repo dedicado com wheel no feed interno

**Resolve lacunas:** L19, L21

### A6 — Validar motor com hepato (sem alterar PRD)

**O que:** rodar motor novo em paralelo com pipeline atual de hepato. Comparar outputs.

**Como:**
1. Pegar amostra de laudos ja processados pelo pipeline atual (com `fl_relevante` conhecido)
2. Processar mesma amostra com motor novo
3. Comparar output por output: concordancia, divergencias, melhorias

**Metricas de validacao:**
- Concordancia geral (% de match exato em fl_relevante)
- Casos onde motor novo detecta relevancia que o atual perdeu (melhoria de recall)
- Casos onde motor novo remove falso positivo (melhoria de precision)
- Se disponivel amostra gold (validacao medica): precision, recall, F-beta(2)

**Potencial:** se stack permitir, validar tambem com colon ou biliar (Grupo 1b/2, mesma base de codigo)

**Resolve lacunas:** L13 (cria baseline de metricas)

### A7 — Coletar dados rotulados para treinamento futuro

**O que:** inventariar todos os dados disponiveis para treinar/fine-tunar modelos em fases futuras.

**Tipos de dados:**
- Lotes validados pelo corpo medico (centenas, validacao parcial) — gold standard
- Outputs do sistema atual (fl_relevante + achados por laudo) — pseudo-labels
- Laudos nao-rotulados (volume potencialmente grande) — para continued pre-training

**Acao Fase 1:** catalogar (nao treinar). Levantar por especialidade: volume, formato, localizacao, nivel de validacao.

**Resolve lacunas:** prepara Fase 3 (fine-tuning)

### A8 — Governanca clinica pragmatica

**Fluxo simples e executavel:**

1. **DS propoe** mudanca de regra clinica (PR no Git com diff do YAML)
2. **Medico valida** (review funcional — pode ser a DS Jr medica do time)
3. **Deploy versionado** (Git tag + merge; `config_version` registrada no output)

**Sem burocracia:** nao exige comite formal nem RACI corporativo neste estagio. O versionamento YAML + Git tag + campo `config_version` no output garante rastreabilidade.

**Resolve lacunas:** L23, L24, L25

---

## 3. Roadmap por fase

| Fase | Sprint(s) | Entrega principal | Infra | Validacao | Dependencia |
|------|-----------|-------------------|-------|-----------|-------------|
| **1 — MVP rule-based** | 1-2 | Lib `clinical_nlp_engine/` com TextPipeline + engine rule-based + YAML hepato + metricas + validacao paralela | **CPU** (clusters atuais) | Motor novo vs pipeline atual em amostra de laudos | Nenhuma (nao altera PRD) |
| **2 — NLP avancado** | 3-4 | Embeddings existentes (MiniLM) como componente opcional + configs biliar/neuroimuno + testes pytest + baseline metricas por especialidade | **CPU** (clusters atuais) | Paridade por especialidade + melhoria mensuravel com embeddings | Decisao runtime e repo com EngML |
| **3 — Encoder** | 5-7 | Continued pre-training BERTimbau + weak supervision + fine-tuning head hepato + validacao comparativa (encoder vs regras vs medico) | **GPU** (a definir com EngML) | F-beta(2) >= baseline com p < 0.05 (McNemar) | GPU cluster disponivel + dados catalogados |
| **4 — Excelencia** | 8+ | Heads por especialidade + calibracao + monitoramento drift + LLM fallback seletivo | GPU + API LLM | Metricas por especialidade + monitoramento continuo | Model Registry ativo |

---

## 4. Decisoes pendentes com EngML

| # | Decisao | Opcoes | Quando |
|---|---------|--------|--------|
| D1 | Runtime Python unico | Convergir para 3.12 (recomendado) vs manter dual | Fase 1 |
| D2 | Repositorio da lib | Pasta no monorepo vs repo dedicado com wheel | Fase 1 |
| D3 | CI/CD gates | Adicionar lint + testes unitarios como pre-req de sync | Fase 2 |
| D4 | Cluster GPU para treinamento | Tipo de VM, policy, scheduling | **Fase 3** (nao antes) |
| D5 | MLflow: experiment naming + model registry | Padrao de nomenclatura e retencao | Fase 3 |
| D6 | Unity Catalog: naming de tabelas do motor | `{env}.ia.tb_diamond_mod_{specialty}_{stage}` | Fase 2 |
| D7 | Padrao de imagem/init script | Eliminar pip ad hoc com deps fixadas | Fase 2 |
| D8 | Canal de saida oficial | Excel/SharePoint permanece; Delta como fonte intermediaria | Fase 1 (confirmar) |

---

## 5. Resumo visual

```
Fase 1 (Sprint 1-2)     Fase 2 (Sprint 3-4)    Fase 3 (Sprint 5-7)    Fase 4 (Sprint 8+)
MVP rule-based           NLP avancado            Encoder                Excelencia
                         + embeddings

ENTREGA:                 ENTREGA:                ENTREGA:               ENTREGA:
- Lib Python             - MiniLM embeddings     - BERTimbau CPT        - Heads todas esp.
- TextPipeline             (componente opcional)  - Weak supervision     - Calibracao
- Config YAML hepato     - Configs biliar/neuro  - Fine-tuning head     - Drift monitoring
- Negacao avancada       - pytest                  hepato               - LLM fallback
- Metricas qualidade     - Baseline metricas     - Validacao tripla     - Active learning
- Validacao hepato       - CI gates

INFRA: CPU               INFRA: CPU              INFRA: GPU             INFRA: GPU + LLM API
ORQUESTRACAO: SQL + Jobs  (sem mudanca)          (sem mudanca)          (sem mudanca)

PRD inalterado -------------------------------------> PRD so muda apos validacao
Canal Excel/SharePoint inalterado -------------------------------------------->
```

---

*Detalhamento tecnico: `visao-refinada-motor-nlp-unificado-v0.md`*
*Briefing EngML: `resumo-alinhamento-engml-v0.md`*
*Analise de engines: `analise-profunda-engines-nlp-v0.md`*
