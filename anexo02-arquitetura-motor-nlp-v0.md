# Anexo -- Arquitetura Tecnica do Motor NLP

**Vinculado a:** `07-relatorio-final-v0-plataforma-nlp-clinica.md`
**Natureza:** documento tecnico -- como e hoje vs como vai ser

---

## 1. Como e hoje

```
                    REPOSITORIOS INDEPENDENTES (9)
                    ==============================

hepatologia/         colon/           DII/              birads/
  model/               model/           model/             model/
    ntb_*.py            ntb_*.ipynb      ntb_*.py/ipynb     ntb_*.py
    [500-1500 linhas]   [idem]           [idem]             [idem]
    - pip install       - pip install    - pip install      - pip install
    - CONFIG inline     - CONFIG inline  - CONFIG inline    - CONFIG inline
    - limpeza texto     - limpeza texto  - limpeza texto    - limpeza texto
    - negacao (basica)  - negacao (avnc) - negacao (avnc)   - sem negacao
    - spaCy Matcher     - SentTransf     - SentTransf+SVM   - regex+NLTK
    - scoring           - scoring        - scoring          - extracao cat.

pulmao/ biliar/ neuroimuno/ reumatologia/ rim/
  [mesmo padrao: notebook monolitico com tudo dentro]

                              |
                              v
                    Azure Pipelines (rdmlops sync)
                              |
                              v
                    Databricks Workspace
                    - Jobs lineares (SQL trigger)
                    - Entrada: Diamond tables
                    - Saida: Delta -> Serving -> Excel/SharePoint
```

### Caracteristicas

| Aspecto | Estado |
|---------|--------|
| Logica NLP | Dentro dos notebooks (500-1500 linhas cada) |
| Configuracao clinica | Hardcoded (listas, thresholds, orgaos inline) |
| Compartilhamento de codigo | Nenhum (copy-paste entre repos) |
| Negacao | 2 implementacoes: basica (hepato, 2 expr.) e avancada (Grupo 1b, 23 expr.) |
| Dependencias | `pip install` ad hoc no inicio de cada notebook |
| Testes | Inexistentes |
| Metricas NLP | Inexistentes (so contagem de execucao) |
| CI/CD | Sync de codigo para workspace (sem gate) |
| Runtime Python | 3.9 (4 pipelines), 3.12 (5 pipelines), sem CI (3 pipelines) |
| Versionamento de regras | Apenas historico Git (sem tag, sem campo no output) |

---

## 2. Como vai ser

```
                    REPOSITORIO DA LIB (1)
                    ======================

clinical_nlp_engine/
  text_pipeline.py      -- limpeza, normalizacao, segmentacao, negacao
  engine.py             -- interface ClinicalNlpEngine + impls (rule-based, embeddings)
  config_loader.py      -- carregamento e validacao de YAML
  scoring.py            -- score continuo (0.0-1.0)
  quality_guard.py      -- validacao de output contra schema
  semantic_expand.py    -- embeddings MiniLM (Fase 2, opcional)

configs/
  hepatologia.yaml      -- taxonomia, keywords, thresholds, findings
  biliar.yaml
  neuroimunologia.yaml
  reumatologia.yaml
  colon.yaml
  dii.yaml
  pulmao.yaml
  birads.yaml
  rim.yaml

tests/
  test_text_pipeline.py
  test_engine.py
  test_config_loader.py
  test_scoring.py
  fixtures/              -- laudos sinteticos (sem PHI)


                    NOTEBOOKS DATABRICKS (~50 linhas cada)
                    ======================================

ntb_ia_{specialty}_motor.py
  1. dbutils.widgets (ambiente, catalogo, data, specialty_id)
  2. from clinical_nlp_engine import TextPipeline, ClinicalNlpEngine
  3. config = load_config("configs/{specialty}.yaml")
  4. engine = ClinicalNlpEngine(config)
  5. df_input = spark.table(contract.input_table)
  6. df_output = engine.process(df_input)
  7. df_output.write.format("delta").save(contract.output_table)
  8. log_metrics(df_output, config)

                    SERVING (inalterado)
                    ====================
  ntb_ia_{specialty}_extracao.py  -- Excel/SharePoint (como e hoje)
  ntb_ia_{specialty}_retorno.py   -- API, FTP (como e hoje)
```

### Comparativo direto

| Aspecto | Hoje | Motor NLP |
|---------|------|-----------|
| Logica NLP | Dentro do notebook | Biblioteca Python (wheel) |
| Configuracao clinica | Hardcoded | YAML versionado (Git) |
| Compartilhamento | Copy-paste | `import clinical_nlp_engine` |
| Negacao | 2 versoes (basica/avancada) | 1 versao (avancada, compartilhada) |
| Dependencias | `pip install` ad hoc | Wheel com deps fixadas |
| Testes | Nenhum | pytest (frases sinteticas) |
| Metricas NLP | Nenhuma | precision, recall, F1, score continuo |
| CI/CD | Sync only | Sync + lint + testes da lib |
| Runtime Python | 3 faixas | 1 (3.12, alinhado com EngML) |
| Versionamento de regras | Historico Git | `config_version` + `engine_version` no output |
| Notebook | 500-1500 linhas | ~50 linhas |
| Adicionar especialidade | Copiar repo, adaptar | Criar YAML + ativar no motor |

---

## 3. Fluxo de dados (inalterado)

O fluxo de dados **nao muda**. O motor substitui apenas a camada de processamento NLP.

```
Lake 1.0/2.0 --> ETL (repositorios mestre) --> Diamond tables
                                                     |
                                    +----------------+----------------+
                                    |                                 |
                              [HOJE: notebook]              [MOTOR: lib + notebook fino]
                              ntb_ia_hepato_algo.py         ntb_ia_hepato_motor.py
                              (1200 linhas, tudo junto)     (50 linhas, chama a lib)
                                    |                                 |
                                    v                                 v
                              Delta (saida)                     Delta (saida)
                                    |                                 |
                                    +----------------+----------------+
                                                     |
                                    Serving layer (inalterado)
                                    - Extracao -> Excel/SharePoint
                                    - API (birads)
                                    - FTP
                                    - Power BI
```

---

## 4. Contrato de dados

### Entrada (comum a todas as especialidades)

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `id_exame` | string | Identificador unico do exame |
| `id_paciente` | string | Identificador do paciente |
| `id_unidade` | string | Unidade hospitalar |
| `exm_laudo_texto` | string | Texto bruto do laudo (HTML, RTF ou plain) |
| `exm_mod` | string | Modalidade do exame |
| `exm_tipo` | string | Tipo do exame |
| `dt_exame` | date | Data do exame |

### Saida (comum + extensoes por especialidade)

| Campo | Tipo | Descricao | Novo? |
|-------|------|-----------|-------|
| `id_predicao` | string | ID unico da predicao | nao |
| `dt_execucao` | timestamp | Quando o motor processou | nao |
| `specialty_id` | string | Qual especialidade | **sim** |
| `config_version` | string | Versao do YAML usado (semver) | **sim** |
| `engine_version` | string | Versao da lib usada (semver) | **sim** |
| `fl_relevante` | int | 0 ou 1 | nao |
| `confidence_score` | float | 0.0 a 1.0 (granular) | **sim** (era binario) |
| `exm_laudo_resultado` | string/json | Achados estruturados | nao |
| `exm_laudo_texto_tratado` | string | Texto limpo | nao |

---

## 5. Evolucao por fase (visao arquitetural)

### Fase 1-2: Motor rule-based + embeddings existentes (CPU)

```
Laudo (raw) --> [TextPipeline] --> [RuleBasedEngine] --> [PostProcess] --> Delta --> Excel/SharePoint
                     |                    |                   |
              Limpeza/Negacao     spaCy Matcher +       Score/Validacao
              (compartilhado)    regex + config YAML   (QualityGuard)
                                 + MiniLM (Fase 2, opt)
```

### Fase 3-4: Encoder compartilhado + specialty heads (GPU) -- objetivo de evolucao

```
Laudo (raw) --> [TextPipeline] --> [SharedEncoder] --> [SpecialtyHead] --> [PostProcess] --> Delta
                     |                   |                   |                  |
              Limpeza/Neg          BERTimbau           Linear+Sigmoid     Score/Validacao
              (compartilhado)    (fine-tuned)       (por especialidade)   (QualityGuard)
```

So avanca para esta fase com evidencia de ganho mensuravel sobre o baseline rule-based (Fases 1-2).
