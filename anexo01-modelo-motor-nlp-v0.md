# Anexo -- O Motor NLP: o que, como e por que

**Vinculado a:** `07-relatorio-final-v0-plataforma-nlp-clinica.md`
**Natureza:** visao geral do motor NLP unificado proposto, das fases iniciais ao objetivo de evolucao
**Publico:** lideranca tecnica, produto, time de DS, EngML

---

## 1. O que e

Um **motor NLP unico, parametrizavel e versionado** que processa laudos clinicos de qualquer especialidade medica. Hoje existem 9 algoritmos independentes fazendo isso com logica duplicada. O motor consolida o melhor de cada um numa biblioteca Python reutilizavel.

**Componentes centrais:**

- **TextPipeline** -- modulo compartilhado de preparacao de texto: limpeza (HTML, RTF, rodape), normalizacao, segmentacao por secao/orgao, deteccao de negacao (23 expressoes, janela de 7 tokens). E o tronco comum a todas as especialidades.
- **ClinicalNlpEngine** -- interface de processamento clinico. Recebe texto preparado e produz resultado padronizado (relevante/nao relevante, achados, score). A implementacao interna varia conforme a especialidade (regras, embeddings, encoder), mas a interface e unica.
- **Configuracao por especialidade (YAML)** -- taxonomias, listas de termos, thresholds e parametros por especialidade vivem em arquivos YAML versionados no Git. Alterar comportamento clinico = alterar configuracao, nao codigo.

---

## 2. Por que construir

### O problema

Cada algoritmo nasceu de forma independente. O resultado:

- **Duplicacao:** funcoes de limpeza de texto, negacao e scoring replicadas em 7+ notebooks. Um bug corrigido num algoritmo continua nos outros.
- **Fragilidade:** cada notebook faz `pip install` no inicio. Sem imagem padronizada, versoes de dependencias podem mudar entre execucoes.
- **Custo de escala:** criar um novo algoritmo exige copiar um existente, adaptar, testar do zero. Novas especialidades levam dias ao inves de horas.
- **Runtimes divergentes:** 3 versoes de Python (3.9, 3.12, e 3 projetos sem CI formal). Impede biblioteca compartilhada sem alinhamento.
- **Sem metricas padronizadas:** nao ha forma sistematica de medir precision, recall ou F1 por algoritmo. Impossivel saber objetivamente se um mudanca melhorou ou piorou o resultado.

### O valor

- **Manutencao centralizada:** corrigir uma vez, aplicar em todas as especialidades.
- **Onboarding rapido:** nova especialidade = novo YAML + configuracao, nao novo projeto do zero.
- **Metricas comparaveis:** baseline padronizado permite medir evolucao real.
- **Base para evolucao:** com a fundacao solida (Fases 1-2), o motor pode evoluir para tecnicas mais avancadas (Fase 3-4) de forma incremental e validada.

---

## 3. Como funciona

### Fluxo de processamento

```
Laudo clinico (texto bruto)
        |
        v
+----------------------------------+
|       TextPipeline               |
|  Limpeza -> Normalizacao ->      |
|  Segmentacao -> Negacao          |
+----------------------------------+
        |
        v
+----------------------------------+
|    ClinicalNlpEngine             |
|  (implementacao varia por fase)  |
|                                  |
|  Fase 1-2: regras + embeddings  |
|  Fase 3-4: encoder + heads      |
+----------------------------------+
        |
        v
+----------------------------------+
|    Pos-processamento             |
|  Score -> Validacao -> Metadados |
+----------------------------------+
        |
        v
Delta  -->  Serving  -->  Excel/SharePoint (canal oficial)
```

### Principios de design

- **Plugavel:** a implementacao interna do engine muda por fase, mas a interface externa (entrada, saida, configuracao) permanece estavel. O notebook que consome o motor nao precisa mudar quando o engine interno evolui.
- **Configuravel:** comportamento clinico (quais termos buscar, quais achados reportar, qual threshold de relevancia) vive em YAML, nao hardcoded. Alteracoes clinicas nao exigem deploy de codigo.
- **Versionado:** cada execucao registra `specialty_id`, `config_version` e `engine_version`. Permite rastrear qual versao do motor e da configuracao gerou cada resultado.
- **Coexistente:** o motor roda em paralelo com os algoritmos atuais. Nao altera, nao substitui, nao desliga nada em producao. So assume apos validacao de paridade.

---

## 4. Evolucao por fases

### Fase 1 -- MVP rule-based (CPU)

**O que:** extrair o TextPipeline do codigo mais maduro (Grupo 1b), implementar o ClinicalNlpEngine como engine deterministico (regras + spaCy Matcher + regex), criar configs YAML para hepatologia (piloto).

**Como:** codigo Python empacotado como biblioteca (wheel); notebooks Databricks reduzidos a orquestracao minima ("fios de ligacao"); validacao paralela com hepato atual.

**Por que:** prova que a unificacao funciona, sem adicionar nenhuma complexidade nova. Usa apenas o que ja existe, reorganizado. Hepatologia e o piloto ideal: o mais simples dos algoritmos, ganho imediato com negacao avancada que hoje so os outros tem.

**Resultado esperado:** motor rule-based com paridade ou melhoria em relacao ao baseline hepato.

### Fase 2 -- NLP avancado com embeddings existentes (CPU)

**O que:** incorporar SentenceTransformer (MiniLM) como componente opcional do engine. Criar configs para biliar, neuroimunologia e demais especialidades. Adicionar testes automatizados.

**Como:** MiniLM ja esta validado em producao (5 de 9 especialidades usam). Entra como `semantic_expand` -- camada opcional que expande a cobertura de termos por similaridade semantica. Nao substitui regras, complementa.

**Por que:** maximiza cobertura sem adicionar dependencia nova. MiniLM roda em CPU (118M params, leve). Baseline de metricas por especialidade permite medir ganho real.

**Resultado esperado:** motor com embeddings opcionais e paridade confirmada por especialidade.

### Fase 3 -- Encoder compartilhado (GPU) -- objetivo de evolucao

**O que:** treinar um encoder (BERTimbau) adaptado ao vocabulario de laudos clinicos PT-BR. Acoplar specialty heads (camadas de classificacao leves) por especialidade. Validar contra baseline rule-based.

**Como:** 3 etapas de treinamento:
1. **Continued Pre-Training** -- o encoder "le" milhares de laudos nao-rotulados para aprender o vocabulario clinico. Nao precisa de anotacao humana.
2. **Weak Supervision** -- os outputs dos algoritmos atuais servem como "labeling functions" para gerar dados de treino em escala, sem custo de anotacao.
3. **Fine-Tuning** -- com dados validados pelo corpo medico, os specialty heads sao ajustados para cada especialidade.

**Por que:** regras sao deterministicas e auditaveis, mas frageis a variabilidade linguistica (sinonimos, erros de digitacao, variantes regionais). O encoder aprende representacoes que generalizam para variantes nao previstas em listas estaticas. E o caminho de evolucao natural para enderecar limitacoes que regras nao resolvem.

**Condicao:** so avanca se as Fases 1-2 estiverem validadas e as metricas mostrarem que regras puras atingem um teto. Gate estatistico: F-beta(2) do encoder >= baseline com p < 0.05.

**Resultado esperado:** modelo fine-tuned com metricas documentadas; decisao data-driven sobre adotar ou nao.

### Fase 4 -- Excelencia (continuo)

**O que:** heads por especialidade, calibracao, monitoramento de drift, NER clinico, LLM como fallback seletivo para casos inconclusivos.

**Por que:** evolucao continua com evidencia. Cada melhoria e validada antes de entrar. O motor nunca para de melhorar, mas nunca muda sem prova.

---

## 5. O que muda e o que nao muda

| Aspecto | Muda? | Detalhe |
|---------|-------|---------|
| Logica NLP (regras, negacao, embeddings) | Reorganiza | Sai de notebooks individuais, entra na biblioteca centralizada |
| Canal de saida (Excel/SharePoint) | **Nao** | Permanece como canal oficial. Motor alimenta Delta, serving layer existente consome |
| Orquestracao (SQL + Jobs Databricks) | **Nao** | Pipeline linear agendada permanece. Motor e chamado dentro do job existente |
| Notebooks | Simplifica | Deixam de conter logica de negocio. Passam a ser "fios de ligacao" que chamam a lib |
| Configuracoes clinicas | Externaliza | Saem do codigo, vao para YAML versionado no Git |
| Algoritmos atuais em producao | **Nao** (coexistencia) | Continuam rodando. Motor novo valida em paralelo ate provar paridade |
| Metricas e observabilidade | Adiciona | Metadados padronizados por execucao; baseline de precision/recall/F1 |
| Infra (clusters) | CPU ate Fase 2 | GPU so entra na Fase 3 (encoder). Fases 1-2 rodam nos clusters atuais |

---

## 6. Como se valida

Cada fase tem criterio de validacao proprio:

| Fase | Validacao | Quem decide |
|------|-----------|-------------|
| 1 (MVP) | Paridade: output do motor = output do algoritmo atual, para amostra dourada | DS + medico (amostragem) |
| 2 (NLP avancado) | Paridade por especialidade + melhoria mensuravel em precision/recall | DS + medico |
| 3 (Encoder) | Tripla: encoder vs regras vs medico. Gate estatistico (F-beta >= baseline, p < 0.05) | DS + medico + gate estatistico |
| 4 (Excelencia) | A/B testing operacional. Metricas de negocio (captacao, conversao) | DS + medico + produto |

**Principio:** nenhuma fase avanca sem evidencia. Nenhum algoritmo em producao e substituido sem validacao de paridade. O corpo medico participa de todas as aprovacoes.

---

## 7. Resumo em uma pagina

**Problema:** 9 algoritmos NLP independentes com codigo duplicado, runtimes divergentes, sem metricas padronizadas. Funciona, mas nao escala.

**Proposta:** motor NLP unico, parametrizavel por especialidade, evoluindo em 4 fases. Comeca simples (regras, CPU), valida, e so entao evolui para tecnicas avancadas (encoder, GPU).

**Posicionamento:** evolucao controlada com coexistencia. Nao e substituicao, nao e big bang, nao muda canal de saida nem orquestracao.

**Piloto:** hepatologia (legado vs motor novo).

**Meta de evolucao:** encoder compartilhado (BERTimbau) com specialty heads por especialidade. So entra quando a base rule-based provar seu valor e as metricas mostrarem teto.

**Validacao:** gate por fase, com corpo medico. Sem excecao.
