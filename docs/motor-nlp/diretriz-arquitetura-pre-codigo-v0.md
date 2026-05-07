# Diretriz -- Arquitetura Pre-Codigo

**Regra:** nenhuma linha de codigo sem antes passar por estes 4 passos.

---

## 1. Cenario do sistema

### Problema

Laudos clinicos (imagem e texto) precisam ser processados por NLP para identificar pacientes relevantes para linhas de cuidado. Hoje existem 9 algoritmos independentes com 5 tipos de engine distintos, logica duplicada, zero testes e zero metricas. Cada novo algoritmo multiplica divida tecnica.

### Usuarios

| Usuario | Expectativa |
|---------|------------|
| **DS (desenvolvedor)** | Criar/manter algoritmos com rapidez, sem reescrever limpeza/negacao a cada especialidade |
| **MLOps** | Deploy padronizado, CI com gates, runtime unico, clusters gerenciaveis |
| **Medico/clinico** | Resultados confiaveis, rastreabilidade de regras, validacao antes de ir a producao |
| **Operacao (enfermagem/navegacao)** | Receber lista de pacientes relevantes via Excel/SharePoint, sem interrupcao |
| **Lideranca** | Escalar para 15+ especialidades sem custo proporcional de manutencao |

### Restricoes

- **PRD inalterado:** nenhuma alteracao nos pipelines em producao ate validacao de paridade
- **Canal de saida:** Excel/SharePoint permanece como canal oficial
- **Orquestracao:** SQL + Jobs Databricks (sem reescrita)
- **Infra Fases 1-2:** CPU only (clusters atuais)
- **LGPD:** sem PHI em repositorio, sem exportar texto de laudo para servicos externos
- **Coexistencia:** motor novo roda em paralelo ate provar paridade ou melhoria

---

## 2. Pilares arquiteturais

Toda decisao de design deve ser validada contra estes pilares:

| Pilar | Aplicacao |
|-------|-----------|
| **Manutenibilidade** | Logica NLP em libs testadas; notebooks com ~50 linhas; alteracao de regra = alteracao de YAML |
| **Escalabilidade** | Nova especialidade = copiar template + ajustar YAML (minutos, nao dias) |
| **Consistencia** | Uma unica implementacao de TextPipeline, negacao, scoring para todas as especialidades |
| **Observabilidade** | `config_version`, `engine_version`, `confidence_score`, metricas (precision/recall/F1) em cada execucao |
| **Simplicidade** | Fase 1 e rule-based puro (CPU). Complexidade adicionada por fase, com gate de validacao |
| **Confiabilidade** | Testes unitarios com frases sinteticas; validacao contra baseline (output atual) antes de promover |
| **Seguranca** | LGPD by default; sem texto de laudo em logs; sem identificadores em claro |

---

## 3. Componentes e comunicacao

```
plataforma-nlp/                          3 LIBS (multi-repo, sem imports cruzados)
  especialidades/{nome}/                 +-----------------+
    config.yaml  ----secao "nlp"-------> | nlp_engine      |
    ntb_motor.py                         |  text_pipeline   |
      (Composition Root)                 |  engine          |
      le YAML                            |  config_loader   |
      extrai secoes                      |  scoring         |
      injeta dict em cada lib            +-----------------+
                  ----secao "data"-----> | data_manage     |
                                         |  loader / saver  |
                                         |  contracts       |
                  ----secao "monitoring"> | monitoring      |
                                         |  metrics         |
                                         |  quality_guard   |
                                         +-----------------+
```

### Fronteiras inviolaveis

- Nenhuma lib importa outra
- Libs recebem `dict` (nao leem arquivo)
- Notebook e o unico ponto que conhece todas as libs
- Serving layer (Excel/SharePoint/API) nao muda

---

## 4. Revisao antes de implementar

Antes de abrir PR, validar:

- [ ] O modulo tem responsabilidade unica? (se faz 2 coisas, dividir)
- [ ] Existe dependencia desnecessaria? (lib importando outra lib?)
- [ ] A variacao entre especialidades vem do YAML, nao do codigo?
- [ ] Tem teste unitario com frase sintetica? (sem PHI)
- [ ] O output inclui `config_version` e `engine_version`?
- [ ] O contrato de dados (entrada/saida) esta sendo respeitado?
- [ ] Pode simplificar? (YAGNI -- nao implementar o que nao e necessario agora)
- [ ] Os trade-offs estao documentados? (se houve escolha entre opcoes)

---

*Detalhamento: `07-relatorio-final-v0-plataforma-nlp-clinica.md`, `anexo02-arquitetura-motor-nlp-v0.md`*
