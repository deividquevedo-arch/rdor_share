# Anexo -- Decisoes de Configuracao e Deploy (alinhamento HEAD + MLOps)

**Origem:** reuniao de alinhamento com HEAD DS e MLOps (Sprint 1)
**Impacto:** refina o design de configuracao YAML e a estrategia de deploy proposta no motor NLP

---

## 1. Ponto do HEAD: desambiguacao por universo de orgaos

### O que foi levantado

O HEAD apontou que o algoritmo base olha para **todos os CIDs** e quebra o laudo por cabecalhos. Quanto mais orgaos o algoritmo conhece, melhor ele descarta o que nao e alvo. A preocupacao: se tivermos 1 YAML por especialidade contendo apenas os orgaos-alvo, **perdemos a capacidade de descartar achados que pertencem a outros orgaos**.

### Evidencia no codigo

A funcao `_snippet_mentions_other_organs()` (presente em biliar, neuroimuno, reumatologia, colon, pulmao) implementa exatamente esse mecanismo:

1. Recebe o CONFIG de **TODOS** os orgaos (`all_organs_config`)
2. Varre o trecho do laudo buscando mencoes a **todos** os orgaos (nao so o alvo)
3. Calcula **distancia em caracteres** entre o achado e cada mencao de orgao
4. Se o orgao mais proximo **nao e o alvo** e esta significativamente mais perto -> **descarta** o achado

Exemplo: laudo de TC abdominal menciona "nodulo" perto de "figado" e tambem perto de "vias biliares". Se target e `doencas_biliares`, e "nodulo" esta textualmente mais perto de "figado", o algoritmo descarta porque provavelmente pertence ao figado.

### Diagnostico

A preocupacao e **valida**. Se o YAML de reumatologia contivesse apenas orgaos reumatologicos, o motor perderia:

- **Segmentacao por cabecalhos** -- `segment_by_headers()` usa aliases de todos os orgaos para delimitar blocos
- **Descarte por proximidade** -- `_snippet_mentions_other_organs()` precisa do universo completo para calcular distancias
- **Desambiguacao multi-orgao** -- laudos de imagem abdominal mencionam figado, rim, pancreas, baco, vias biliares; sem conhecer todos, nao isola corretamente

### Decisao: config em duas camadas

O YAML por especialidade **nao elimina** o universo de orgaos. Ele **separa responsabilidades**:

**Camada 1 -- Universo compartilhado (1 arquivo, todas as especialidades referenciam)**

```yaml
# configs/shared/organs.yaml
organs:
  figado:
    seeds: [figado, hepatico, parenquima hepatico]
    regex: ["f[ií]gado"]
  rim:
    seeds: [rim, renal, rins]
  colon_reto:
    seeds: [colon, retal, reto, colorretal]
  doencas_biliares:
    seeds: [vias biliares, vesicula, coledoco]
  pancreas:
    seeds: [pancreas, pancreatico]
  reumatologia:
    seeds: [articular, articulacao, sinovial]
  # ... todos os orgaos conhecidos

header_aliases:
  figado: [figado, fígado]
  rim: [rim, rins]
  # ...
```

**Camada 2 -- Especialidade (1 arquivo por projeto, so o que e especifico)**

```yaml
# configs/reumatologia.yaml
specialty_id: reumatologia
target_organs: [reumatologia]

findings:
  reumatologia:
    artrite_reumatoide:
      seeds: [artrite reumatoide, AR, erosoes articulares]
    espondiloartrite:
      seeds: [espondilite, sacroileite]

postprocessing:
  threshold: 0.5
  use_embeddings: false
```

**O `config_loader.py` faz merge automatico:**

```python
def load_config(specialty_path):
    shared = load_yaml("configs/shared/organs.yaml")
    specialty = load_yaml(specialty_path)
    return {
        "all_organs": shared["organs"],           # para segmentacao e desambiguacao
        "header_aliases": shared["header_aliases"], # para segment_by_headers
        "target_organs": specialty["target_organs"], # o que BUSCAR
        "findings": specialty["findings"],           # achados por especialidade
        "postprocessing": specialty["postprocessing"]
    }
```

### Beneficio vs estado atual

| Aspecto | Hoje | Com duas camadas |
|---------|------|------------------|
| Universo de orgaos | Copiado **identicamente** em cada notebook (7+ copias) | **1 arquivo** compartilhado |
| Adicionar orgao novo | Atualizar manualmente cada notebook | Atualizar 1 arquivo, todas as especialidades se beneficiam |
| Risco de inconsistencia | Alto (copias divergem) | Eliminado (fonte unica) |
| Comportamento de desambiguacao | Igual ao atual | **Identico** (mesma logica, mesmos dados) |

---

## 2. Ponto do MLOps: YAML para deploy e propagacao de mudancas

### O que foi levantado

O MLOps apontou que o YAML como descrito parece voltado para solucoes novas. Para solucoes existentes que evoluem, a preocupacao e:

- Se muda a versao do algoritmo, precisa recompilar/atualizar cada notebook manualmente
- Isso gera retrabalho ao subir solucao nova ou evoluir solucao existente

Ele citou o padrao **Jenkins/Groovy da SulAmerica**: um template de codigo base + YAML de parametrizacao. No deploy, a esteira gera um arquivo de integracao novo a partir do template + parametros. Quando o template evolui (nova versao), a esteira re-roda com os mesmos parametros e gera artefato atualizado.

### Diagnostico

A preocupacao e **valida** e complementar ao nosso design. O YAML na nossa proposta faz **dois papeis** que merecem ser separados:

**YAML de configuracao clinica (runtime):**
- Taxonomias, keywords, thresholds, achados
- Muda quando medico valida nova regra
- Carregado em tempo de execucao pelo motor
- **Nao precisa de recompilacao**

**YAML de integracao (build-time):**
- Nome de tabelas, catalogo, ambiente, versao da lib, specialty_id
- Alimenta a esteira de deploy
- Gera o notebook concreto a partir de um template
- Muda quando a infra ou a versao do motor evolui

### Decisao: dois YAMLs + template de notebook

```
configs/
  shared/
    organs.yaml                  <-- universo de orgaos (camada 1, runtime)
  hepatologia.yaml               <-- config clinica (camada 2, runtime)
  biliar.yaml
  ...

deploy/
  template_notebook.py           <-- template base do notebook Databricks
  integration/
    hepatologia.yaml             <-- parametros de integracao (build-time)
    biliar.yaml
    ...
```

**YAML de integracao (build-time):**

```yaml
# deploy/integration/hepatologia.yaml
specialty_id: hepatologia
lib_version: "1.2.3"
catalog: ia
input_table: "{env}.ia.tb_diamond_mod_hepatologia_entrada"
output_table: "{env}.ia.tb_diamond_mod_hepatologia_saida"
metrics_table: "{env}.ia.tb_diamond_mod_metricas_qualidade"
environment: prd
```

**Template de notebook (base):**

```python
# deploy/template_notebook.py
# --- Gerado automaticamente pela esteira. NAO editar manualmente. ---
# specialty: {{specialty_id}}
# lib_version: {{lib_version}}
# generated_at: {{timestamp}}

%pip install clinical_nlp_engine=={{lib_version}}

import dbutils
from clinical_nlp_engine import TextPipeline, ClinicalNlpEngine, load_config

environment = dbutils.widgets.get("environment")
config = load_config("configs/{{specialty_id}}.yaml")
engine = ClinicalNlpEngine(config)

df_input = spark.table("{{input_table}}")
df_output = engine.process(df_input)
df_output.write.format("delta").mode("overwrite").saveAsTable("{{output_table}}")
```

### Fluxo de deploy

```
                MUDANCA                          ESTEIRA                        RESULTADO
                ======                           =======                        =========

Logica NLP      Publica nova lib (v1.3.0)
mudou               |
                    v
                Atualiza lib_version             Reroda esteira                 Notebooks
                em deploy/integration/*.yaml --> (template + YAML) ----------> regenerados
                                                                               com lib v1.3.0

Config          Altera configs/hepatologia.yaml
clinica             |
mudou               v
                Git tag + merge                  rdmlops sync                   Motor carrega
                                                                               novo YAML em
                                                                               runtime (sem
                                                                               regenerar notebook)

Template        Altera deploy/template_notebook.py
mudou               |
                    v
                                                 Reroda esteira                 Todos os notebooks
                                                 (template + YAML) ----------> regenerados com
                                                                               novo template

Nova            Cria configs/nova.yaml
especialidade   + deploy/integration/nova.yaml
                    |
                    v
                                                 Esteira gera                   Notebook novo
                                                 notebook novo                  pronto para deploy
```

### Cenarios de atualizacao (sem retrabalho manual)

| Cenario | O que muda | O que acontece | Recompila? |
|---------|-----------|----------------|------------|
| Nova regra clinica | `configs/hepatologia.yaml` | Motor carrega YAML novo em runtime | **Nao** |
| Nova versao da lib | `deploy/integration/*.yaml` (lib_version) | Esteira regenera notebooks | **Sim, automatico** |
| Novo boilerplate Databricks | `deploy/template_notebook.py` | Esteira regenera todos | **Sim, automatico** |
| Nova especialidade | `configs/nova.yaml` + `deploy/integration/nova.yaml` | Esteira gera notebook novo | **Sim, automatico** |
| Rollback | Volta lib_version no YAML de integracao | Esteira regenera com versao anterior | **Sim, automatico** |

### Comparativo com a abordagem alternativa (so lib, sem code gen)

| Aspecto | So lib (wheel) | Lib + Code Generation |
|---------|---------------|----------------------|
| Propagacao de mudanca | Manual (atualizar pin em cada notebook) | Automatica (esteira regenera) |
| Rollback | Manual (reverter pin) | Automatico (mudar lib_version no YAML) |
| Rastreabilidade | Git da lib | Git da lib + Git do YAML de integracao |
| Complexidade de esteira | Menor (so sync) | Maior (template rendering + sync) |
| Encaixa no padrao MLOps existente | Parcialmente | **Sim** (padrao Jenkins/Groovy conhecido) |

---

## 3. Impacto no design proposto

Esses dois pontos **nao invalidam** a proposta de motor NLP unificado. Eles **refinam** o design da camada de configuracao e deploy:

| Proposta original | Refinamento |
|-------------------|-------------|
| 1 YAML por especialidade (tudo junto) | 2 camadas: shared/organs.yaml (universo) + {specialty}.yaml (alvo) |
| YAML so como config runtime | 2 tipos de YAML: runtime (config clinica) + build-time (integracao) |
| Notebooks finos escritos manualmente | Notebooks gerados por esteira a partir de template + YAML |
| Propagacao de mudanca via wheel | Propagacao automatica via esteira (padrao MLOps) |

### Estrutura final de arquivos

```
clinical_nlp_engine/              <-- LIB (wheel versionado)
  text_pipeline.py
  engine.py
  config_loader.py                <-- merge automatico: shared + specialty
  scoring.py
  quality_guard.py
  semantic_expand.py

configs/                          <-- CONFIG RUNTIME (Git, carregado em execucao)
  shared/
    organs.yaml                   <-- universo de orgaos (desambiguacao)
    header_aliases.yaml           <-- aliases para segmentacao por cabecalho
  hepatologia.yaml                <-- alvo + achados + thresholds
  biliar.yaml
  reumatologia.yaml
  ...

deploy/                           <-- CONFIG BUILD-TIME (alimenta esteira)
  template_notebook.py            <-- template base
  integration/
    hepatologia.yaml              <-- tabelas, catalogo, lib_version
    biliar.yaml
    ...

tests/
  test_text_pipeline.py
  test_engine.py
  test_config_loader.py
  fixtures/
```

---

## 4. Proximos passos

1. **Alinhar com MLOps:** formato exato do YAML de integracao e como a esteira faz template rendering (Jinja2, cookiecutter, ou solucao propria)
2. **Alinhar com HEAD:** validar que `shared/organs.yaml` contem o universo completo que ele espera, e que `_snippet_mentions_other_organs()` recebe `all_organs` corretamente
3. **Incorporar no `config_loader.py`:** merge automatico de shared + specialty na Fase 1 (S03)
4. **Incorporar na esteira:** template + YAML de integracao na Fase 2 (S11, junto com CI gates)
