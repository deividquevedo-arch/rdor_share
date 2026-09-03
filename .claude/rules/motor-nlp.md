---
description: Diretrizes do Motor NLP Clinico -- 3 libs, config YAML, clean arch, RPI, SDD, anti-vibe-coding
---

# Motor NLP Clinico -- Regras para o Agent

## Metodologia (obrigatoria)

- **RPI:** Research (ler legado, entender) -> Plan (SPEC do modulo) -> Implement (codar com teste)
- **SPEC antes de codigo:** definir input, output, edge cases, o que NAO faz. So entao implementar
- **Progressive disclosure:** refatorar em camadas (extrair -> tipar -> refinar -> otimizar). Cada camada e um PR
- **Anti-vibe-coding:** toda decisao explicita. Sem copia cega de notebook. Sem "depois melhoro"
- **Code review estrutural:** acoplamento, coesao, simplicidade, contrato, nomeacao, paridade com legado, YAGNI
- Ref: `docs/motor-nlp/diretriz-tech-lead-refatoracao-v0.md`

## Arquitetura

- O sistema e composto por 3 libs independentes: `nlp_engine`, `data_manage`, `monitoring`
- Nenhuma lib importa outra. Comunicacao por dict/DataFrame
- Notebooks Databricks sao Composition Root (~50 linhas): leem YAML, extraem secoes, injetam dict em cada lib
- Serving layer (Excel/SharePoint/API) permanece inalterado

## Escopo e backlog

- **So implementar** trabalho que esteja em **historias/tasks acordadas** com o time (ids **Sxx** / **Txx.y** ou equivalente no task board).
- **Fonte canonica:** `docs/motor-nlp/anexo03-historias-e-tasks-v0.md`, `docs/motor-nlp/05-roadmap-entregas-sprint-v0.md`, e o que estiver fechado para a **sprint corrente**.
- **Nao criar** tasks intermediarias, refactors ou melhorias que nao constem desse alinhamento (YAGNI ao nivel de backlog).
- **MLOps / infra** (CI/CD, clusters, feeds de artefactos, politicas de deploy): **fora de escopo de implementacao** ate decisao explicita do time; no maximo **sugestao embasada** em documento ou decisao registada no repo, se for pedido.

### Entregas (ligacao ao board)

- Antes de codar: **confirmar e citar** qual **Sxx / Txx.y** (ou id do board) esta em causa.
- Se o pedido **nao** corresponder a nenhuma task mapeada: **parar** e propor alinhar backlog / quebrar em tasks no anexo ou board; **nao** avancar com escopo implicito.
- Em commits ou descricao de PR, **referenciar** o id da task/historia sempre que possivel.

## Antes de codar

- Nao implemente sem entender o contexto. Leia as diretrizes em `docs/motor-nlp/diretriz-*.md`
- Siga RPI: primeiro Research (ler o notebook legado), depois Plan (SPEC), depois Implement
- Modulos **compartilhados** entre especialidades: Research com **inventario multi-notebook** e SPEC consolidado — ver secao Research em `docs/motor-nlp/diretriz-tech-lead-refatoracao-v0.md`
- Identifique qual lib sera modificada. Se impacta mais de uma, revise a separacao
- Confirme que a variacao entre especialidades vem do YAML, nao do codigo
- Se extraiu funcao de notebook, garanta paridade (output identico ao original)

## Regras de codigo

- Funcoes < 30 linhas. Nomes descritivos. Type hints em funcoes publicas
- Zero hardcode clinico (keywords, thresholds, orgaos). Tudo no YAML
- Sem comentarios obvios. Comentarios so para intencao nao-evidente
- Feature flags via config dict para comportamento condicional
- Backward compatibility obrigatoria (semver)
- Sem PHI (dados de paciente) em testes, logs ou repositorio
- Sem `eval()`, `exec()`, imports mortos ou `nltk.download('all')`

## Testes

- pytest com frases sinteticas (sem PHI)
- Cenarios: input valido, invalido, com negacao, sem negacao, YAML valido/invalido
- 1 arquivo de teste por modulo de negocio

## Config YAML

- Duas camadas: `shared/organs.yaml` (universo de orgaos) + `{specialty}/config.yaml` (por lib)
- Libs recebem dict, nao leem arquivo. Notebook extrai e injeta
- `config_version` e `engine_version` obrigatorios em todo output

## Config de especialidade -- nada passa com bloco morto

**Config declarada e nao consumida nao passa em revisao.** Vale para PR de qualquer
especialidade, sem excecao. Referencia: `cancer_rim`.

### O que o runner LE

`specialty_id` -- `config_version` -- `model_version` -- `data` -- `nlp` -- e **`runtime.llm_router`**.

⚠️ A SPEC 27 §6.1 afirma que "nada neste pipeline le `runtime`". **Esta ERRADO.** O
`ntb_ia_loader.py:105-113` faz `self.llm_router.update(runtime_llm)` -- o `runtime` **sobrescreve**
o `nlp`. Foi o que derrubou a primeira subida do ca-estomago. Nao confiar na SPEC nesse ponto.

### O que NAO e lido, e portanto nao entra

- `catalog` -- o catalogo efetivo vem do `EnvironmentConfig`. Pior que inutil: costuma apontar para
  o ambiente errado (`diamond_ia_hml`, workspace antigo) e quem promove le "dev" na config de prd.
- `monitoring` -- nenhuma chave. E a `metrics_table` declarada nem e a tabela usada.
- `distribution` -- `outbound_volume`, `outbox` e `inbox` so aparecem dentro das proprias configs.
- placeholders `{catalog}` e `{run_id}` -- ninguem interpola; ficam literais.

### Chave que decide comportamento e SEMPRE declarada

Mesmo quando o valor coincide com o default. `nlp.llm_router.enabled` e o caso canonico: ausente,
a lib assume `False`, mas um bloco com `mode`, `model`, `uncertainty_band` e `prompt_system`
completo faz o leitor concluir o contrario. Vale nos dois sentidos -- ligado sem declarar e tao
ruim quanto desligado sem declarar.

O guia da propria plataforma ja pede isso: `docs/usage/boas-praticas/02`, Passo 6 --
*"mantenha `runtime` coerente com `nlp.llm_router`"*.

### Por que a regra e dura

Bloco morto nao parece placeholder: os valores sao **plausiveis** -- um catalogo que existe, uma
tabela com nome verossimil, um prompt inteiro. Ninguem desconfia. E a mesma classe do extra
`[databricks]` vazio, que dava `pip install` com sucesso sem instalar nada.

### Cabecalho e changelog

Config e artefato revisavel: cabecalho `# MAGIC %md` dizendo o que a linha decide, de onde veio a
regua, o que foi adaptado e quais divergencias sao deliberadas; e o historico de versoes **no
arquivo**, com o numero que sustentou cada uma. Historico que so existe no `git log` nao chega em
quem abre a config.

## Entrega ao negocio -- perfil COMPLETO, nunca estagio intermediario

- **O alvo de toda especialidade e o fluxo completo:** regra -> hibrido calibrado -> juiz.
  `rule_only` e estagio de DESENVOLVIMENTO, nao configuracao de entrega.
- **Nenhuma lista vai ao negocio a partir de perfil parcial.** Homologacao nao transfere entre
  comportamentos diferentes, e as camadas puxam em direcoes OPOSTAS: o hibrido tende a SUBIR
  recall (traz casos que o negocio nunca viu) e o juiz tende a DERRUBAR (some com casos que o
  negocio ja aprovou). Ligar qualquer um depois invalida o gabarito nos dois sentidos.
- **Camada que piora o resultado e refinamento, nao veredito.** Banda, prompt e regua sao os
  parametros. Nao existe caso medido neste projeto em que `rule_only` tenha superado o fluxo
  completo CALIBRADO.
- **Se a escalada for inevitavel depois da homologacao:** rodar os 3 perfis na **mesma janela ja
  homologada** e levar ao negocio so o conjunto de DISCORDANCIAS (o que o hibrido acrescenta, o
  que o juiz remove). Re-homologar o DELTA, nao o todo.

### Por que -- os dois casos que pagaram por isso

- **Ca-estomago:** a regua sozinha entregava 75 laudos com 55 errados (precisao 0,267). O que
  levou a 0,929 nao foi tirar o juiz, foi ALINHAR o prompt dele a regra de negocio -- seis
  versoes medidas, tres revertidas.
- **TI-RADS (2026-08):** a linha nao usa juiz, mas usa o LLM na camada quantitativa. Quando o
  endpoint caiu (403), a taxa de entrega caiu pela METADE (6,7% -> 3,2%) e os rebaixamentos por
  falta de medida foram de 48 para 824 num dia. Nem a linha "so regua" funciona sem LLM.

### Armadilhas ao medir os perfis

- `embeddings.use_embeddings: True` com `embedding_model` invalido ou placeholder (`PREENCHER`)
  cai em `token_overlap` **sem erro e sem log** -- o perfil "hibrido" medido NAO e hibrido.
- `llm_router` declarado **sem** `enabled`: a lib assume `False`. Config com modelo, banda e
  `prompt_system` mas sem `enabled` **nao roda o juiz** -- e quem le a config conclui o contrario.
- Medir em janela NOVA responde volumetria, nao acerto. Sem gabarito, delta nao se avalia.

## Contrato de dados

- Entrada: `id_exame`, `exm_laudo_texto`, `exm_mod`, `exm_tipo`, `dt_exame`
- Saida: `fl_relevante`, `confidence_score` (0.0-1.0), `config_version`, `engine_version`, `specialty_id`

## Documentacao de referencia

- `docs/motor-nlp/diretriz-tech-lead-refatoracao-v0.md` -- RPI, SDD, progressive disclosure, code review
- `docs/motor-nlp/diretriz-arquitetura-pre-codigo-v0.md` -- protocolo pre-codigo
- `docs/motor-nlp/diretriz-desenvolvimento-libs-v0.md` -- padroes das 3 libs
- `docs/motor-nlp/diretriz-config-e-governanca-v0.md` -- YAML + governanca clinica
- `docs/motor-nlp/checklists/checklist-implementacao-motor-nlp-fase1-v0.md` -- checklist Fase 1
- `docs/motor-nlp/anexo02-arquitetura-motor-nlp-v0.md` -- arquitetura detalhada
- `docs/motor-nlp/anexo03-historias-e-tasks-v0.md` -- stories e tasks
- `docs/motor-nlp/05-roadmap-entregas-sprint-v0.md` -- roadmap por sprint
