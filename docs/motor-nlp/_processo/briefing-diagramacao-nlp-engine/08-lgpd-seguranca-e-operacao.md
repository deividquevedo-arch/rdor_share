---
titulo: LGPD, segurança e operação da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - src/.../llm_router_backend.py · 0.13.0
  - azure-pipelines.yml · scripts/check_release.py
  - Makefile · .pre-commit-config.yaml
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém preenche as colunas laterais da prancha 1 — o que a lib faz e não faz com o
  dado que a atravessa, e como ela é construída, testada e publicada.
relacionado:
  - 02-arquitetura-e-camadas.md
  - 09-codigos-decisoes-e-intencao.md
---

# LGPD, segurança e operação

> **O que este arquivo é:** o que a lib faz e, sobretudo, o que ela **não** faz com o dado clínico que a atravessa; e como ela é construída, travada e publicada.
> **O que ele não é:** a classificação de dado da plataforma. Catálogo, schema, grant, máscara e base legal são da camada que persiste — a lib não persiste.

## 🔴 Desvio de unidade, declarado

O template pede classificação por coluna, onde é mascarada, onde é auditada, base legal e parecer
de DPO. **Nada disso existe na lib, e a ausência é a informação.**

A `nlp-engine` **não é titular de dado**: não persiste, não cria tabela, não define grant, não
aplica máscara. Ela é uma função — recebe texto, devolve decisão. A tabela abaixo mantém a coluna
`classificacao`, e o que ela classifica é **o que a lib faz com o dado enquanto ele passa**.

## O dado que atravessa

| coluna | classificacao | mascarada_onde | auditada_onde | base_legal | parecer_dpo |
|---|---|---|---|---|---|
| `exm_laudo_texto` | **texto clínico** | ⚠️ **em lugar nenhum dentro da lib** — atravessa como veio | não é registrada em log pela lib | da plataforma | `P8` |
| `exm_laudo_texto_tratado` | **texto clínico derivado** | idem | devolvida ao chamador, nunca logada | da plataforma | `P8` |
| `id_paciente` · `id_unidade` | identificadores | não | atravessam **sem uso na decisão** | da plataforma | `P8` |
| `findings` · `findings_match` | **achado clínico** | não | na saída e na trilha | da plataforma | `P8` |
| chave de API do LLM | **credencial** | 🟢 **`_scrub()` antes de qualquer mensagem de erro** | `llm_api_key_origin` registra a **origem**, nunca o valor | — | — |
| trecho enviado ao LLM | **texto clínico em trânsito** | não | `llm_input_chars` registra o **tamanho**, não o conteúdo | da plataforma | 🔴 `P9` |

## O que a lib garante, e é o que vai na coluna lateral

| garantia | como |
|---|---|
| **Não persiste nada** | devolve `list[dict]`; não conhece Spark, catálogo ou Delta |
| **Não registra texto de laudo em log** | o logging estruturado registra contagem, motivo e versão — nunca o conteúdo |
| **Não vaza credencial em erro** | `_scrub()` roda **antes** do truncamento. ⚠️ A ordem importa: truncar primeiro parte o token na fronteira e o fragmento escapa do `replace` |
| **Declara a origem da credencial** | `llm_api_key_origin` diz se veio de `api_key` literal ou de `api_key_env` — ⚠️ e o literal **vence** o de ambiente |
| **Não descobre conexão** | `base_url` e chave vêm da config ou do ambiente; sem `databricks-sdk` |
| **Sem PHI em teste** | as fixtures são sintéticas (`SYN`, `fixture-e2e-001`); o único arquivo "de produção" versionado é uma **lista de 21 nomes de chave** |
| **Guarda de segredo no commit** | hook próprio bloqueia CPF, token Databricks (`dapi` + 32 hex) e chave PEM |

## Minimização — o que deliberadamente não é trazido

| não entra | por quê |
|---|---|
| nome de paciente, documento, contato | a decisão não depende deles; a lib nem os recebe no contrato de entrada |
| `id_paciente` na decisão | recebido e **repassado**, nunca usado como sinal |
| o texto enviado ao LLM, no blob | só o **tamanho** (`llm_input_chars`) é registrado |
| conteúdo em log | nenhum caminho de log da lib emite o texto |

## 🔴 Duas pendências que dependem de parecer

| codigo | pendência | por que a demora vira retrabalho |
|---|---|---|
| `P8` | **A classificação e a base legal do dado que a lib processa nunca foram formalizadas** — elas existem para a plataforma, e a lib herda por adjacência | se a classificação exigir tratamento **dentro** da lib (pseudonimização antes da decisão, por exemplo), a mudança é de contrato e atinge as 6 linhas em produção |
| `P9` | **Texto clínico em trânsito para o endpoint de LLM** — a lib envia trecho do laudo ao Model Serving, e o parecer sobre esse trânsito não está registrado no repositório | quanto mais linhas ligarem o juiz, maior a superfície; ligar primeiro e perguntar depois inverte a ordem correta |

ℹ️ **O endpoint é interno** (Databricks Model Serving, no mesmo workspace do job), o que reduz o
alcance — mas *reduzir* não é o mesmo que *ter parecer*.

## Operação e confiabilidade

### O gate de construção — sete alvos

`ruff` · `ruff format` · `mypy --strict` · referência de API · superfície pública · doctest ·
cobertura. **Estado em 18/09/2026: 1.240 testes, 88,11% por ramo, piso declarado em 85%.**

**Dois estágios de hook:** 6 verificações no commit (rápidas, sobre o que mudou) e 10 no push
(inclui a suíte com cobertura). ⚠️ O hook de push **pagou o próprio custo no primeiro uso**: pegou
um `ModuleNotFoundError` que passava em `python -m pytest` e teria quebrado o CI.

### Não-regressão

Golden com corpus de referência — **2.920 linhas, `sha256` idêntico** contra a tag ancestral, com a
pré-condição de cobertura impressa junto. Ver `05`.

### Publicação

| | |
|---|---|
| build | `hatchling`, versão única no `pyproject.toml` |
| destinos | **dois feeds**: `fabrica-ai-hml` (da `hml`) e `fabrica-ai` (da `main`) |
| gate de release | `check_release.py` — coerência de versão nos docs, ausência de segredo, e se a versão já existe no feed de **destino** |
| 🔴 lição registrada | **o feed é imutável.** Mesmo número com dois conteúdos não se resolve: exige bump. E **critério global não decide por ambiente** — a tag é única, os feeds são dois, e o gate já concluiu "já publicada" e **pulou a publicação em produção, verde** |
| `L3` | o caminho `livre` da esteira **segue sem prova**; só o `publicada` foi exercitado |

### Quem é acionado

⚠️ **A lib não tem alarme próprio.** Ela expõe o subpacote `monitoring` — volumetria, PSI, schema e
baselines — que é **chamado pelo notebook da plataforma**, e a tabela de monitoramento é de lá.

🔴 **E há um vazio conhecido nessa fronteira:** a tabela de monitoramento **não tem nenhuma coluna
de LLM**. O `alert_threshold_relevance_drop` não pega falha de LLM, porque a régua sustenta a taxa —
provado no TI-RADS, 3,17% → 3,21% enquanto 4.703 chamadas falhavam. **A classe de defeito mais cara
do sistema é justamente a que não gera alerta.**

### Reprocessamento

Da plataforma, não da lib. O widget `reprocess_enable` só funciona em dev; a dedup por `id_exame`
**sem `config_version`** bloqueia janela já processada **em silêncio**, e o sinal é um run que fecha
em segundos com sucesso.
