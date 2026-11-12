---
description: Padrão de código da plataforma NLP (fabrica-ia-nlp-platform) — estrutura de notebook, carga por %run, classes, config, job, SQL/Spark, dados e validação. Fonte de verdade = specs e POPs da plataforma.
paths:
  - "fabrica-ia-nlp-platform/**"
  - "_worktrees/**/plataform/**"
  - "_worktrees/**/jobs/**"
  - "**/plataform/**/*.py"
  - "**/jobs/definicoes/*.json"
  - "**/jobs/clusters/*.json"
---

# Padrão de código da plataforma NLP

> **A plataforma é a fonte de verdade**, não esta regra. Cada item cita de onde vem (`docs/usage/specs/NN`,
> `docs/usage/boas-praticas/NN`, POP-IA-08). Se a plataforma mudar, **o código vizinho vence** — conferir um módulo
> irmão antes de escrever, e atualizar esta regra.
>
> **Validar antes de commit/PR:** skill `validar-padrao-plataforma` (roda `.claude/scripts/validar-padrao-plataforma.py`).

## 1. Estrutura de arquivo (spec 01, módulos de `tools/`)

- Todo `.py` em `plataform/` (exceto `tests/`) é **notebook**: 1ª linha `# Databricks notebook source`.
- Módulo novo de `tools/` e notebook de task: cabeçalho `# MAGIC %md` com **título**, o que faz, **alçada** e **ordem de
  carga** (modelo `tools/randomization`). ⚠️ Não é universal: `pipeline_e2e/`, `data/`, `observability/` e as configs de
  navegação não usam — não cobrar deles.
- Células separadas por `# COMMAND ----------`; em notebook de task, **`# DBTITLE 1,<título>`** em cada célula.
- `runs/` e notebooks de task leem widgets no nível do módulo (correto); módulo **de biblioteca** carregado por `%run` não
  chama `dbutils` no nível do módulo.
- Imports na primeira célula de código.

## 2. Carga e dependência (spec 01 §4)

- Código carregado por **`%run` com caminho literal**, um por célula. **Nunca** `sys.path` + `import` dentro de `plataform/`.
- Dependência entre módulos = nomes do namespace do `%run`, declarada com `# ruff: noqa: F821` e a ordem no cabeçalho.
- **Sem `dbutils` no nível do módulo** em `tools/`: os testes executam o notebook por caminho (modelo
  `tests/randomization/test_resolver.py`). Configs vêm por `dbutils.notebook.run` (spec 03).
- Notebook de task em serverless: cabeçalho `environment_version = "5"` (pydantic 2). Criar o logger **logo após** o
  `%run` do logging — notebooks seguintes sombreiam `Logger`.

## 3. Classes e documentação

- Gerenciadores: **pydantic** `BaseModel` com `Field(..., description=...)` e `ConfigDict(arbitrary_types_allowed=True)`
  (modelo `pipeline_e2e/nlp_ia_06_view.py`).
- Docstring **Google** (`Args`, `Returns`, `Raises`, `Example`) em toda função/classe pública; privadas com uma linha
  quando a intenção não é evidente. Português, registro técnico.
- Funções pequenas; `C901` se resolve extraindo, não com `noqa` (regra 08 #8).

## 4. Config de especialidade (spec 27, POP-IA-08, regra `motor-nlp`)

- Dicionário literal `CONFIG`, devolvido por `dbutils.notebook.exit(json.dumps(CONFIG, ensure_ascii=False))`.
- **Sem bloco morto:** nada de `catalog`, `monitoring`, `distribution`, `{catalog}`/`{run_id}` literais.
- Chave que decide comportamento é **sempre declarada** (ex.: `llm_router.enabled`).
- Cabeçalho `%md` (o que a linha decide, de onde veio a régua, divergências deliberadas) e **changelog no arquivo**,
  com o número que sustentou cada versão.

## 5. Job e cluster (spec 29, spec 31, boas-praticas/06)

- `pause_status`: `"${job_status_<linha>_batch}"` **sem `?`** — com `?`, variável ausente remove a chave e o job nasce
  despausado. Variável declarada em `jobs/ambientes` (Administrador), começando `PAUSED`.
- Versão do motor **literal** por linha (`"nlp_engine_version": "0.12.3"`), nunca `${nlp_engine_version}`/`latest`.
- Tasks de envio (`onedrive_*`, `api_*`) com `"disabled": "${disabled?}"` — senão hml posta na navegação real.
- `environment` vem de `${env_motor}` (o runner aceita `prd`, não `prod`).
- Cluster, `node_type`, `policy`, `jobs/ambientes` e `mlops.yml` são do **Administrador** (POP-IA-08): propor, não
  impor.

## 6. SQL e Spark

- Valor que entra em SQL passa por **literal escapado** (aspas e barra invertida) ou **validação de identificador**;
  nada de f-string com entrada livre.
- Regex de filtro em Spark **sem `\b`** perto de acento (Spark 18: `\b` é ASCII) — fronteira explícita
  `(^|[^a-zà-ÿ])`. Barra em literal SQL dobra (`'\\b'`); a cadeia `.py → Python → SQL → regex` erra em silêncio.
- Janela de datas **fechada nos dois lados** quando há data de referência (reprocessar não lê dia posterior).
- Data do run em UTC: execução manual entre 21h e 24h BRT cai no dia seguinte (POP-IA-08 bug 5).

## 7. Dados e contrato

- Saída no **contrato da tabela de saída** das linhas (`tb_mod_diamond_<linha>_saida_v0`), com `config_version` e
  `engine_version`; schema = nome da linha (spec 00/14). Mudar contrato de saída ou da view **é demanda à plataforma**.
- **PII só na view** de exportação (decrypt lá); nunca em tabela de saída, log, teste ou repositório.
- A view filtra por `tb_cad_pipeline_configuracao_envio_nlp_platform` (desde 14/10/2026): em dev sai vazia para toda
  linha — validar com o filtro trocado, não concluir defeito.

## 8. Testes e validação (regra 06)

- `pytest` sem Spark, dado **sintético**; teste de **recusa** além de aceitação.
- **Mutação** nos comportamentos novos, com o script afirmando que a mutação foi aplicada (regra 08 #5).
- Mudança que promete não alterar resultado: **paridade** medida (mesma janela, contagem por chave), com **controle
  positivo** que prova que a comparação detecta diferença.
- `ruff` limpo e testes com exit code conferido sem pipe (regra 08 #1).

## 9. Alçada (POP-IA-08)

| quem | edita |
|---|---|
| Ciência de Dados | config da especialidade, `jobs/definicoes/<job>.json`, arquivo de navegação, vocabulário |
| Dono do motor / plataforma | código de `plataform/` (runner, tools, pipeline), contrato de saída e da view |
| Administrador Databricks | catálogo, schema, grants, cluster, policy, `jobs/ambientes`, `mlops.yml`, esteira |

Antes de tocar o quadro de outro, alinhar. Pedido de procedimento (schema, grant) não vai em PR.
