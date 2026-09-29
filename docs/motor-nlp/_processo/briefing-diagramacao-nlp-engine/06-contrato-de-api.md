---
titulo: Contrato da API Python da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - src/.../contracts.py · 0.13.0
  - tests/api_surface.json · gerado, travado por gate
  - docs/REFERENCIA-API.md · gerado, travado por gate
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha a prancha 5 sabendo o que se chama, o que se recebe, e — o que
  mais importa — como a lib distingue "não é relevante" de "não consegui decidir".
relacionado:
  - 03-inventario-de-objetos.md
  - 07-sequencia-e-degradacao.md
  - 09-codigos-decisoes-e-intencao.md
---

# Contrato da API Python

> **O que este arquivo é:** a superfície pública da biblioteca — o que se chama, o que entra, o que sai e em que estados.
> **O que ele não é:** o contrato de **dados** de saída (as colunas da tabela Delta e o que a view expõe). Esse é da fronteira lib↔plataforma, vive em `contrato-saida-0.12.1` e no card `283647`, e está fora deste pacote — ver `BL-D-01` no `05`.

## 🔴 Duas coisas diferentes, e só uma está aqui

| | **API Python** — este arquivo | **contrato de dados** — fora |
|---|---|---|
| quem consome | quem **programa** contra a lib | a tabela Delta e a view de exportação |
| o que é | `process()` e três `TypedDict` | as colunas gravadas e as chaves do blob |
| onde é travado | `api_surface.json` e `REFERENCIA-API.md`, por gate | PR 7228 da plataforma |

A lib devolve `EngineOutputRow`. **Quem decide o que vira coluna, como o blob é serializado e o que
a view expõe é a plataforma.** Campo pode existir no retorno e não chegar à tabela — ou chegar com
outro nome. Desenhar os dois como um só é o erro clássico nesta fronteira.

## A superfície pública

**35 módulos declarados e travados por gate**, em dois subpacotes irmãos:
`nlp_engine.nlp_engine` (o motor, 20 módulos mais 11 de `text_pipeline`) e `nlp_engine.monitoring`
(observabilidade, 6). ⚠️ **Não há import cruzado entre eles**, por arquitetura declarada.

## Os "endpoints"

| metodo_e_rota | tabela_que_serve | recurso_de_origem | situacao | ressalva |
|---|---|---|---|---|
| `ClinicalNlpEngine(engine_version)` | — | `engine.py` | 🟢 | construtor; sem efeito colateral |
| `.process(rows, nlp_config, *, specialty_id, config_version)` | — (devolve `list[EngineOutputRow]`) | `engine.py` | 🟢 | **a rota principal**; síncrona, sem estado entre chamadas |
| `normalize_config(cfg)` | — | `config_loader.py` | 🟢 | ⚠️ **o motor NÃO normaliza**; quem chama precisa fazer isso, ou a régua roda sem negação |
| `merge_with_shared_organs(cfg)` | — | `config_loader.py` | 🟢 | injeta o `ORGANS_SHARED` |
| `validate_engine_output_row(row)` | — | `output_invariants.py` | 🟢 | usado pelo gate; disponível ao consumidor |
| `monitoring.runner.*` | Delta de métricas | `monitoring/runner.py` | 🟡 | ponto de entrada do notebook de monitoramento — `L4`, sem documentação própria |
| `monitoring.data_quality.*` | — | `monitoring/data_quality.py` | 🟡 | volumetria, PSI e schema da entrada — `L4` |
| `monitoring.baselines.*` | Delta de baselines | `monitoring/baselines.py` | 🟡 | persiste métrica e compara com baseline — `L4` |
| `monitoring.quality_guard.*` | — | `monitoring/quality_guard.py` | 🟡 | precisão, recall, F1 — `L4` |

ℹ️ **As entradas 🟡 permanecem no contrato.** Elas são consumidas hoje por 8 arquivos da
plataforma; o amarelo é sobre **documentação ausente**, não sobre funcionamento. Removê-las da
prancha esconderia que a fronteira existe.

## O que entra — `EngineInputRow`

| campo | tipo | obrigatório | observação |
|---|---|---|---|
| `id_exame` | `str` | **sim** | atravessa intacto até a saída |
| `exm_laudo_texto` | `str` | **sim** | o texto do laudo; ⚠️ pode chegar como documento RTF inteiro |
| `exm_mod` | `str` | sim | modalidade |
| `exm_tipo` | `str` | sim | tipo de exame |
| `dt_exame` | `str` | sim | data |
| `id_paciente` · `id_unidade` | `str` | não | atravessam sem uso na decisão |
| `Laudo` | `str` | não | ⚠️ **nome herdado do legado**; mantido por compatibilidade |

## O que sai — `EngineOutputRow`, 19 campos

Os 7 de entrada atravessam, mais 12:

| campo | tipo | o que é |
|---|---|---|
| `fl_relevante` | `int` | **a decisão** — 0 ou 1 |
| `confidence_score` | `float` | confiança calibrada em `[0, 1]` |
| `findings` · `findings_match` · `findings_spans` | `str` | o achado, o termo que casou, a posição no texto |
| `exm_laudo_texto_tratado` | `str` | o texto que a régua de fato leu |
| `exm_laudo_resultado` | `str` (JSON) | **a trilha** — `ExmLaudoResultadoPayload`, 30 campos |
| `specialty_id` · `config_version` · `engine_version` | `str` | rastreabilidade da decisão |
| `id_predicao` · `dt_execucao` | `str` | identificação da execução |

## 🔴 Estados de resposta — a parte que mais importa

**Lista vazia nunca significa "sem achado".** A lib distingue, e a distinção está na trilha, não no
`fl_relevante`:

| estado | como se reconhece | significa |
|---|---|---|
| **completo — não relevante** | `fl_relevante: 0` · `decision_trail.outcome.reason` preenchido · sem `llm_error` | a régua rodou e **não encontrou** |
| **completo — relevante** | `fl_relevante: 1` · `findings` preenchido · `n_positive_spans > 0` | a régua encontrou e a decisão se sustenta em evidência |
| 🔴 **relevante SEM evidência de régua** | `fl_relevante: 1` · **`n_positive_spans: 0`** | ver `R7`: promoveu sem achado léxico. **Medido: 118 laudos em 30 dias** |
| **parcial — degradado em silêncio** | `semantic_backend: token_overlap` com `use_embeddings: True` | executou um perfil **diferente do configurado** — `R3` |
| **parcial — juiz não consultado** | `llm_router_mode: deterministic` com `enabled: True` | `mode` não reconhecido; o juiz não foi chamado — `R6` |
| **indisponível — falha de transporte** | `llm_error` preenchido | ⚠️ e o que acontece depois **depende de `fallback_policy`**: `keep_current` mantém; `positive_in_band` **entrega** — `R2` |
| **erro de configuração** | exceção `ConfigInvalida` | a lib recusa a config em vez de rodar errado |

ℹ️ **A lib recusa config inválida, e isso é decisão:** `ConfigInvalida` é levantada no
`normalize_config()`, antes de qualquer laudo ser processado. Falhar cedo e alto vale mais que
decidir com régua meio carregada.

🟡 **Uma dívida declarada:** `confidence` inválido na configuração do `llm_fallback` vira `0.5`
**sem erro e sem log**. A alternativa correta é recusar a config; ficou para o bump que mexer em
validação.

## O que vale para toda chamada

| propriedade | como é |
|---|---|
| **paginação** | não há: o chamador monta o lote. ⚠️ `process()` recebe e devolve **lista inteira em memória** — streaming saiu de escopo, `D2` |
| **máscara** | a lib **não mascara**; o texto atravessa como veio. Ver `08` |
| **auditoria** | a trilha de 30 campos é gravada **por laudo**, sempre |
| **frescor** | não se aplica: a lib é sem estado e decide sobre o que recebe |
| **metadados de resposta** | `engine_version` e `config_version` em toda linha |
| **versionamento** | SemVer, versão única no `pyproject.toml`. Mudança de saída ou de chave de config exige **alinhamento antes** de implementar |

## ⚠️ O que muda na `0.14.0`

**Mudança de contrato mapeada e não implementada** — `P3` no `09`. Entram
`llm_prompt_tokens` e `llm_completion_tokens` **na camada quantitativa**, que hoje chama o LLM e não
registra token nenhum: **221 das 270 chamadas de um dia ficam sem contabilidade**. A prancha 5 deve
nascer com essa pendência visível.
