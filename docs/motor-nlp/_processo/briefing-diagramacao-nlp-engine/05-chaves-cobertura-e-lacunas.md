---
titulo: Cascata de decisão, cobertura do contrato e lacunas da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - scripts/golden_300.py · 0.13.0 · execução de 2026-09-18
  - tests/chaves_observadas_em_run_real.json · 21 chaves em 1.500 blobs
  - medições de produção e de dev, 2026-08 a 2026-09
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha a prancha 4 com números que têm denominador e data, e sabe
  exatamente onde a decisão perde rastro.
relacionado:
  - 02-arquitetura-e-camadas.md
  - 07-sequencia-e-degradacao.md
  - 09-codigos-decisoes-e-intencao.md
---

# Cascata de decisão, cobertura e lacunas

> **O que este arquivo é:** como um laudo caminha de etapa em etapa, com quanto passa em cada uma, e o que não se consegue medir.
> **O que ele não é:** o desenho das etapas — isso é o `02`; nem a degradação quando algo falha — isso é o `07`.

## 🔴 Desvio de unidade, declarado

O template pede uma **ponte de chaves entre sistemas**. A lib não reconcilia chave: recebe linha e
devolve linha, com o `id_exame` intacto. A estrutura análoga — e é ela que a prancha 4 precisa — é a
**cascata de decisão**: quanto de cada etapa sobrevive para a seguinte.

As colunas são as do template. `acerto` é a fração que avança; `denominador` é sobre o quê.

## A cascata, passo a passo

| passo | de | para | criterio | acerto | denominador | fonte |
|---|---|---|---|---|---|---|
| 1 | lote recebido | texto tratado | conversão, boilerplate, rodapé, normalização | **99,2%** | 10.000 laudos | run `cancer_rim` em dev, 2026-09-16 — 77 saíram com texto vazio: 56 já chegaram vazios, **21 o tratamento zerou** |
| 2 | texto tratado | seções lidas | `segmentation.mode` | **14,2%** (linha com `auto`) · **100%** (linha com `full_doc`) | 4.507 laudos | hepatologia, 2026-09-08 — `segmentation_coverage < 1,0` em 3.867, com 3.196 cabeçalhos descartados |
| 3 | seções lidas | achado léxico | termo casado, não negado, no órgão-alvo | **1,2%** | 10.783 laudos / 61 dias | `cancer_estomago`, homologação 01/05 a 30/06 — 75 relevantes |
| 4 | sem achado léxico | promoção semântica | `semantic_score >= similarity_threshold` | **0,02%** (limiar `0,92`) · **75%** (limiar `0,80`) | 10.000 · 44 | `cancer_rim` em dev 2026-09-16: **2 de 10.000**, máximo observado 0,9654 · ateromatose `0.2.1`: **33 de 44** |
| 5 | score composto | banda do juiz | `uncertainty_band` | **0,06%** (banda `[0,60; 0,97]`) · **81,5%** (banda larga) | 10.783 · 7.500 | `cancer_estomago`: 7 chamadas · ateromatose `0.2.0`: **6.111 de 7.500, todos sem achado** |
| 6 | juiz chamado | decisão mantida | veredito do LLM | **48,1%** removidos | 27 divergências | ateromatose, 2026-09-17 — o juiz cortou 14 de 27 e **não criou nenhuma divergência nova** |
| 7 | decisão | linha devolvida | invariantes de saída | **100%** | todas | `output_invariants`, gate da suíte |

🔴 **O passo 5 é o achado que decide arquitetura.** A mesma lib, com a mesma régua, chama o juiz em
**0,06% ou em 81,5% dos laudos** dependendo de um único par de números na configuração. Não há
outro parâmetro no sistema com essa alavancagem — nem sobre qualidade nem sobre custo.

⚠️ **E o passo 4 é a via que a banda não alcança.** Quando a semântica promove, ela transforma
`fl_relevante` de 0 em 1 **sem passar pelo juiz**, porque a arbitragem só ocorre dentro da banda.
Estreitar a banda para conter o passo 5 **abre** o passo 4. Nenhuma banda fecha os dois.

## Cobertura do contrato de saída

| campo | base_da_medicao | cobertura | data_da_medicao | consequencia_se_vazio |
|---|---|---|---|---|
| chaves emitidas no blob | 1.500 blobs de produção | **21 chaves** observadas; o contrato declarava menos | 2026-09 | consumidor quebra ao ler campo não declarado |
| `llm_prompt_tokens` · `llm_completion_tokens` | 270 chamadas de LLM num dia | **18,1%** — só o caminho do juiz | 2026-09-10 | **221 das 270 chamadas do dia sem contabilidade nenhuma** |
| `semantic_evidence` | 1.500 blobs | emitido e **não declarado** até a `0.12.1` | 2026-09 | quem tipa a saída rejeita a linha |
| `segmentation_coverage` | 4.507 laudos | 100% emitido | 2026-09-08 | sem ele, a perda de 86% da hepatologia seria invisível |
| `embedding_model` | — | **não é emitido** | — | ⚠️ não dá para saber, pelo blob, **qual** modelo resolveu |
| `engine_version` | tabelas de saída de 6 linhas | 100% | 2026-09-15 | sem rastreabilidade de qual motor decidiu |

🔴 **A cobertura do contrato fechou em três ondas**, e a lição está registrada: **verificação por
fixture é estruturalmente insuficiente**. Quatro chaves foram achadas por auditoria; **três só
apareceram no blob de um run real**. Daí o piso versionado.

## Não-regressão — a medição que sustenta a versão

| | resultado |
|---|---|
| corpus | 365 laudos sintéticos × 8 configurações = **2.920 linhas** |
| baseline | `v0.12.3` — a tag **ancestral** da branch |
| resultado | **`sha256` idêntico, byte a byte** |
| pré-condição: régua com negação | 420 linhas exercitam |
| pré-condição: camada semântica | 1.825 |
| pré-condição: ordinal, relevante e não | 900 |
| pré-condição: juiz LLM | 295 |

⚠️ **A pré-condição é impressa junto com o resultado, e não é formalidade:** duas configurações do
próprio harness ficaram **inertes em silêncio** antes de o relatório existir, e o golden passava
verde medindo nada.

## Lacunas

| codigo | o_que_falta | onde_apareceria_na_tela | causa | quem_destrava | o_que_muda_quando_destravar |
|---|---|---|---|---|---|
| `L1` | latência com o modelo real de embeddings | RNFD da prancha 3 | o modelo vive num Volume; local roda `token_overlap` | run no ambiente | dimensiona o custo real da etapa 3 |
| `L2` | latência do juiz com rede | RNFD da prancha 3 | o harness estuba o cliente HTTP, de propósito | run no ambiente | dimensiona o custo dominante do sistema |
| `L3` | prova do caminho `livre` da esteira | bloco de operação da prancha 1 | só o caminho `publicada` foi exercitado | próximo bump | fecha a dúvida sobre publicação em dois feeds |
| `L4` | documentação própria do `monitoring` | faixa de observabilidade da prancha 1 | nunca foi escrita | dono da lib | 6 módulos consumidos por 8 arquivos saem do escuro |
| `L5` | `embedding_model` não é emitido no blob | prancha 2, bloco da semântica | campo não declarado no contrato | bump da lib | passa a ser possível provar **qual** modelo decidiu |
| `L6` | tokens da camada quantitativa | coluna de custo | só `llm_router_step` os escreve | `0.14.0` · `P3` | fecha 221 das 270 chamadas diárias sem registro |

## Bloqueios de entrega

| codigo | bloco_ou_endpoint_afetado | lacuna_que_causa | nasce_como |
|---|---|---|---|
| `BL-D-01` | prancha 5 — contrato de **dados** de saída | fora do escopo deste pacote por decisão: descrevemos a **API Python** | 🔴 **não desenhar**; apontar para `contrato-saida-0.12.1` (PR 7228) e para o card `283647` |
| `BL-D-02` | RNFD de latência na prancha 3 | `L1` e `L2` | 🟡 desenhar o requisito **com `[NAO INFORMADO]` visível**, não omitir a linha |
