# Embeddings no run de `cancer_rim` em dev — o que os dados dizem

> 17/09/2026 · run de **16/09**, `diamond_fabrica_ia_dev.cancer_rim`, 10.000 laudos,
> config `0.6.0-cancer_rim`, engine `0.12.3` · branch `feature/embedding` da `fabrica-ia-nlp-platform`

**Relato:** *"não está retornando embedding"*.

**Diagnóstico: a camada semântica rodou, com modelo real, em 100% dos laudos. Não há falha.**

---

# 1. O que a trilha mostra

| | |
|---|---|
| laudos | 10.000 |
| com `[sentence_transformers]` | **10.000** |
| com `token_overlap` | **0** |
| com `FALLBACK:` | **0** |

Trilha: `neutral — best 0.91 (below threshold) [sentence_transformers]`

✅ **Zero degradação.** É a primeira vez neste projeto que a camada semântica roda com modelo real
numa linha inteira — em produção a mesma camada falha em 86% a 100% dos laudos nas quatro linhas
que a declaram (card `305810`).

# 2. Por que nada foi promovido

O `cancer_rim` declara `similarity_threshold: 0.92`. A distribuição dos 10.000:

| medida | valor |
|---|---|
| máximo | **0,9654** |
| média | 0,7005 |
| p50 | 0,7177 |
| p99 | 0,8443 |
| p99,9 | 0,9002 |
| **≥ 0,92** | **2 laudos** |
| ≥ 0,90 | 11 |
| ≥ 0,85 | 85 |

**Dois laudos em dez mil cruzam o limiar.** É o comportamento que a config declara, com a razão
escrita ao lado do número:

> *"CONSERVADOR (ref. ca-estomago): a 0.92 NENHUM candidato da população-nova é promovido sem
> validação humana (medido: 0/127 na amostra de 26-31/08); candidatos ficam LOGADOS na telemetria
> (`semantic_score`) p/ calibrar a v0.6.x com dado real"*

**"Não retorna embedding" é, aqui, "não promove"** — e não promover era o objetivo do 0,92.

ℹ️ Os candidatos que aquele comentário previa **agora existem**: são os 85 acima de 0,85, com score
gravado laudo a laudo. É o insumo para calibrar a `0.6.x`, que até ontem não existia.

# 2.1 🔴 E os dois que cruzaram foram entregues SEM evidência de regra e SEM juiz

Os únicos 2 laudos acima de 0,92:

| `fl` | `decision_source` | `n_positive_spans` | `semantic_score` | termo casado | `llm_called` | score calibrado |
|---|---|---|---|---|---|---|
| **1** | `hybrid_calibrated` | **0** | 0,9654 | `tumor renal` | **false** | 0,546 |
| **1** | `hybrid_calibrated` | **0** | 0,9298 | `neoplasia de rim` | **false** | 0,526 |

**A camada semântica promoveu sozinha.** E o juiz não arbitrou porque o score **calibrado** fica
abaixo do piso da banda do `cancer_rim` (`uncertainty_band: [0.75, 0.95]`) — a trilha registra
`llm_judge: not_called — calibrated_score outside uncertainty band`.

🔴 **A pendência de arbitragem nunca é resolvida.** No modo `hybrid` o código marca a promoção
semântica como *"pendente de arbitragem — ver `step_decide_llm`"*, mas a arbitragem só ocorre
dentro da banda. Fora dela, a promoção sai entregue sem que ninguém tenha conferido.

**É o espelho do P0-29** (card `283648`), medido em 15/09 na hepatologia: lá o **juiz** promove sem
evidência de regra; aqui a **semântica** promove sem evidência de regra e sem juiz.

⚠️ **E a proveniência se perde.** `decision_source` sai `hybrid_calibrated` — sobrescrito pelo passo
seguinte —, não "promovido pela semântica". `semantic_promoted` existe no estado interno e **não é
emitido**. A única forma de identificar esses casos no dado é cruzar `n_positive_spans = 0` com
`semantic_score >= similarity_threshold`:

```sql
SELECT count(*)
FROM   <saida>
WHERE  fl_relevante = 1
  AND  cast(get_json_object(exm_laudo_resultado,'$.n_positive_spans') AS INT) = 0
  AND  get_json_object(exm_laudo_resultado,'$.llm_called') = 'false'
  AND  cast(get_json_object(exm_laudo_resultado,'$.semantic_score') AS DOUBLE) >= <threshold>
```

ℹ️ **Em dev, com 2 casos em 10.000, o volume é pequeno — mas a via está aberta.** Ligar embeddings
em produção, onde hoje tudo degrada para `token_overlap`, transforma isso numa população real.
**Dimensionar antes de promover a mudança.**

# 2.2 🔴 Os casos relatados existem — e apontam outro defeito, não os embeddings

O relato descreve laudos com `semantic_score = 0`, `semantic_matched_term = ""`,
`semantic_evidence = ""`. **Eles existem: são 77 em 10.000 (0,77%).**

⚠️ **Mas a combinação relatada não se confirma.** Nenhum dos 77 tem `uncertainty_band_hit = true` —
a interseção é **zero**. O `band_hit` e os campos semânticos zerados vêm de laudos diferentes.

**A causa dos 77 não é a camada semântica. É que o texto tratado ficou VAZIO:**

| | |
|---|---|
| laudos com `exm_laudo_texto_tratado` vazio | **77 de 77** |
| destes, já vinham com o bruto vazio | **56** — nada a fazer |
| **destes, tinham texto bruto e o tratamento zerou** | **21** |
| tamanho desses 21 | 158 a 64.512 caracteres, média **61.448** |
| `fl_relevante` dos 77 | **0**, em silêncio |

Com texto tratado vazio, `_sentence_chunks` devolve lista vazia e
`_evidence_with_sentence_transformers` retorna `SemanticEvidence(0.0, "", "sentence_transformers",
...)` — score zero, termo vazio, evidência vazia. **O backend aparece porque o modelo carregou; o
zero vem de não haver texto para comparar.**

## O que foi descartado, e como

- 🔴 **Não é truncamento.** Há laudos de **2,5 milhões** de caracteres no mesmo run que tratam
  normalmente. O 64.512 era o maior dos 21, não um teto.
- 🔴 **Não é o formato HTML do editor.** Os 21 são HTML de um editor (`<div data-wate-document=""
  data-wate-version="1.0">`). Reproduzido localmente com HTML sintético no mesmo molde — div único,
  aninhado, e 400 blocos numa linha só — **o `to_plain` trata os quatro casos corretamente**.
- 🔴 **Não é regra de boilerplate da config.** O `cancer_rim` declara
  `text_pipeline.trailing_line_patterns: []`.

⚠️ **A causa está no conteúdo específico desses 21, e a inspeção é PHI** — tem de acontecer no
Databricks, não em máquina local. O caminho é rodar `to_plain` sobre eles e observar em que etapa
o texto zera: conversão de HTML, `ftfy`, ou descarte de linha.

ℹ️ **É reincidência de uma classe conhecida:** laudo que vira texto tratado vazio e sai `fl = 0`
**sem erro e sem log**. Corrigido uma vez na `0.9.1` (regra de boilerplate descartando a linha
única), e a suspeita geral — *"regra por LINHA + laudo numa linha"* — ficou registrada em memória.
Aqui reaparece na `0.12.3`, por outra via.

🟡 **Sem card.** Dimensionar antes: 0,77% do run, e só 0,21% (os 21) são perda de conteúdo real —
os outros 56 já chegam vazios, o que é problema da montagem da entrada, não do motor.

# 3. ⚠️ O que os dados NÃO provam

**Que foi a resolução do Model do Unity Catalog que fez o modelo carregar.**

O `embedding_model` **não é emitido no blob** — saiu do núcleo da saída na `0.9.0`, e o que ficou na
trilha é apenas o *backend*. E em **dev** o path antigo do Volume também resolve: medido zero
`FileNotFoundError` em 109.563 laudos de três linhas. Os dois caminhos produzem o mesmo
`[sentence_transformers]`.

**O que distingue está no log do driver**, na linha que o próprio `_resolve_embedding_model` emite:

```
embedding: 'mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm' -> '/tmp/...' (offline).
```

Presente essa linha, foi o UC. Ausente, foi o path do Volume e a mudança não foi exercitada.

# 4. 🔴 A query que circulou estava errada

O acessor usado era `$.decision_trail.semantic`. **O correto é
`$.decision_trail.steps.semantic`** — a trilha é `{findings, steps, outcome}`, e o passo semântico
vive dentro de `steps`.

⚠️ **Com o caminho errado o campo vem NULO, e a leitura inverte:** `degradou = 0` parece sucesso e
`modelo_ok = 0` parece falha. Foi o que aconteceu na primeira leitura deste mesmo run.

**Query correta:**

```sql
SELECT count(*) laudos,
       sum(CASE WHEN get_json_object(exm_laudo_resultado,'$.decision_trail.steps.semantic')
                     LIKE '%[sentence_transformers]%' THEN 1 ELSE 0 END) modelo_ok,
       sum(CASE WHEN get_json_object(exm_laudo_resultado,'$.decision_trail.steps.semantic')
                     LIKE '%FALLBACK:%' THEN 1 ELSE 0 END) degradou,
       round(max(cast(get_json_object(exm_laudo_resultado,'$.semantic_score') AS DOUBLE)),4) maior
FROM   diamond_fabrica_ia_dev.<schema>.tb_mod_diamond_<linha>_saida_v0
WHERE  dt_execucao_modelo >= current_date() - 1
```

**Aceite:** `modelo_ok = laudos` e `degradou = 0`.

⚠️ `max()` sobre a **string** da trilha é máximo lexicográfico, não numérico — o `0.91` que aparece
no texto não é o maior score. O maior é 0,9654, e sai do `semantic_score` convertido para `DOUBLE`.

# 5. Revisão da abordagem da branch

✅ **O desenho está correto**, e um ponto sutil está certo por mérito e não por sorte: o motor roda
no **driver** (`queue_df.toLocalIterator`), então o diretório em `/tmp` e as variáveis
`HF_HUB_OFFLINE`/`TRANSFORMERS_OFFLINE` ficam no processo que de fato carrega o modelo. Numa
execução distribuída por UDF, nada disso valeria — o path local não existiria no executor.

✅ **Falhar alto em vez de degradar** é a decisão certa, e está alinhada com o que já custou caro
aqui: `embedding_model` inválido cai em `token_overlap` sem erro e sem log, e o perfil que executa
deixa de ser o que a config declara.

✅ **O cache por cluster** (`<name>/<version>`) evita rebaixar ~470 MB a cada run, e limpa o
diretório quando o marcador não é encontrado — trata download interrompido.

**Dois pontos a conferir, nenhum bloqueante:**

- 🟡 **A detecção da raiz do modelo** (`_ST_MARKERS`) cai para `config.json` quando não acha
  `modules.json`. Num artefato que não seja `sentence-transformers` puro, isso pode devolver a
  pasta do transformer cru — que carrega, com pooling default, e produz score **diferente** sem
  erro. Vale confirmar que o artefato registrado no UC tem `modules.json`.
- 🟡 **`_load_sentence_model` na lib tem `@lru_cache` chaveado pelo path.** Como o path inclui a
  versão do Model, trocar a versão invalida o cache corretamente. Mas re-registrar a **mesma
  versão** com conteúdo diferente reusaria o modelo antigo na mesma sessão.

# 6. Encaminhamento

1. **Confirmar no log do driver** qual caminho resolveu (§3). É o que fecha se a mudança foi
   exercitada.
2. **Levar os 85 laudos com score ≥ 0,85 para revisão**, que é o uso previsto pela própria config.
   O limiar de 0,92 passa a ter dado para ser calibrado — ele foi fixado sem população medida.
3. **Confirmar `modules.json` no artefato do UC** (§5).
4. A mudança **não** resolve o card `305810` sozinha: ali o problema é o caminho em **produção**, e
   este run é dev.
