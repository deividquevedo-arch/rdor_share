# Alinhamento — configuração das especialidades e o bloco `runtime`

> **Para:** time de plataforma / MLOps · **De:** Ciência de Dados · **15/09/2026**
>
> **Natureza deste documento:** pedido de acordo, não notificação de mudança. **Nada será alterado
> nas configs antes deste alinhamento.** O objetivo é que o trabalho entre no backlog do time com
> capacity prevista, e que a sequência seja executada uma especialidade por vez, com medição.

---

# 1. O pedido, em uma página

**O que existe hoje:** o bloco `runtime` das configs de especialidade **sobrepõe** o bloco `nlp` no
carregamento, e é o resultado dessa sobreposição que o motor executa. A documentação da plataforma
afirma o contrário, em dois documentos. Consequência concreta: há configuração em produção que
**declara um valor e executa outro**, num ponto que decide entrega.

**O que propomos:** eliminar o bloco `runtime` — ele perdeu a função que tinha — e deixar a decisão
inteiramente no bloco `nlp`, que é onde o cientista calibra, testa e valida.

**O que pedimos:**

| # | pedido | de quem |
|---|---|---|
| 1 | **Acordo com a sequência da §5** e entrada no backlog, com capacity prevista | plataforma |
| 2 | **Correção da documentação** — 5 divergências entre a SPEC 27 e o código, mais o guia de criação de especialidade | plataforma |
| 3 | **Desbloqueio do PR 7228** ou a indicação do que falta | plataforma |
| 4 | **Revisão dos PRs de config**, um por especialidade, na ordem acordada | plataforma |

**O que entregamos antes de pedir qualquer coisa:** a nossa metade inteira — declarar nas configs o
que hoje vem por sobreposição, remover o bloco, e medir delta zero. Ao fim disso o merge no
`ConfigLoader` fica **inerte**, e removê-lo passa a ser remoção de código sem entrada.

---

# 2. O problema, medido

## 2.1 O `runtime` sobrepõe, e decide

`plataform/config/ntb_ia_loader.py:104-113`:

```python
self.llm_router = nlp_config.get("llm_router", {})   # base: o bloco clínico
runtime_llm = runtime_config.get("llm_router", {})
if runtime_llm:
    self.llm_router.update(runtime_llm)              # runtime SOBREPÕE
...
nlp_config["llm_router"] = self.llm_router           # e é isto que o motor lê
```

## 2.2 O efeito em produção, hoje

| config | `nlp.llm_router.enabled` | `runtime.llm_router.enabled` | efetivo |
|---|---|---|---|
| hepatologia | *ausente* → a lib assume `False` | `True` | **ligado** |
| transplante_pulmao | **`False` explícito** | `True` | **ligado** |
| cancer_estomago | `True` | `True` | ligado — coerente |

Run de produção das últimas 24 h:

| linha | laudos | `llm_router_mode` | juiz chamado |
|---|---|---|---|
| hepatologia | 12.184 | `llm` em 12.184 | **156** |
| transplante_pulmao | 312 | `llm` em 298 · `skipped_deterministic` em 14 | 0 |

🔴 **No transplante a configuração declara `False` e o juiz roda.** Não é omissão — é contradição
entre o que a config diz e o que executa.

🔴 **E na hepatologia não é só liga/desliga:** o `runtime` sobrepõe também `fallback_policy`, de
`keep_current` para **`positive_in_band`** — o comportamento **quando a chamada ao LLM falha**.

## 2.3 O bloco perdeu a função que tinha

O `runtime` existia para permitir sobreposição **em tempo de execução**, sem alterar código, durante
testes e validação. Essa possibilidade não existe mais: o runner declara **15 widgets**
(`specialty`, `environment`, `persist`, `limit_rows`, `reprocess_enable`, `nlp_engine_version`,
`embedding_enable`, `logger_level`, `mlflow_*`, `date_range_*`, `model_execution_date`,
`persist_input`, `start_date`, `end_date`) e **nenhum alimenta `llm_router` nem `runtime`**.

**Sem origem dinâmica, deixou de ser uma camada e virou um segundo lugar estático declarando a mesma
chave — sobrepondo a primeira em silêncio.**

ℹ️ E apenas `runtime.llm_router` é lido. O `runtime.profile`, declarado em 5 das 6 configs, **nunca
é consultado**.

---

# 3. A documentação afirma o contrário — e é o que faz o erro se repetir

| # | onde | afirma | o código faz |
|---|---|---|---|
| 1 | SPEC 27 §2.1 e §6.1 | `runtime` é **ignorado** — *"nada neste pipeline lê `runtime`"* | `runtime` **sobrepõe** `nlp` |
| 2 | SPEC 27 §7 | `transplante_pulmao` usa `gold_query` e **nenhum filtro textual é aplicado** | usa `gold_filter.keywords` com 5 regex — **já corrigido** |
| 3 | SPEC 27 §7 | regex com lookbehind: *"não há caminho por config"* | `(?<!ergo)espiromet` está **em produção** |
| 4 | SPEC 27 §7 | catálogo efetivo é `diamond_ia_dev` / `diamond_ia_hml` / `diamond_ia` | é **`diamond_fabrica_ia_dev` / `_hml` / `diamond_fabrica_ia`** |
| 5 | SPEC 27 §7 | *"o CONFIG é um dicionário literal, e nada mais"* | a config do ca-cólon termina com `assert` — e é melhor assim |

⚠️ **O mais grave não é a SPEC — é o guia.** `boas-praticas/02-criando-uma-nova-especialidade.md`,
Passo 6, instrui a preencher `runtime` *"para documentação, sabendo que não tem efeito"*, com exemplo
literal `'llm_router': {'enabled': False, ...}`. **Quem seguir o guia ao criar uma linha nova desliga
o juiz em silêncio.**

⚠️ A afirmação já se propagou para comentários dentro das configs de `cancer_rim`
(*"é ESTA chave que o motor lê (não a de runtime)"*) e `cancer_estomago`.

ℹ️ **A nº 4 não é cosmética:** `diamond_ia_hml` é o catálogo do workspace antigo. Quem a usa para
saber onde o dado foi, erra.

---

# 4. O PR 7228 está parado esperando este alinhamento

O PR `docs(usage): o que a 0.12.1 do motor mudou para quem consome`
(`docs/contrato-saida-0.12.1` → `hml`) está aberto desde **08/09**, com voto **`-10` de revisor
obrigatório**, e o retorno registrado foi:

> *"abrir uma história para avaliar o que quebra ou não a partir da mudança"* · *"favor abrir uma
> história e levar para o próximo refinamento marcando o que deve ser mudado no contexto do
> NLP-Platform"*

**Este documento é o conteúdo dessa história.** O PR documenta o contrato de saída da `0.12.1`; as
correções da §3 e a sequência da §5 são o que muda no contexto da plataforma.

---

# 5. A sequência — nossa metade primeiro, e sem ping-pong

| # | passo | quem | por que nesta ordem |
|---|---|---|---|
| 1 | **Este alinhamento** e entrada no backlog | ambos | nada se altera antes |
| 2 | Declarar em `nlp.llm_router` o que hoje vem por sobreposição | **nós** | delta zero por construção |
| 3 | Remover `runtime.llm_router` das configs | **nós** | já é no-op depois do passo 2 |
| 4 | Remover `runtime.profile` das configs | **nós** | nunca foi lido |
| 5 | Medir e comprovar delta zero | **nós** | prova, não suposição |
| 6 | Remover o merge do `ConfigLoader` | **plataforma** | a essa altura é código sem entrada |
| 7 | Corrigir SPEC 27 e `boas-praticas/02` | **plataforma** | a documentação passa a descrever o estado final |

🔴 **Inverter a ordem desliga o juiz em produção.** Se o passo 3 ou 6 acontecer antes do 2, a
hepatologia perde as **156 arbitragens diárias** do juiz **e** muda a política de falha — sem erro e
sem log.

**Os passos 2 a 5 são executados uma especialidade por vez**, com PR próprio e medição própria.

---

# 6. O que muda em cada config

## 6.1 Faixa A — delta zero

### A1 · Declarar o efetivo — 5 chaves em 3 arquivos

| config | chave | declarado em `nlp` | o que executa |
|---|---|---|---|
| **hepatologia** | `enabled` | *ausente* | **`True`** |
| | `api_key_env` | *ausente* | `DATABRICKS_TOKEN` |
| | `fallback_policy` | `keep_current` | **`positive_in_band`** |
| **tirads** | `enabled` | *ausente* | `False` |
| **transplante_pulmao** | `enabled` | **`False`** | **`True`** |

`cancer_estomago`, `cancer_rim` e `reumatologia` já declaram o efetivo e não mudam.

⚠️ **Declara-se o que executa.** Se `positive_in_band` é a política correta para a hepatologia é
outra discussão, que se resolve com medição — ver B2.

### A2 · Remover o bloco `runtime` — as 6 configs

### A3 · Remover os blocos não lidos

| config | `catalog` | `monitoring` | `distribution` | `runtime.profile` | `data.legacy` |
|---|---|---|---|---|---|
| hepatologia | sim | sim | sim | sim | **sim** |
| tirads | sim | sim | sim | sim | — |
| transplante_pulmao | sim | sim | — | sim | — |
| cancer_estomago | sim | sim | sim | sim | — |
| cancer_rim | — | — | — | sim | — |
| **reumatologia** | — | — | — | — | — |

ℹ️ **A `reumatologia` já está limpa** — é a referência do formato final.

### Como o delta zero é comprovado

Mesma janela, antes e depois: `llm_router_mode` por laudo, contagem de chamadas ao juiz
(`llm_called` de topo) e `fl_relevante` — os três idênticos. **Com a pré-condição impressa:** quantos
laudos exercitaram o caminho do LLM. Sem isso, "nenhuma diferença" pode significar que o caminho não
rodou.

## 6.2 Faixa B — exige medição, sobe depois e em PR próprio

| # | item | config | o que falta | bloqueio |
|---|---|---|---|---|
| **B1** | `segmentation.mode: auto` descarta 86% dos laudos | hepatologia | A/B contra `full_doc` na mesma janela | nenhum |
| **B2** | promover a config calibrada **`0.1.14-hep-v3`** | hepatologia | revalidar: a validação é de 24/07 contra `nlp_engine 0.6.2`; produção roda `0.12.3` | nenhum |
| **B3** | `gold_filter` não captura punção — 67 exames em 16 dias não chegam ao motor | tirads | run medido: volume adicional e quantos passam a entregar | nenhum |
| **B4** | `waive` para PAAF — config `0.9.0-tirads` pronta | tirads | — | aval de chave nova e dois campos de saída |
| **B5** | `embedding_model` com caminho literal de volume | 4 configs | — | mecanismo por ambiente |

🔴 **Nenhum item da Faixa B entra no PR da Faixa A.** Misturar mudança medida com declaração de
estado destrói a única propriedade que torna a Faixa A barata de revisar: o delta zero.

ℹ️ **B4 e B5 já têm card próprio** e não fazem parte deste pedido.

---

# 7. O que se pede, explicitamente

1. **Concordância com a sequência da §5**, e entrada no backlog com capacity prevista para os passos
   6 e 7.
2. **Correção das 5 divergências da §3**, mais o Passo 6 do guia de criação de especialidade.
3. **Desbloqueio do PR 7228**, ou a indicação objetiva do que falta nele.
4. **Revisão dos PRs de config da Faixa A**, um por especialidade, na ordem acordada.

**Compromisso deste lado:** nenhuma config é alterada antes do acordo; cada PR carrega a medição que
o sustenta; e a Faixa B só é proposta quando a conclusão de cada item estiver pronta.
