# Entrega — Transplante de Pulmão na plataforma NLP

**Data:** 2026-08-04 · **Para:** MLOps · **Status:** validado, pronto para o fluxo diário

> Comportamentos da plataforma que valem para qualquer especialidade estão em
> `docs/motor-nlp/_fundacao/notas-plataforma-nlp-mlops.md`. Aqui só o que é do pulmão.

---

## 1. O que está sendo entregue

| item | valor |
|---|---|
| especialidade | `transplante_pulmao` |
| config | `plataform/config/speciality/ntb_ia_transplante_pulmao_config.py` |
| branch | `feature/validacao-plataforma` (`fabrica-ia-nlp-platform`) |
| `config_version` | `0.1.2-pulmao-failover` |
| `nlp_engine` | `0.7.1` |
| perfil | quantitativo puro — limiares de VEF1/CVF/DLCO, `on_met: promote`. Sem embeddings, sem juiz de relevância |

---

## 2. Validação — 1:1 contra a base ouro

Execução em `dev`, janela **2026-06-01 a 2026-06-30**, cruzada por `id_exame` com o run homologado na plataforma antiga (`dt_execucao = 2026-07-23`, mesma `config_version`).

| | |
|---|---|
| laudos comparados | **1.852** |
| TP · FP · FN · TN | **126 · 0 · 0 · 1.726** |
| precisão · recall · MCC | **1,0000 · 1,0000 · 1,0000** |
| **concordância** | **100,00%** |

**Nenhuma divergência.** A plataforma nova reproduziu o resultado homologado exame por exame — validando de uma vez a lib `0.7.1`, o runner novo e a config adaptada.

---

## 3. Base ouro entregue junto

**`docs/motor-nlp/pulmao/dados/base-ouro-transplante-pulmao-v1-2026-07-23.csv`**

1.852 linhas · colunas `id_exame, verdade` · 126 positivos / 1.726 negativos · **sem texto de laudo**, por LGPD.

Origem: gabarito humano construído com a especialista (388 laudos revisados) mais amostra de recall (1.470). O run homologado bateu MCC 1,000 contra esse gabarito, então a saída dele equivale ao rótulo humano — é o que está no arquivo.

Serve para reproduzirem a validação de forma independente e como referência de regressão em qualquer mudança de lib, runner ou infraestrutura.

---

## 4. Ponto específico do pulmão

### `gold_query` foi convertido para `gold_filter.keywords`

A versão homologada usava `gold_query` — chave que **esta plataforma não lê**. É o caso que a doc de vocês (`boas-praticas/04`) já marca com 🔴, citando justamente este config como exemplo de especialidade rodando sem filtro nenhum.

As 5 alternativas do regex original viraram 5 keywords. Equivalência verificada no lake: **4.517 = 4.517** em jun+jul/2026.

O lookbehind `(?<!ergo)espiromet`, que evita ergoespirometria, foi preservado — `rlike` do Spark aceita.

---

## 5. Widgets do run de validação

Só o que difere do padrão descrito na nota geral:

| widget | valor |
|---|---|
| `specialty` | `transplante_pulmao` |
| `start_date` / `end_date` | `2026-06-01` / `2026-06-30` |
| `embedding_enable` | **`false`** — a config tem `use_embeddings: False`; ligar só carregaria o modelo no driver sem uso |

**Conferência rápida do lote:** entrada com **2.085 linhas** (1.852 com texto + 233 vazios) e **126 relevantes** na saída.

---

## 6. Uso do LLM

`llm_called: true` em **1.525 de 2.085** (73%). Taxa alta é esperada: sendo quantitativo puro, todo laudo com menção a espirometria vai para o extrator de medidas.

4 laudos (0,19%) tiveram `parse_failed:invalid_json` — detalhado na nota geral, seção 5. Sem impacto aqui: os 4 são negativos no gabarito e na saída nova.
