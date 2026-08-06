# Entrega — Transplante de Pulmão na plataforma NLP

**Data:** 2026-08-06 · **Para:** MLOps · **Status:** validado, pronto para o fluxo diário

> Comportamentos da plataforma que valem para **qualquer especialidade** estão no documento
> *Plataforma NLP — observações de uso e um pedido*, anexo ao mesmo card. Aqui só o que é do pulmão.

---

## 1. O que está sendo entregue

| item | valor |
|---|---|
| especialidade | `transplante_pulmao` |
| config | `plataform/config/speciality/ntb_ia_transplante_pulmao_config.py` |
| branch | `feature/validacao-plataforma` (`fabrica-ia-nlp-platform`) |
| `config_version` | `0.1.2-pulmao-failover` |
| `nlp_engine` | `0.7.5` |
| perfil | quantitativo puro — limiares de VEF1/CVF/DLCO, `on_met: promote`. Sem embeddings, sem juiz de relevância |

---

## 2. Validação — 1:1 contra a base ouro

Execução em `dev`, janela **2026-06-01 a 2026-06-30**, cruzada por `id_exame` com a base ouro.

| | |
|---|---|
| laudos comparados | **1.852** (coorte integral — nenhum ausente) |
| TP · FP · FN · TN | **126 · 0 · 0 · 1.726** |
| precisão · recall · MCC | **1,0000 · 1,0000 · 1,0000** |
| **concordância** | **100,00%** |
| relevantes sem `findings` | **0** |

**Nenhuma divergência.** A plataforma nova reproduziu o resultado homologado exame por exame — validando de uma vez a lib, o runner novo e a config adaptada.

Resultado **reconfirmado na `0.7.5`** com o lote completo, após a inclusão das colunas de achados (seção 3): números idênticos aos da validação original em `0.7.1`. As colunas novas não alteram nenhuma decisão — era o requisito.

---

## 3. Colunas de achados (`nlp_engine >= 0.7.5`)

Atende ao pedido de ter os achados explícitos por laudo, sem parsear o `exm_laudo_resultado`.

| coluna | conteúdo | exemplo |
|---|---|---|
| `findings` | nome da doença em rastreio | `DPOC / doença supurativa; Doença intersticial` |
| `findings_spans` | doença + medida + limiar que promoveu | `DPOC / doença supurativa (36.0% < 40.0); Doença intersticial (cvf_pct_previsto 53.0% < 70.0)` |
| `findings_match` | trecho do laudo, para auditoria clínica | `DPOC / doença supurativa: Volume expiratório forçado no primeiro segundo reduzido (VEF1 36% previsto).` |

**Nada precisa mudar do lado de vocês.** As colunas são criadas automaticamente — o persister grava em `append` com `mergeSchema`, e chave nova vira coluna. Validado em execução real, sem intervenção.

Três garantias de contrato:

- **Só achado positivo entra.** Negado ou não atendido fica de fora.
- **Ausência é string vazia, nunca nulo.** O consumidor não precisa tratar `null`.
- **Ordem determinística.** A mesma decisão gera a mesma string entre execuções.

O nome da doença vem de `label`, declarado por critério na config. É **opcional**: sem ele a coluna cai no identificador técnico do critério (ex.: `funcao_intersticial`), sem quebrar nada. Especialidades sem critérios quantitativos recebem as colunas alimentadas pelos achados léxicos, também sem mudança de config.

⚠️ Nem toda especialidade tem doença-alvo declarada — quem tiver se beneficia do nome; as demais seguem com o identificador.

---

## 4. Base ouro entregue junto

Anexo: **`base-ouro-transplante-pulmao-v1-2026-07-23.csv`**

1.852 linhas · colunas `id_exame, verdade` · 126 positivos / 1.726 negativos · **sem texto de laudo**, por LGPD.

Origem: gabarito humano construído com a especialista (388 laudos revisados) mais amostra de recall (1.470). O run homologado bateu MCC 1,000 contra esse gabarito, então a saída dele equivale ao rótulo humano — é o que está no arquivo.

Serve para reproduzirem a validação de forma independente e como referência de regressão em qualquer mudança de lib, runner ou infraestrutura.

---

## 5. Ponto específico do pulmão

### `gold_query` foi convertido para `gold_filter.keywords`

A versão homologada usava `gold_query` — chave que **esta plataforma não lê**. É o caso que a doc de vocês (`boas-praticas/04`) já marca com 🔴, citando justamente este config como exemplo de especialidade rodando sem filtro nenhum.

As 5 alternativas do regex original viraram 5 keywords. Equivalência verificada no lake: **4.517 = 4.517** em jun+jul/2026.

O lookbehind `(?<!ergo)espiromet`, que evita ergoespirometria, foi preservado — `rlike` do Spark aceita.

---

## 6. Widgets do run de validação

Só o que difere do padrão descrito na nota geral:

| widget | valor |
|---|---|
| `specialty` | `transplante_pulmao` |
| `start_date` / `end_date` | `2026-06-01` / `2026-06-30` |
| `embedding_enable` | **`false`** — a config tem `use_embeddings: False`; ligar só carregaria o modelo no driver sem uso |

**Conferência rápida do lote:** entrada com **2.085 linhas** (1.852 com texto + 233 vazios) e **126 relevantes** na saída.

---

## 7. Uso do LLM

`llm_called: true` em **1.525 de 2.085** (73%). Taxa alta é esperada: sendo quantitativo puro, todo laudo com menção a espirometria vai para o extrator de medidas.

4 laudos (0,19%) tiveram `parse_failed:invalid_json` — detalhado na nota geral, seção 6. Sem impacto aqui: os 4 são negativos no gabarito e na saída nova.
