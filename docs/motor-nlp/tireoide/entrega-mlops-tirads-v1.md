# Entrega — TI-RADS **V2** na plataforma NLP

**Data:** 2026-08-10 · **Para:** MLOps · **Status:** validado, pronto para o fluxo diário

> ⚠️ **Escopo: régua V2** — nódulo/cisto ≥ 1 cm, TI-RADS 4/5, massa, linfonodo suspeito e bócio
> nodular, sobre exames de imagem. A **V3** (cintilografia e exame de sangue) está **em
> desenvolvimento** e **não faz parte desta entrega**.

> Comportamentos da plataforma que valem para **qualquer especialidade** estão no documento
> *Plataforma NLP — observações de uso e um pedido*, anexo ao mesmo card. Aqui só o que é do TI-RADS.

---

## 1. O que está sendo entregue

| item | valor |
|---|---|
| especialidade | `tirads` |
| config | `plataform/config/speciality/ntb_ia_tirads_config.py` |
| origem | `hml` (`fabrica-ia-nlp-platform`) — subida pelo PR 7009 |
| `config_version` | `0.1.0-tirads-rads-v22.11-v2` |
| `nlp_engine` | `0.8.0` |
| perfil | TI-RADS (`rads_extraction`) + achados clínicos + embeddings (hybrid) + juiz LLM + quantitativo |

---

## 2. Validação contra a base ouro

Execução em `dev`, janela **2026-06-25 a 2026-06-27**, cruzada por `id_exame` com a base ouro V2.

### Base ouro entregue junto

Anexo: **`base-ouro-tirads-v2-2026-07-24.csv`**

895 linhas · colunas `id_exame, verdade_v1, verdade_v2, reclassificado` · **sem texto de laudo**, por LGPD.

⚠️ **Use a coluna `verdade_v2` e considere só os valores `0` e `1`:**

| `verdade_v2` | linhas |
|---|---|
| `0` (não relevante) | 459 |
| `1` (relevante) | 127 |
| `PENDENTE` | 300 |
| `?` | 9 |

São **586 ids resolvidos**; os 309 restantes não foram fechados clinicamente e devem ser excluídos de qualquer cálculo. A coluna `verdade_v1` é a régua anterior — 7 rótulos foram reclassificados de `1` para `0` na V2 (marcados em `reclassificado`), com dupla confirmação. Os critérios e a auditoria da reclassificação estão registrados do nosso lado; se precisarem, pedimos e enviamos.

| | plataforma nova (`nlp_engine 0.8.0`) | referência homologada |
|---|---|---|
| laudos processados | 2.709 | — |
| coorte presente | **440 de 586 (75,1%)** | 586 |
| positivos cobertos | **121 de 127** | 127 |
| TP · FP · FN · TN | 121 · 3 · **0** · 316 | 127 · 4 · 0 · 455 |
| precisão | **0,9758** | 0,9695 |
| recall | **1,0000** | 1,0000 |
| MCC | **0,9832** | 0,9803 |

**Zero falso-negativo**, precisão e MCC acima da referência.

**Estabilidade entre versões da lib.** O mesmo lote foi processado em `0.7.1`, `0.7.5`, `0.7.6` e
`0.8.0`, com resultado **idêntico**. As colunas de achados e as correções desse intervalo são
aditivas: nenhuma alterou decisão. É o requisito que queríamos demonstrar antes de entregar.

---

## 3. Uma correção de seleção durante a validação

A primeira execução processou 2.162 laudos e cobriu só 376 da coorte — **25 positivos não chegaram ao motor** (19,7% do recall). Não era erro de régua: sobre o que entrava, o motor acertava tudo.

Causa: na plataforma nova as keywords casam o **nome do exame**; no runner legado casavam o **texto do laudo**. Os 25 eram TC de pescoço, US de pescoço/cervical e biópsia de linfonodo — nenhum tem "tireoide" no nome.

Correção, seguindo a lista de exames da spec clínica:

```
['tireoide', 'tireóide', 'pescoco', 'pescoço']
```

Resultado: 2.162 → 2.711 laudos (+25%), positivos cobertos de 102 → **121**, recall mantido em 1,000. `ultrassom tireoide` foi removida — era substring de `tireoide`, no-op.

Ficaram fora de propósito: `cervical` (+1.120 exames para 3 positivos, casa "coluna cervical") e `biopsia`/`punc` genéricos (+1.996 para 4). Nenhum consta da spec.

---

## 4. Colunas de achados (`nlp_engine >= 0.7.6`)

Três colunas planas, para saber o que o motor encontrou **sem parsear o `exm_laudo_resultado`**:

| coluna | conteúdo | exemplo |
|---|---|---|
| `findings` | nome clínico do achado | `Nódulo; Cisto` |
| `findings_spans` | idem, quantificando a evidência | `Nódulo(2); Cisto(1)` |
| `findings_match` | nome + termo casado, para auditoria | `Nódulo: nódulos, nódulo sólido` |

**Nada precisa mudar do lado de vocês** — as colunas são criadas automaticamente pelo `mergeSchema`
do persister. Validado em execução real.

Os sete achados saem com **nome clínico**, não com o identificador técnico:

`Nódulo` · `Cisto` · `Massa` · `Linfonodomegalia` · `Tumor` · `Bócio` · `Hipertireoidismo`

Distribuição observada no run: `Nódulo` 111 · `Cisto` 26 · `Nódulo; Cisto` 13 · `Linfonodomegalia` 10.

⚠️ **A string não é chave de agrupamento.** A ordem varia com a quantidade de evidências, então
`Nódulo; Cisto` e `Cisto; Nódulo` são o mesmo par. Para agregar, quebre por `; `.

ℹ️ Relevante com `findings` vazio é **informação, não falha**: significa decisão sem lastro
determinístico — veio do juiz LLM ou da semântica. No run foram 5 de 124.

---

## 5. Pontos específicos do TI-RADS

### 4.1 `column_map` foi reescrito por inteiro

A versão homologada lia da **canônica** (`an`, `Laudo`, `dataexame`, `modalidade`, `tipoexame`) — nenhuma dessas colunas existe na Gold. Foi o único dos configs portados que exigiu reescrita completa desse bloco.

### 4.2 `exm_mod` não tem equivalente na Gold

A canônica tinha `modalidade` (US/CT/RM), derivada no pipeline dela. A Gold não tem, então usamos `cod_procedimento`/`tp_codigo_procedimento` e a calibração cai no neutro — ela só ajusta score em US/RM.

Efeito esperado: divergência em casos de borda. Medido no piloto V3 do tireoide: **1 caso em 675**, com confiança 0,654.

### 4.3 `embedding_enable` precisa ficar `true`

Diferente do pulmão. A config do TI-RADS tem `use_embeddings: True` e `decision_mode: hybrid`; com o widget em `false` a decisão híbrida não acontece.

---

## 6. Pendência clínica, não técnica

6 positivos de 127 seguem fora: **"região cervical/supraclavicular"** e **"PAAF/biópsia de linfonodo"**. Não constam da lista de exames da spec.

Incluí-los custaria +3.100 exames (2,7× o lote) para ganhar 6. Está com o especialista.

---

## 7. Widgets do run de validação

Só o que difere do padrão descrito na nota geral:

| widget | valor |
|---|---|
| `specialty` | `tirads` |
| `date_range_enable` | **`true`** — com `false` as datas são ignoradas em silêncio |
| `start_date` / `end_date` | `2026-06-25` / `2026-06-27` |
| `limit_rows` | **vazio** — o default `100` corta o lote |
| `nlp_engine_version` | `0.8.0` |
| `embedding_enable` | **`true`** — a config usa `decision_mode: hybrid` |

**Conferência rápida do lote:** entrada com **2.709 linhas** e **572 relevantes**.

---

## 8. Bloqueio em aberto: falta o schema

Com a migração `diamond_ia_*` → `diamond_fabrica_ia_*`, o schema `tirads` **não foi provisionado**:

```
[SCHEMA_NOT_FOUND] The schema `diamond_fabrica_ia_dev.tirads` cannot be found.
```

Em `diamond_fabrica_ia_dev` existem `cancer_rim`, `transplante_pulmao`, `hepatologia` e `flowhub`
(criados em 06/08); `tirads` ficou de fora. Também falta em `_hml` e produção, que hoje só têm
`hepatologia` e `flowhub` — o que afeta igualmente o **transplante de pulmão** na promoção.

A validação desta entrega foi feita no catálogo **anterior** (`diamond_ia_dev`), antes da migração.
