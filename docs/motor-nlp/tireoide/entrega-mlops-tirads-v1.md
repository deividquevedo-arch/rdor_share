# Entrega — TI-RADS na plataforma NLP

**Data:** 2026-08-04 · **Para:** MLOps · **Status:** validado, pronto para o fluxo diário

> Comportamentos da plataforma que valem para qualquer especialidade estão em
> `docs/motor-nlp/_fundacao/notas-plataforma-nlp-mlops.md`. Aqui só o que é do TI-RADS.

---

## 1. O que está sendo entregue

| item | valor |
|---|---|
| especialidade | `tirads` |
| config | `plataform/config/speciality/ntb_ia_tirads_config.py` |
| branch | `feature/validacao-plataforma` (`fabrica-ia-nlp-platform`) |
| `config_version` | `0.1.0-tirads-rads-v22.11-v2` |
| `nlp_engine` | `0.7.1` |
| perfil | TI-RADS (`rads_extraction`) + achados clínicos + embeddings (hybrid) + juiz LLM + quantitativo |

---

## 2. Validação contra a base ouro

Execução em `dev`, janela **2026-06-25 a 2026-06-27**, cruzada por `id_exame` com
`docs/motor-nlp/tireoide/dados/base-ouro-tirads-v2-2026-07-24.csv` (586 ids resolvidos, 127 positivos).

| | plataforma nova | referência homologada |
|---|---|---|
| laudos processados | 2.711 | — |
| coorte presente | **440 de 586 (75,1%)** | 586 |
| positivos cobertos | **121 de 127** | 127 |
| TP · FP · FN · TN | 121 · 3 · **0** · 316 | 127 · 4 · 0 · 455 |
| precisão | **0,9758** | 0,9695 |
| recall | **1,0000** | 1,0000 |
| MCC | **0,9832** | 0,9803 |

**Zero falso-negativo**, precisão e MCC acima da referência.

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

## 4. Pontos específicos do TI-RADS

### 4.1 `column_map` foi reescrito por inteiro

A versão homologada lia da **canônica** (`an`, `Laudo`, `dataexame`, `modalidade`, `tipoexame`) — nenhuma dessas colunas existe na Gold. Foi o único dos configs portados que exigiu reescrita completa desse bloco.

### 4.2 `exm_mod` não tem equivalente na Gold

A canônica tinha `modalidade` (US/CT/RM), derivada no pipeline dela. A Gold não tem, então usamos `cod_procedimento`/`tp_codigo_procedimento` e a calibração cai no neutro — ela só ajusta score em US/RM.

Efeito esperado: divergência em casos de borda. Medido no piloto V3 do tireoide: **1 caso em 675**, com confiança 0,654.

### 4.3 `embedding_enable` precisa ficar `true`

Diferente do pulmão. A config do TI-RADS tem `use_embeddings: True` e `decision_mode: hybrid`; com o widget em `false` a decisão híbrida não acontece.

---

## 5. Pendência clínica, não técnica

6 positivos de 127 seguem fora: **"região cervical/supraclavicular"** e **"PAAF/biópsia de linfonodo"**. Não constam da lista de exames da spec.

Incluí-los custaria +3.100 exames (2,7× o lote) para ganhar 6. Está com o especialista.

---

## 6. Widgets do run de validação

Só o que difere do padrão descrito na nota geral:

| widget | valor |
|---|---|
| `specialty` | `tirads` |
| `start_date` / `end_date` | `2026-06-25` / `2026-06-27` |
| `embedding_enable` | **`true`** |

**Conferência rápida do lote:** entrada com **2.711 linhas** e **572 relevantes**.
