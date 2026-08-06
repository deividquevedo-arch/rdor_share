# Design — medida de fonte ESTRUTURADA na camada quantitativa

**Data:** 2026-08-06 · **Status:** desenho aprovado, não implementado · **Lib:** `nlp_engine` (proposta ≥ 0.8.0)
**Motivador:** exames de sangue do Tireoide V3 · **Escopo:** feature global, agnóstica a especialidade

> Documento de apoio para não perder contexto entre tarefas paralelas. Registra **o achado que
> motivou**, **a decisão**, **o desenho** e **o que falta**.

---

## 1. Por que existe

A camada `quantitative_criteria` extrai medida com **LLM row-level** a partir do TEXTO do laudo.
Foi a decisão certa para o transplante de pulmão, onde o valor vem embutido em prosa
("distúrbio restritivo, CVF 42% previsto") e nenhum regex daria conta.

Os exames de sangue são o caso **oposto**: o valor já vem estruturado.

### O que o lake realmente tem

`gold_corporativo_ia.corporativo.tb_gold_mov_exame` → `proced_lista_exames` é um **array de
componentes rotulados**, não um texto:

```
nme_exame = "Resultado"  ->  laudo_transformado = "4.25"
nme_exame = "METODO"     ->  laudo_transformado = "Método..: ELETROQUIMIOLUMINESCÊNCIA"
```

⚠️ Ler só `element_at(..., 1)` engana: o primeiro elemento costuma ser o método ou o material,
**não o valor**. Foi o que levou a spec de negócio a registrar "o resultado é um número nu, sem
faixa de referência" — leitura parcial. O struct tem inclusive
`limite_inferior_faixa_referencia` / `limite_superior_faixa_referencia`.

### Medido em junho/2026 (filtrando componentes de método/material)

| analito | exames/mês | valor numérico direto | com faixa de referência |
|---|---|---|---|
| TSH | 34.449 | **34.227 — 99,4%** | 13.106 (38%) |
| T4 livre | 32.548 | **32.425 — 99,6%** | 12.898 (40%) |

Medianas (1,81 e 1,22) batem com as da spec — mesmo dado, lido melhor.

### A conta que decide

Usar o LLM aqui significaria **~67 mil chamadas/mês** só de TSH e T4 livre, para ler números sem
ambiguidade nenhuma. O pulmão inteiro faz 1.525/mês. É **44×** o volume, e herda o
não-determinismo já medido (±2 relevantes entre execuções idênticas com `temperature=0`).

Quando decidimos "manter um único mecanismo (LLM), sem duplicar", o gatilho de revisão ficou
registrado: *"revisitar só se escala, custo ou reprodutibilidade virar problema real"*. Os três
viraram simultaneamente. **Esta é a revisão prevista, não uma reversão da decisão.**

---

## 2. Requisito que quase passou despercebido: valores CENSURADOS

O resultado nem sempre é um número. Distribuição real do TSH em junho:

| forma | n | exemplos |
|---|---|---|
| número puro | 34.227 | `0.42` · `98,60` |
| **censurado à esquerda** | **133** | `<0,01` · `Inferior a 0.01` |
| censurado à direita | 30 | `Superior a 100.00` |
| outro texto | 59 | laudo narrativo com o valor embutido |

**Os 133 censurados à esquerda são o "TSH indetectável"** — o sinal mais forte de hipertireoidismo
manifesto, segundo a própria spec clínica.

Dimensionando contra a população-alvo (TSH < 0,4):

| | n |
|---|---|
| TSH < 0,4 numérico | 965 |
| TSH censurado à esquerda (todos `< 0,01`, logo todos < 0,4) | **133** |
| **total relevante** | **1.098** |

Um parser ingênuo com `try_cast` descartaria os 133 em silêncio: **12,1% de perda de recall,
concentrada nos casos mais graves.** O requisito de censura não é refinamento — é condição para
o critério funcionar.

---

## 3. Desenho proposto

### 3.1 Princípio

O critério declara **de onde vem a medida**. Default inalterado (`llm`), então nenhuma config
existente muda. Opt-in, config-in, agnóstico — serve qualquer especialidade com exame laboratorial,
e sangue não será o último.

### 3.2 Config

```python
'quantitative_criteria': {
    'tsh_suprimido': {
        'label': 'Hipertireoidismo',
        'description': 'TSH suprimido',
        'measure': {
            'name': 'tsh',
            'unit': 'mUI/L',
            'source': {'kind': 'row_field', 'field': 'exm_valor_resultado'},
        },
        'threshold': {'op': '<', 'value': 0.4},
        'applies_to_exam_type': ['tsh'],
        'on_met': 'promote',
    },
}
```

`source.kind` abre espaço para outras origens no futuro sem quebrar o contrato. Ausente = `llm`.

**Um critério por analito.** Uma linha = um exame, então TSH e T4 livre nunca coexistem na mesma
linha — não cabe `any_of` entre analitos. Isso é compatível com a redefinição de negócio de
2026-08-03: *"um ou outro já serve"*, cada analito promove sozinho.

### 3.3 Semântica de censura — intervalo, não número

O valor lido vira um **intervalo**, e a condição é avaliada contra ele:

| forma no dado | intervalo | `< 0,4` | `> 1,8` |
|---|---|---|---|
| `0.42` | [0,42 , 0,42] | não | não |
| `Inferior a 0.01` / `<0,01` | (−∞ , 0,01) | **sim** | não |
| `Inferior a 0.5` | (−∞ , 0,5) | **indeterminado** | não |
| `Superior a 100.00` | (100 , +∞) | não | **sim** |

Regra: **satisfeito** só quando o intervalo INTEIRO satisfaz; **não satisfeito** quando o
intervalo inteiro viola; **indeterminado** (`met=None`, fail-safe) quando o intervalo cruza o
limiar. Determinístico e auditável.

### 3.4 Audit

O bloco por critério passa a distinguir a origem:

- `source: "row_field"` · `llm_called: false`
- `raw`: a string original (`"Inferior a 0.01"`) — auditoria clínica precisa ver o que estava lá
- `censored`: `"left"` / `"right"` quando aplicável
- valor ausente ou não parseável → `met=None` com `source` **distinguível**
  (`field_missing` / `parse_failed`)

⚠️ **Falha tem de ser observável.** É a lição do fallback de embeddings, que degrada em silêncio
(`REFERENCIA-PARAMETROS.md`, débito aberto). Aqui, campo ausente **não pode** parecer
"medida não encontrada".

### 3.5 Onde encaixa no motor

`process_quantitative_criteria` hoje recebe só o `treated`. Para ler um campo, precisa receber
também a linha (`st.row`). É a única mudança de assinatura; o caminho LLM segue idêntico.

Critério com `source` **não chama o LLM** — o gate de âncora e o custo desaparecem.

---

## 4. Dependência do lado da plataforma

O motor lê um **campo da linha**. Alguém precisa levar o valor do array para lá — é o
`column_map`, na camada de dados.

O componente correto é o que sobra ao filtrar `nme_exame` em (`METODO`, `MÉTODO`, `MATERIAL`).
**Confirmar com o MLOps** se o `column_map` da plataforma nova consegue projetar elemento de array,
ou se precisa de um passo de achatamento antes.

Sem isso a feature não roda — é a dependência crítica.

---

## 5. O que fica de fora deste desenho

- **V3.2** (sangue positivo → buscar USG com doppler): é lógica ENTRE exames/linhas, que o motor
  row-level não faz. Precisa de desenho próprio.
- **Faixa de referência por laboratório**: 38–40% das linhas trazem os limites estruturados. Usar
  o limiar do próprio laboratório em vez do fixo é possível e mais correto clinicamente, mas
  aumenta o escopo. Deixado para uma V3.1.
- **Os 59 "outro texto"** do TSH (0,2%): laudo narrativo com o valor embutido. Caem no fail-safe.
  Se virarem problema, são exatamente o caso de uso do caminho LLM — que continua disponível.

---

## 6. Pendências antes de implementar

| # | Pendência | Com quem |
|---|---|---|
| 1 | Validar os limiares (TSH < 0,4 · T4L > 1,8 ng/dL · TRAb > 1,5) | Lucas (especialista) |
| 2 | Confirmar se `column_map` projeta elemento de array | MLOps |
| 3 | Re-rotular a base ouro para `verdade_v3` | time + revisão médica |

⚠️ **A pendência 3 não é burocracia.** Validar régua nova contra gabarito antigo não significa
nada — está registrado na nota de método da spec.

**O item 1 não bloqueia a implementação da lib**: a feature é agnóstica, os números vivem na
config. Dá para construir e testar com os limiares atuais e ajustar depois.

---

## 7. Anti-TPO — correção à spec de negócio

A spec classificou o Anti-TPO como **qualitativo** ("reagente / não reagente"). O dado diz outra
coisa:

```
"Inferior a 0,2"   -> 2.174
"Inferior a 5.61"  ->    70
"0,23" / "0.50"    -> valores diretos
```

É **numérico com censura à esquerda**, o mesmo padrão do TSH. Tratar como qualitativo perderia os
valores altos, que são justamente os relevantes. O desenho de censura acima já o cobre —
não precisa de mecanismo separado.

---

## 8. Referências

- Spec de negócio: `docs/motor-nlp/tireoide/spec-negocio-tireoide-discovery-v1.md`
- Camada quantitativa: `docs/motor-nlp/tireoide/doc-spec-camada-criterios-quantitativos-v0.md`
- Débito do fallback silencioso: `nlp-engine-lib/docs/REFERENCIA-PARAMETROS.md`
- Decisão "menos é mais" (não criar regex-extractor) e seu gatilho de revisão: memória
  `pulmao-linha-cuidado-story-v1`
