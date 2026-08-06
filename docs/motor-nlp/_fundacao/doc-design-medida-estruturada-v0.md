# Design — medida de fonte ESTRUTURADA na camada quantitativa

**Data:** 2026-08-06 · **Status:** ✅ **IMPLEMENTADO** na `nlp_engine 0.8.0` (branch `feat/medida-valor-texto`)
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
            # A medida vem do PROPRIO texto, por parse deterministico — nao do LLM.
            # Ver secao 4: o valor ja chega em `exm_laudo_texto`.
            'source': {'kind': 'value_text'},
        },
        'threshold': {'op': '<', 'value': 0.4},
        'applies_to_exam_type': ['tsh'],
        'on_met': 'promote',
    },
}
```

`source.kind` abre espaço para outras origens no futuro (`row_field`, se algum dia a plataforma
expuser o componente isolado) sem quebrar o contrato. Ausente = `llm`, comportamento de hoje.

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

- `source: "value_text"` · `llm_called: false`
- `evidence`: o texto original (`"Inferior a 0.01"`). No caminho determinístico o texto **é** a
  evidência — por isso **não** há campo `raw` separado, que seria a mesma string duas vezes
- `censored`: `"left"` / `"right"` quando aplicável. ⚠️ Nesse caso `value` é o **limite**
  observado, não medida exata
- texto sem valor parseável → `met=None` com `source` **distinguível** (`parse_failed`), nunca
  confundível com "medida não encontrada"

⚠️ **Falha tem de ser observável.** É a lição do fallback de embeddings, que degrada em silêncio
(`REFERENCIA-PARAMETROS.md`, débito aberto). O formato tabular (§4.3) é justamente onde o parse
vai falhar — e precisa aparecer, não sumir.

### 3.5 Onde encaixa no motor

`process_quantitative_criteria` já recebe o `treated` — que é exatamente onde o valor está.
**Nenhuma mudança de assinatura é necessária.** O caminho LLM segue idêntico.

Critério com `source: value_text` **não chama o LLM** — o gate de âncora e o custo desaparecem.
O escopo do analito vem de `applies_to_exam_type`, que já existe.

---

## 4. Dependência da plataforma — VERIFICADA, NÃO EXISTE

**Auditado em 2026-08-06 nos dois repositórios. Não é preciso perguntar ao MLOps.**

### 4.1 O que o `column_map` consegue (e não consegue)

`InputMapper.map_input` (`plataform/data/ntb_ia_input.py:53-83`) projeta **7 campos fixos**
(`engine_input_fields`: id_exame, id_paciente, id_unidade, exm_laudo_texto, exm_mod, exm_tipo,
dt_exame) e resolve cada um via `resolve_column`, que apenas **escolhe o primeiro nome de coluna
existente** entre candidatos — depois `F.col(source).cast(StringType())`.

Conclusão: o `column_map` **não** expressa projeção de array, campo aninhado nem expressão. Campo
novo também não passaria: a lista é fixa.

### 4.2 Mas o valor já chega ao motor

Antes do mapeamento, **as duas plataformas achatam o array na mão**, com o mesmo código:

```python
F.array_join(F.transform("proced_lista_exames", lambda x: x["laudo_original"]), "\n")
```

- plataforma nova: `plataform/pipeline_e2e/nlp_ia_02_input.py:396`
- runner legado:  `apps/databricks/nlp_engine/pipeline_e2e/nlp_ia_02_input.py:143`

O resultado vira `proced_laudo_exame_original` → `exm_laudo_texto`. **O valor do exame de sangue
já está no texto que o motor recebe.**

### 4.3 Como o valor chega, medido (TSH, junho/2026, n=34.449)

| forma do `exm_laudo_texto` | fração | exemplo |
|---|---|---|
| número puro | **75,5%** | `1,24` |
| método/material + valor | ~24% | `MATERIAL: SORO ~ MÉTODO: QUIMIOLUMINESCÊNCIA AMPLIADA ~ 1,863` |
| relatório tabular | minoria | `Data de Coleta/Recebimento: 24/06/2026 ... EXAME ...` |

⚠️ Detalhe que torna o parse determinístico: **o texto de método/material não contém dígito**.
Nos dois primeiros formatos o único token numérico é o valor. O terceiro formato tem datas e
horas — ali o parse ingênuo erra, e é justamente o caso que deve cair no fail-safe (ou no caminho
LLM, que continua disponível).

### 4.4 Consequência para o desenho

**Não há dependência de plataforma, e o `source: row_field` fica desnecessário.** A medida é
extraída do próprio `exm_laudo_texto`, de forma determinística, com o critério restrito ao analito
por `applies_to_exam_type` (que já existe e já é usado — `exm_tipo` mapeia `proced_descricao`).

Isso reduz a feature a **uma mudança só na lib**, sem tocar em runner, `column_map` ou
`data_manager`. Nada a pedir ao MLOps.

### 4.5 Precedente no legado

O padrão de explodir o array e filtrar por `nme_exame` já existe — `explode(proced_lista_exames)`
em `birads/data/ntb_ia_entrada.py:216` e `hepatologia/data/ntb_ia_entrada.py:187`; `transform(...)`
em `tirads/data:232` e `ateromatose/data:236`. Mas sempre em **notebook de entrada por
especialidade**, nunca no `data_manager` compartilhado. É o caminho a seguir **se** um dia
precisarmos do componente isolado (ex.: usar a faixa de referência do próprio laboratório, V3.1).

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

**Implementado em 2026-08-06** (`971ac02`): `ValorMedido` (intervalo) + `parse_valor_texto` +
`compare_intervalo` + `Criterion.measure_source` + ramo `_assess_value_text`. 13 testes partindo da
config, com caller que **levanta** se o LLM for chamado. Mutante morto (sem a censura, 4 testes caem).
Contrato completo em `nlp-engine-lib/docs/REFERENCIA-PARAMETROS.md` §9 e §10.3.

| # | Pendência | Com quem |
|---|---|---|
| 1 | Validar os limiares (TSH < 0,4 · T4L > 1,8 ng/dL · TRAb > 1,5) | Lucas — **não bloqueia**: seguir com os atuais e revisar depois (decisão de 2026-08-06) |
| 2 | ~~Confirmar se `column_map` projeta elemento de array~~ | ✅ **RESOLVIDO** — ver §4: não há dependência |
| 3 | Base ouro `verdade_v3` | **surge depois**: implementar a V3, rodar um lote e construir o gabarito a partir dele (decisão de 2026-08-06) |

⚠️ Segue valendo que **validar régua nova contra gabarito antigo não significa nada** (nota de
método da spec). Por isso a base ouro V3 nasce do lote processado pela V3, não antes dele.

**Nada bloqueia a implementação.** A feature é agnóstica e os números vivem na config.

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
