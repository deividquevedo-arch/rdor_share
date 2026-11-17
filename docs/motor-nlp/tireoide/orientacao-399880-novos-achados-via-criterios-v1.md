# Tireoide — novos achados relevantes só com config (card `399880`)

> Card `399880` — *[Tireoide] Ampliar critérios de identificação de laudos relevantes com novos achados*
> Base: config de produção `0.8.0-tirads` (idêntica em `main` e `hml`) · lib `nlp_engine 0.12.3`
> Arquivo: `plataform/config/speciality/ntb_ia_tirads_config.py` · branch a partir da `hml`

## 1. O pedido

Manter a regra atual e passar a enviar:

- nódulo ou cisto **≥ 1 cm sem categoria TI-RADS** no laudo;
- **linfonodo suspeito**;
- **massa / tumor**;
- **bócio nodular**.

TI-RADS 1, 2 e 3 **não entram**, mesmo com nódulo grande.

## 2. A solução: sem implementação na lib

**Não trocar para `normal_plus_ordinal`.** Nesse modo o nódulo ≥ 1 cm vira relevante pela régua léxica,
e TR1–TR3 não têm como rebaixá-lo: a lib só promove (0→1).

**Manter `ordinal_only`** (a regra atual fica intacta) e incluir os achados novos como **critérios
com `on_met: 'promote'`**. Conferido no código da `0.12.3` (`quantitative.py`):

| comportamento | onde |
|---|---|
| `promote` roda antes do `gate_relevance` | ordenação `_order` em `apply_quantitative_criteria` |
| laudo promovido por critério não é rebaixado pelo gate coordenado | `_coordinated_gate_demotes` (`promoted`) |
| critério `promote` sem âncora no laudo não roda e não afeta o gate | `_registrar_ancora_ausente` (`skipped_no_anchor`) |
| `label` do critério atendido aparece na coluna `findings` | `findings_columns` (`decision_pipeline.py`) |

## 3. Comportamento esperado

| laudo | resultado |
|---|---|
| nódulo ou cisto ≥ 1 cm, **sem** categoria TI-RADS no laudo | **envia** |
| TR1–3 com nódulo ou cisto ≥ 1 cm | **não envia** |
| TR1–3 + bócio nodular ou linfonodo suspeito | **envia** (pelo bócio / linfonodo) |
| TR4 ≥ 1 cm, TR5 | como hoje |
| bócio difuso, linfonodo reacional, nódulo sem medida | não envia |

## 4. Alterações na config

### 4.1 `ordinal_extraction.relevance_mode` — fica `'ordinal_only'`

Só o comentário muda:

```python
# 0.10.0: SEGUE ordinal_only. Os achados do card 399880 entram como CRITERIOS `promote`;
# em normal_plus_ordinal o nodulo >= 1 cm viraria relevante e TR1-TR3 nao teriam como rebaixa-lo.
'relevance_mode': 'ordinal_only',
```

### 4.2 `linfonodo_suspeito` — de gate para promote

```python
'label': 'Linfonodo suspeito',
'on_met': 'promote',   # era 'gate_relevance' (0.10.0, card 399880)
```

O efeito sobre o TR4 não muda (`met=True` mantinha; agora promove). Muda o custo: como gate só chamava o
LLM com `fl=1`; como `promote` chama em todo laudo com o achado `linfonodo`.

### 4.3 Nódulo e cisto ≥ 1 cm sem categoria — cópias dos critérios existentes

Cópias de `nodulo_maior_1cm` e `cisto_maior_1cm`, **com os mesmos `description`, `measure`, `threshold`,
`require_measure` e `llm`**: a comparação ≥ 1 cm continua numérica. Mudam três coisas:

- `on_met: 'promote'`;
- **sem** `gates_ordinal_promotion` (fica só nos originais, condicionando o TR4);
- âncora **só por texto**, que casa apenas em laudo **sem nenhuma categoria TI-RADS**.

```python
'nodulo_sem_categoria': {
    'label': 'Nódulo ≥ 1 cm sem TI-RADS',
    # Mesma medida do `nodulo_maior_1cm`. O que este criterio acrescenta e SO a condicao
    # "sem categoria TI-RADS no laudo", expressa na ancora: casa 'nodul' apenas quando o laudo
    # inteiro nao cita TI-RADS/TR1-6. Laudo com categoria fica com a regua ordinal.
    'anchor': {'text': r'^(?![\s\S]*(?:ti-?\s?r{1,2}ads|\btr\s?[1-6]\b))[\s\S]*n[oó]dul'},
    # description / measure / threshold / require_measure / llm: iguais ao nodulo_maior_1cm
    'on_met': 'promote'},
'cisto_sem_categoria': {
    'label': 'Cisto ≥ 1 cm sem TI-RADS',
    'anchor': {'text': r'^(?![\s\S]*(?:ti-?\s?r{1,2}ads|\btr\s?[1-6]\b))[\s\S]*cist[oó]'},
    # description / measure / threshold / require_measure / llm: iguais ao cisto_maior_1cm
    'on_met': 'promote'},
```

### 4.4 Massa / tumor — dois critérios (a âncora aceita um achado só)

```python
'massa_tireoidiana': {'kind': 'qualitative', 'label': 'Massa/tumor',
    'anchor': {'finding': 'massa'},
    'question': 'Ha, no laudo, massa, tumor, neoplasia ou lesao expansiva REAL (afirmada, nao '
                'negada, fora da indicacao/hipotese) na tireoide ou em cadeias cervicais? '
                'Responda false se for apenas hipotese a esclarecer ou pedido de pesquisa.',
    'on_met': 'promote',
    'llm': {...}},   # mesmo bloco llm dos criterios existentes
'tumor_tireoidiano': {...},   # igual ao de cima, com 'anchor': {'finding': 'tumor'}
```

### 4.5 Bócio nodular

O achado léxico `bocio` já exclui o difuso (`exclude`), salvo quando nodular ou mergulhante (`unless`).

```python
'bocio_nodular': {'kind': 'qualitative', 'label': 'Bócio nodular',
    'anchor': {'finding': 'bocio'},
    'question': 'Ha, no laudo, bocio NODULAR, MULTINODULAR ou MERGULHANTE afirmado? Responda '
                'false para bocio difuso/homogeneo, aumento difuso da glandula ou tireoidopatia '
                'difusa sem nodulos.',
    'on_met': 'promote',
    'llm': {...}},   # mesmo bloco llm
```

### 4.6 Versão e cabeçalho

- `config_version: '0.10.0-tirads'` — o `0.9.0-tirads` está reservado na branch `tirads/feature/waive-paaf`.
- Changelog no `%md`: `0.10.0` — achados do card `399880` (nódulo/cisto ≥ 1 cm sem TI-RADS, linfonodo
  suspeito, massa/tumor, bócio nodular) via critérios `promote`; TR1–3 seguem fora.
- Matriz de escopo: linha nova **"V2 ampliada"** (`ordinal_only` + critérios `promote`), marcada como ativa.

### 4.7 O que não muda

Juiz (`runtime.llm_router.enabled: False`), `gold_filter`, `findings_policy` e critérios de sangue.
Os blocos `catalog` / `monitoring` / `distribution` ficam para PR separado — limpeza não entra junto
com mudança de régua.

## 5. Validação, nesta ordem

1. **Rodada de medição: os 7 critérios novos ou alterados em `on_met: 'annotate_only'`.** Avaliam e
   gravam no blob, sem promover — a entrega fica igual à `0.8.0`. Dev, janela já processada,
   `reprocess_enable`.
   - `met=True` por critério, projetado para o mês, contra os ~695/mês estimados no card;
   - amostra lida de cada critério.
2. **Controle das âncoras de nódulo e cisto** (regex de âncora inválida é ignorada em silêncio):
   - nenhum laudo com TI-RADS no texto pode aparecer com `nodulo_sem_categoria` / `cisto_sem_categoria` avaliados;
   - laudos sem TI-RADS e com nódulo/cisto têm de aparecer avaliados;
   - contar os laudos com nódulo e **só a legenda do ACR** (sem categoria atribuída): ficam bloqueados pela âncora.
3. **Rodada de entrega: tudo em `promote`.** Contra a `0.8.0`, por `id_exame`: só **promovidos**,
   **zero rebaixados** — rebaixado indica efeito no gate. Conferir a coluna de achado no Excel de dev.
4. **Homologação:** levar ao negócio só os casos novos (os que a `0.8.0` não entregava).
5. **PR para a `hml`.**

## 6. Ponto de atenção em produção

Com o endpoint de LLM fora do ar, nenhum achado novo promove e nada dá erro (`met=None`). Acompanhar
`llm_error` nos critérios novos depois da subida.
