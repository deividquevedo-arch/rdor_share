# Tireoide — novos achados relevantes só com config (card `399880`) — v2

> Card `399880` — *[Tireoide] Ampliar critérios de identificação de laudos relevantes com novos achados*
> Base: config de produção `0.8.0-tirads` (idêntica em `main` e `hml`) · lib `nlp_engine 0.12.3`
> Arquivo: `plataform/config/speciality/ntb_ia_tirads_config.py` · branch a partir da `hml`
> **v2 (17/11):** pares `promote` + `gate_relevance` para linfonodo, bócio, massa e tumor (o `promote` sozinho não
> segura o TR4 já relevante); âncora de "sem categoria" cobrindo todas as grafias de TI-RADS; massa/tumor só na tireoide.
> Anterior: [[orientacaoaaaaaaaaa-399880-novos-achados-via-criterios-v1|v1]].

## 1. O pedido

Manter a regra atual e passar a enviar:

- nódulo ou cisto **≥ 1 cm sem categoria TI-RADS** no laudo;
- **linfonodo suspeito**;
- **massa / tumor** na tireoide;
- **bócio nodular**.

TI-RADS 1, 2 e 3 **não entram** pelo nódulo/cisto, mesmo grande.

## 2. A solução: sem implementação na lib

**Manter `relevance_mode: 'ordinal_only'`** (a regra atual fica inPedro1707
tacta) e incluir os achados novos como **critérios
quantitativos**. Em `normal_plus_ordinal` o nódulo ≥ 1 cm viraria relevante pela régua léxica e TR1–TR3 não teriam como
rebaixá-lo — a lib só promove (0 → 1).

Comportamento da `0.12.3` que o desenho usa (`quantitative.py`):

| comportamento | onde |
|---|---|
| critérios `promote` rodam antes dos `gate_relevance` | ordenação `_order` em `apply_quantitative_criteria` |
| `promote` só marca a promoção quando `fl` vai de **0 para 1** | `apply_on_met` (`if on_met == "promote" and met is True and current_fl == 0`) |
| o gate coordenado rebaixa só quando **todos** os gates dão `False`; `True` ou `None` mantêm | `_coordinated_gate_demotes` |
| em `ordinal_only` nenhum achado léxico conta como motivo de manter (`finding_keys` vazio) | idem |
| critério `gate_relevance` não roda com `fl = 0` (`skipped_not_relevant`) | laço de `apply_quantitative_criteria` |
| critério sem âncora no laudo não roda (`skipped_no_anchor`); sem `require_measure`, não entra no gate | `_registrar_ancora_ausente` |
| `label` de critério `promote` atendido aparece na coluna `findings` | `findings_columns` (`decision_pipeline.py`) |

**Por que pares.** Num laudo TR4 o `fl` já é 1. Um critério `promote` atendido ali **não** marca a promoção, e o gate
do `nodulo_maior_1cm` (nódulo < 1 cm) rebaixa o laudo mesmo com bócio nodular, massa ou linfonodo suspeito. Por isso
cada um desses achados tem **duas versões com a mesma pergunta**:

| versão | cobre |
|---|---|
| `…` com `on_met: 'promote'` | laudo com `fl = 0` — TR1–TR3 ou sem categoria: o achado **promove** |
| `…` com `on_met: 'gate_relevance'` | laudo TR4 com nódulo < 1 cm: `met = True` **impede o rebaixamento** |

Nódulo e cisto sem categoria **não precisam de par**: a âncora deles só casa em laudo sem nenhuma menção a TI-RADS,
então nunca rodam num TR4.

## 3. Comportamento esperado

| laudo | resultado |
|---|---|
| nódulo ou cisto ≥ 1 cm, **sem** categoria TI-RADS no laudo | **envia** |
| TR1–3 com nódulo ou cisto ≥ 1 cm | **não envia** |
| TR1–3 + bócio nodular, massa/tumor ou linfonodo suspeito | **envia** (pelo achado) |
| **TR4 + nódulo < 1 cm + bócio nodular ou massa/tumor** | **envia** (o gate do achado segura o TR4) |
| **TR4 + nódulo < 1 cm + linfonodo suspeito** | **envia** (como hoje) |
| TR4 + nódulo < 1 cm, sem outro achado | não envia (como hoje) |
| TR4 ≥ 1 cm, TR5 | envia (como hoje) |
| bócio difuso, linfonodo reacional, nódulo sem medida, massa só na indicação/hipótese | não envia |

## 4. Alterações na config

O bloco `llm` de todos os critérios novos é **o mesmo dos critérios existentes** (`databricks-claude-haiku-4-5`,
fallback `databricks-claude-sonnet-4-5`, `api_key_env: DATABRICKS_TOKEN`, `temperature: 0`, `max_tokens: 256`).
**A pergunta é idêntica nas duas versões do par** — com `temperature: 0`, as duas respostas tendem a coincidir.

### 4.1 `ordinal_extraction.relevance_mode` — fica `'ordinal_only'`

```python
# 0.10.0: SEGUE ordinal_only. Os achados do card 399880 entram como CRITERIOS (pares promote + gate);
# em normal_plus_ordinal o nodulo >= 1 cm viraria relevante e TR1-TR3 nao teriam como rebaixa-lo.
'relevance_mode': 'ordinal_only',
```

### 4.2 Linfonodo suspeito — o original continua gate, entra a cópia promote

`linfonodo_suspeito` **não muda** (`on_met: 'gate_relevance'`): é o que já segura o TR4 com linfonodo suspeito. Entra:

```python
'linfonodo_suspeito_promove': {
    # mesma 'kind', 'anchor', 'question' e 'llm' do linfonodo_suspeito
    'label': 'Linfonodo suspeito',
    'on_met': 'promote'},
```

### 4.3 Nódulo e cisto ≥ 1 cm sem categoria — cópias dos critérios de medida

Cópias de `nodulo_maior_1cm` e `cisto_maior_1cm` com os mesmos `description`, `measure`, `threshold`,
`require_measure` e `llm` (a comparação ≥ 1 cm continua numérica). Mudam: `on_met: 'promote'`, **sem**
`gates_ordinal_promotion`, e âncora **só por texto** que casa apenas em laudo sem nenhuma menção a TI-RADS. A âncora
cobre as mesmas grafias do padrão ordinal da config (`TI-RADS`, `TIRADS`, `TI RADS`, `TI_RADS`, `TRADS`, `T-RADS`,
`TR3`, `TR 3`):

```python
'nodulo_sem_categoria': {
    'label': 'Nódulo ≥ 1 cm sem TI-RADS',
    # Mesma medida do `nodulo_maior_1cm`. O que este criterio acrescenta e SO a condicao
    # "sem categoria TI-RADS no laudo", expressa na ancora: casa 'nodul' apenas quando o laudo
    # inteiro nao cita TI-RADS/TR1-6. Laudo com categoria fica com a regua ordinal.
    'anchor': {'text': r'^(?![\s\S]*(?:t[i]?[-_\s]?r{1,2}ads|\btr\s?[1-6]\b))[\s\S]*n[oó]dul'},
    # description / measure / threshold / require_measure / llm: iguais ao nodulo_maior_1cm
    'on_met': 'promote'},
'cisto_sem_categoria': {
    'label': 'Cisto ≥ 1 cm sem TI-RADS',
    'anchor': {'text': r'^(?![\s\S]*(?:t[i]?[-_\s]?r{1,2}ads|\btr\s?[1-6]\b))[\s\S]*cist[oó]'},
    # description / measure / threshold / require_measure / llm: iguais ao cisto_maior_1cm
    'on_met': 'promote'},
```

### 4.4 Massa / tumor — pares, um por âncora (a âncora aceita um achado só)

```python
PERGUNTA_MASSA = ('Ha, no laudo, massa, tumor, neoplasia ou lesao expansiva REAL (afirmada, nao negada, '
                  'fora da indicacao/hipotese) NA TIREOIDE? Responda false se for apenas hipotese a '
                  'esclarecer, pedido de pesquisa, ou achado fora da tireoide.')

'massa_tireoidiana_promove': {'kind': 'qualitative', 'label': 'Massa/tumor', 'anchor': {'finding': 'massa'},
                              'question': PERGUNTA_MASSA, 'on_met': 'promote', 'llm': {...}},
'massa_tireoidiana_gate':    {'kind': 'qualitative', 'anchor': {'finding': 'massa'},
                              'question': PERGUNTA_MASSA, 'on_met': 'gate_relevance', 'llm': {...}},
'tumor_tireoidiano_promove': {... igual, 'anchor': {'finding': 'tumor'}, 'on_met': 'promote'},
'tumor_tireoidiano_gate':    {... igual, 'anchor': {'finding': 'tumor'}, 'on_met': 'gate_relevance'},
```

A constante é só para a leitura deste guia; na config, o texto vai literal nos dois critérios (a SPEC 27 pede
dicionário literal).

### 4.5 Bócio nodular — par

O achado léxico `bocio` já exclui o difuso (`exclude`), salvo quando nodular ou mergulhante (`unless`).

```python
PERGUNTA_BOCIO = ('Ha, no laudo, bocio NODULAR, MULTINODULAR ou MERGULHANTE afirmado? Responda false para '
                  'bocio difuso/homogeneo, aumento difuso da glandula ou tireoidopatia difusa sem nodulos.')

'bocio_nodular_promove': {'kind': 'qualitative', 'label': 'Bócio nodular', 'anchor': {'finding': 'bocio'},
                          'question': PERGUNTA_BOCIO, 'on_met': 'promote', 'llm': {...}},
'bocio_nodular_gate':    {'kind': 'qualitative', 'anchor': {'finding': 'bocio'},
                          'question': PERGUNTA_BOCIO, 'on_met': 'gate_relevance', 'llm': {...}},
```

### 4.6 Versão e cabeçalho

- `config_version: '0.10.0-tirads'` — o `0.9.0-tirads` está reservado na branch `tirads/feature/waive-paaf`.
- Changelog no `%md`: `0.10.0` — achados do card `399880` (nódulo/cisto ≥ 1 cm sem TI-RADS, linfonodo suspeito,
  massa/tumor na tireoide, bócio nodular) via critérios; pares `promote` + `gate_relevance` porque o `promote` não
  segura o TR4 já relevante; TR1–3 seguem fora pelo nódulo/cisto.
- Matriz de escopo: linha nova **"V2 ampliada"** (`ordinal_only` + critérios), marcada como ativa.

### 4.7 O que não muda

Juiz (`runtime.llm_router.enabled: False`), `gold_filter`, `findings_policy`, critérios de sangue, `nodulo_maior_1cm`,
`cisto_maior_1cm` e `linfonodo_suspeito`. Os blocos `catalog` / `monitoring` / `distribution` ficam para PR separado.

## 5. Validação, nesta ordem

1. **Rodada de medição: os 9 critérios novos em `on_met: 'annotate_only'`.** Avaliam e gravam no blob, sem promover
   nem segurar — a entrega fica igual à `0.8.0`. Dev, janela já processada, `reprocess_enable`.
   - `met=True` por critério, projetado para o mês, contra os ~695/mês estimados no card;
   - amostra lida de cada critério;
   - **par coerente:** as duas versões de cada achado têm de dar o mesmo `met` no mesmo laudo.
2. **Controle das âncoras de nódulo e cisto** (regex de âncora inválida é ignorada em silêncio):
   - nenhum laudo com TI-RADS no texto pode aparecer com `nodulo_sem_categoria` / `cisto_sem_categoria` avaliados;
   - laudos sem TI-RADS e com nódulo/cisto têm de aparecer avaliados;
   - contar os laudos com nódulo e **só a legenda do ACR** (sem categoria atribuída): ficam bloqueados pela âncora.
3. **Rodada de entrega: valores finais de `on_met`.** Contra a `0.8.0`, por `id_exame`:
   - só **promovidos**, **zero rebaixados** — rebaixado indica efeito indevido no gate;
   - **controles obrigatórios**, com laudo real ou sintético: TR4 + nódulo < 1 cm + bócio nodular → `1`;
     TR4 + nódulo < 1 cm + linfonodo suspeito → `1`; TR4 + nódulo < 1 cm sem outro achado → `0`;
     TR3 + nódulo 1,5 cm → `0`; TR3 + nódulo 1,5 cm + bócio nodular → `1`;
   - conferir a coluna de achado no Excel de dev.
4. **Homologação:** levar ao negócio só os casos novos (os que a `0.8.0` não entregava).
5. **PR para a `hml`.**

## 6. Pontos de atenção em produção

- Com o endpoint de LLM fora do ar, nenhum achado novo promove e nada dá erro (`met=None`); nos gates, `None` mantém o
  TR4 (comportamento atual). Acompanhar `llm_error` nos critérios novos depois da subida.
- Custo: nos laudos promovidos por bócio, massa, tumor ou linfonodo, a versão gate roda depois e chama o LLM de novo
  (duas chamadas, só nos laudos com a âncora). A rodada `annotate_only` mede o volume.
