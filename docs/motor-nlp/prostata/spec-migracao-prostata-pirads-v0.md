# SPEC de migração — Próstata (PI-RADS)

> **Estágio:** Research fechado, Plan escrito. **Implement não começou.**
> Levantado em **25/09/2026** direto do legado, da lib e da gold. Dono proposto: **Leandro**.
>
> Card guarda-chuva: `303791` — *Plano de Migração algoritmos final*.

---

## 1. Por que esta linha é a mais barata da fila

**A régua inteira cabe num bloco de config que a lib já executa.** Não há léxico clínico a
traduzir, não há portão de órgão, não há camada semântica: PI-RADS é **uma categoria ordinal
extraída do texto**, e `ordinal_extraction` faz exatamente isso, dirigido por config.

🟢 **E já foi medido.** Em 23/06/2026, em bancada local, o motor extraiu a categoria PI-RADS contra
a referência do lake (`diamond_pirads`, que é um classificador independente):

| | representativa (1.000) | estratificada (259) |
|---|---|---|
| concordância de categoria (1–6) | **98,99%** | **99,46%** |
| MCC da relevância `≥ 4` | **0,968** | **0,973** |

📄 `docs/motor-nlp/rads/notas/Relatorio-homologacao-pirads-tirads-bancada-v0.md`.

⚠️ **Aquilo foi bancada com a `0.1.1`, não homologação.** Serve para dizer que a tradução é viável
e onde ela erra — **não** substitui a paridade contra a saída gravada do legado.

---

## 2. O universo de entrada — medido, não suposto

O legado filtra sobre três colunas concatenadas (`proced_descricao_ajustado`, `dsc_codigo`,
`cod_procedimento`), e o que está **ativo** é pequeno:

```
UPPER(trim(tp_procedimento)) IN ('IMG','IMA')
AND (rm|ressonancia|resso|rnm)
AND (prost|pelve)
AND NOT angi
```

ℹ️ Os blocos de **TC**, **US transretal** e **biópsia** estão **comentados** no notebook, assim
como um filtro pelo texto do laudo (`pi ?rad?s?`). Não reativar sem decisão de negócio: comentário
é decisão anterior de alguém, não sobra de código.

**Medido na gold, 30 dias (25/08–23/09), `tp_procedimento IN ('IMG','IMA')`:**

| recorte | exames | /dia | citam PI-RADS no laudo |
|---|---|---|---|
| **RM próstata** (`prost`) | **896** | 30 | **800 — 89,3%** |
| RM pelve sem próstata | 7.251 | 242 | 267 — 3,7% |
| **total do filtro legado** | **8.147** | 272 | **1.067 — 13,1%** |
| controle: toda a gold IMG/IMA | 682.276 | — | **1.265** |

🟢 **O filtro tem recall de 84,3%** (1.067 de 1.265). O termo que sustenta isso é `pelve`: sozinho
rende 3,7%, mas é por onde entram os laudos de próstata descritos como pelve. **Não remover sem
medir os dois sentidos.**
🔴 **Faltam ~198 exames em 30 dias** que citam PI-RADS e o filtro não pega. É a primeira medição a
refazer com o filtro da plataforma, e é a mesma classe do que custou 18,6% na reumatologia.

⚠️ **Verificação obrigatória antes de qualquer número:** o `gold_filter` da plataforma passa por
`F.expr()` e o literal vai para o **parser SQL**. `\b` em aspas simples vira **BACKSPACE**, não
*word boundary* — precisa de barra dupla no valor Python. Medido na reumatologia: custou 2.550
exames em 30 dias e 2,7 pontos contra o que foi homologado.

---

## 3. A régua do legado, e o que dela se descarta

`model/ntb_ia_pirads_algoritmo.ipynb` **não tem `CONFIG`** — é da geração anterior ao formato que o
`nlp_engine` consome. É regex puro sobre o texto normalizado.

| o que o legado faz | o que vira na plataforma |
|---|---|
| remove acento e caractere especial | tratamento de texto da lib |
| reescreve `bi rads` → `pi_rads` (resíduo de cópia do BI-RADS) | **descartar** — é defeito, não regra |
| normaliza aliases: `pi rads`, `pirads`, `prrads`, `acr pirads`, `pirads_acr`, `acrpirads` | `systems.pi_rads.aliases` + `patterns` |
| **apaga a legenda cortando N caracteres fixos** após um marcador (`.{385}`, `.{700}`) e por `replace` da frase inteira do emissor | 🔴 `aggregation_legend_filter: {'enabled': True}` |
| `categoria = max(lista)` | `aggregation_policy: 'max_category'` |
| `fl_relevante = pirads in ('4','5')` | `promote_categories` com as duas categorias altas |
| sem categoria → `-1` | ausência de categoria; **não** é anomalia |

🔴 **O corte por número fixo de caracteres é o defeito central do legado.** Ele depende da redação
exata de um emissor e de contar 385 caracteres. Qualquer emissor novo, ou a mesma frase com uma
vírgula a mais, e a legenda inteira volta para o `max()` — entregando **PI-RADS 5 de legenda** num
laudo cuja conclusão é 2. Foi o erro dominante que a bancada de junho encontrou, e a lib resolve
isso de forma genérica desde a `0.10.1`.

⚠️ **`get_num_birads`, `generate_birads`, `pirads_old`** — nomes de função e coluna ainda dizem
*birads*. Confirma que a linha nasceu de cópia. **Contar esses defeitos, não herdá-los.**

---

## 4. A config proposta — o bloco que importa

Clonar `ntb_ia_tirads_config.py`, que é a referência viva do `ordinal_extraction`, e trocar o
sistema. **Nada de lib é necessário**: `systems` é inteiramente dirigido por config — o
`config_loader.py` valida `aliases`, `categories`, `patterns` (com um grupo de captura) e
`aggregation_legend_filter`, e não há nenhuma referência a `pirads` no código da lib.

```python
'ordinal_extraction': {
    'enabled': True,
    'aggregation_policy': 'max_category',
    'relevance_mode': 'ordinal_only',      # so a categoria promove; sem lexico
    'systems': {
        'pi_rads': {
            'aliases': ['PI-RADS', 'PIRADS', 'PI RADS', 'ACR PI-RADS'],
            'categories': ['PI1', 'PI2', 'PI3', 'PI4', 'PI5'],
            'patterns': [...],             # derivar do pattern do TI-RADS, trocando o alias
            'normalization': {'roman_to_arabic': True},
            'aggregation_legend_filter': {'enabled': True},
            'promote_categories': ['PI4', 'PI5'],
        }
    },
},
'embeddings': {'use_embeddings': False},
'llm_router': {'enabled': False},
```

**Por que `ordinal_only`:** o legado não tem achado léxico nenhum — a relevância é a categoria e
nada mais. Declarar `findings` seria inventar escopo que o negócio não pediu.

**Por que sem semântica e sem juiz no primeiro corte:** a paridade se mede contra um legado que
também não os tem. Ligá-los muda o perfil e invalida o gabarito nos dois sentidos.
🔴 **Mas `rule_only` é estágio de desenvolvimento, não de entrega** — nenhuma lista vai ao negócio
a partir dele. A escalada entra depois, medindo só o **delta** na mesma janela já homologada.

---

## 5. O que NÃO fazer

- 🔴 **Não reativar** os blocos comentados de TC, US transretal e biópsia no filtro.
- 🔴 **Não reproduzir** o corte de 385/700 caracteres. É o defeito, não a regra.
- 🔴 **Não usar `max()` sobre a categoria em consulta de auditoria** — sobre string é
  lexicográfico. Agrupar.
- 🔴 **Não declarar bloco morto** (`catalog`, `monitoring`, `distribution`): reprova em revisão.
- ⚠️ **Não clonar a segmentação de outra linha.** Declarar `full_doc` e conferir
  `segmentation_coverage` — a hepatologia com `mode: auto` descarta 86% do laudo.

---

## 6. Critérios de aceite

1. **Filtro de entrada medido nos dois sentidos**, com custo por termo, em pelo menos dois dias:
   quantos exames entram, quantos citam PI-RADS, e quantos citando PI-RADS ficam de fora. O `\b`
   conferido por controle manual.
2. **Paridade contra a saída gravada do legado** por `id_exame`, três dias ou mais, com as
   divergências enumeradas e classificadas em *legado errou* / *motor errou* / *fora de escopo*.
3. **A legenda sai pela `aggregation_legend_filter`**, provado: contar `legend_indices` maior que
   zero nos laudos que embutem a escala, e mostrar que nenhum deles promove por ela.
4. **Run em dev ponta a ponta** — runner, `gold_filter`, `column_map`, view e envio. Validação
   local prova a régua e não vê nada disso; na reumatologia seis bloqueios só apareceram rodando.
5. **Schema `prostata` provisionado em dev e prd** pelo time da Fábrica, pedido pelo fluxo próprio
   — **não** na descrição do PR.
6. Config com **cabeçalho e changelog no arquivo**, com o número que sustentou cada versão.

---

## 7. Passos, em ordem

| # | passo | por quê |
|---|---|---|
| 1 | pedir o schema `prostata` em dev e prd | foi o que virou bloqueio na reumatologia na hora de promover |
| 2 | traduzir o `gold_filter` e medi-lo nos dois sentidos | sem isso a paridade mede o que chegou, não o que deveria chegar |
| 3 | escrever a config clonando o TI-RADS, sem semântica e sem juiz | |
| 4 | golden local contra os CSVs de divergência de junho | `bancada/divergencias_pirads_{repr,strat}.csv` já existem |
| 5 | run em dev e paridade por `id_exame` | |
| 6 | PR para `hml` com os 6 arquivos | checklist em `docs/motor-nlp/checklists/checklist-revisao-pr-ds.md` |
| 7 | só depois: ligar semântica e juiz, remedindo o **delta** | |

---

## 8. Dimensionamento

**~30 laudos de RM de próstata por dia**, dos quais ~27 citam PI-RADS. É a **menor** linha da fila,
menor que o transplante de pulmão. O esforço está no filtro e na paridade, não na régua.
