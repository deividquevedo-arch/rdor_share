# Briefing — migração da reumatologia para o `nlp_engine`

> Levantamento do algoritmo legado, com volumetria e baseline medidos em 2026-09-04.
>
> **Fonte:** repositorio **`IAAzureDatabricksReumatologia`**, branch **`hml`**, commit
> `b200286` (2026-07-23). Dados em `hive_metastore.ia`, workspace antigo.
>
> ⚠️ **Cada linha legada tem repositorio proprio** (`IAAzureDatabricksColon`,
> `IAAzureDatabricksRim`, ...). A copia em `fabrica-ia-plataforma/apps/databricks/reumatologia/`
> **nao e a fonte**: parou em 2026-05-15.
>
> ⚠️ **Producao roda a `hml`, nao a `main`.** Verificado no job `ia-reumatologia`
> (`836374875734315`), que aponta para `/Repos/AzureDatabricksMLOps/IAAzureDatabricksReumatologia`
> com a `hml` conectada. A `main` parou em `f770f81`, 2024-09-11, com estrutura anterior
> (`ntb_ia_algoritmo.py`) que producao **nao** executa.
>
> ℹ️ O **algoritmo** e byte-identico entre a `hml` e a copia de maio — a analisa da regua abaixo
> vale para os dois. Difere so o notebook de **entrada**, em 85 linhas.

---

## 1. O que é a linha hoje

**Linha viva em produção**, no runner legado, fora do Unity Catalog. Último laudo processado em
**2026-09-02**.

| dimensão | valor |
|---|---|
| entrada acumulada | **914.286 laudos** · janela 2025-12-29 → 2026-09-02 |
| volumetria diária | **3.200 a 4.500 laudos/dia** |
| saída | 792.660 linhas · **2.971 relevantes** · taxa **0,37%** |
| juiz LLM | **não existe** |
| embeddings | expansão semântica local, `min_sim 0,65`, `top_k 24` |

⚠️ **É a maior linha do parque por uma ordem de grandeza.** O ca-estômago entrega ~150 laudos/dia;
a reumatologia processa ~3.700. Qualquer custo por laudo — LLM incluído — multiplica por 25.

⚠️ **Entrada e saída divergem em 121.626 linhas** (914.286 contra 792.660). A causa não foi
levantada; pode ser janela de carga, filtro posterior ou perda. **Medir antes de usar qualquer um
dos dois números como denominador.**

---

## 2. Como o legado decide

```python
df_laudos['fl_relevante'] = df_laudos['achados'].apply(lambda x: 1 if x != [] else 0)
```

**Relevância é presença de achado léxico que sobreviva à negação.** Sem limiar, sem score, sem
juiz. É o mesmo desenho do ca-cólon, e traduz-se diretamente para o `nlp_engine`:
`findings` + `negation`, `llm_router.enabled: False`.

### Baseline de paridade — distribuição dos 2.971 relevantes

| achados | n |
|---|---|
| `artrite reumatoide` | 1.757 |
| `sacroileite` | 898 |
| `artrite psoriasica` | 98 |
| `espondilite` | 95 |
| `sacroileite` + `artrite reumatoide` | 56 |
| `espondilite` + `artrite psoriasica` | 24 |
| `espondilite` + `sacroileite` | 15 |
| `artrite reumatoide` + `artrite psoriasica` | 15 |
| `sacroileite` + `artrite psoriasica` | 9 |
| `artrite reumatoide` + `nodulo reumatoide` | 2 |
| três achados (2 combinações) | 2 |

**Doze combinações, e duas respondem por 89%.** Este é o alvo de paridade: mesma coorte, mesma
distribuição, comparada por `record_id`.

ℹ️ **`nodulo_reumatoide` é quase morto** — 2 ocorrências em 2.971, nunca isolado. Candidato a
revisão clínica, não a tradução automática.

---

## 3. A régua legada

O `CONFIG` do notebook já está **no formato que o `nlp_engine` espera** — `negation`,
`organs.<x>.seeds/regex`, `findings`, `semantic`. **Migração é tradução de config, não reescrita.**

### 3.1 O que é da reumatologia

**`organs.reumatologia`** — 97 seeds e 19 regex, cobrindo coluna, articulações periféricas,
estruturas vertebrais e sinoviais.

**`findings.reumatologia`** — 5 achados, 29 termos:

| achado | termos |
|---|---|
| `espondilite` | 4 |
| `sacroileite` | 8 |
| `artrite_reumatoide` | 6 |
| `artrite_psoriasica` | 8 |
| `nodulo_reumatoide` | 3 |

**`negation`** — 23 frases, janela de 7 tokens, **sem direção declarada** (o legado aplica janela
simples). ⚠️ A `0.11.1` fixou `left` como default no semântico; a direção efetiva do legado precisa
ser medida, não assumida.

### 3.2 🔴 Vocabulário estrangeiro embutido

O `CONFIG` declara **20 órgãos** para uma linha de reumatologia:

```
figado · vias_biliares · vesicula_biliar · pancreas · baco · rins · ureteres
bexiga · adrenais · utero · ovarios · prostata · vasos · peritonio
retroperitonio · pulmao · musculoesqueletico · conclusao · colon_reto · reumatologia
```

E `findings` carrega o bloco inteiro do **cólon** (`lesao`, `polipo`, `ulceracao`,
`tumor_ou_massa`) junto com o da reumatologia.

**Mesma classe de problema já mapeada no ca-cólon**, que por sua vez carrega 97 seeds de
reumatologia. Os dois notebooks são cópias um do outro com o alvo trocado.

⚠️ **`TARGET_ORGAN = "reumatologia"` limita o escopo em runtime**, então o vocabulário estrangeiro
provavelmente não altera a saída. **"Provavelmente" não é medição** — remover muda ou não muda, e
isso se decide com um A/B, não por leitura.

ℹ️ O título da célula do modelo diz `Target Organ = DII`. Resíduo de cópia.

---

## 4. A entrada

Filtro em três camadas sobre `exame_nr`, `dsc_codigo_txt` e `cod_procedimento_txt`:

1. **modalidade** — tomografia, ultrassom, ressonância (`tc`, `usg`, `rm`, `rnm`)
2. **região** — articulação, bacia, braço, cervical, coluna, cotovelo, coxa, dorsal, joelho,
   lombar, lombossacra, mão, ombro, osso, pé, perna, pescoço, punho, quadril, sacro, temporo /
   temporomandibular, tornozelo, vértebra
3. **exclusões** — angio, artérias, carótidas, venoso, PAAF, punção, biópsia

🔴 **A região craniana foi REMOVIDA em 2026-07-23** — PR 6893, *"Bugfix de exames incorretos"*.
Saíram `crânio`, `cabeça`, `face`, `intracranian` e `mastoid`; `face` foi substituída por
`temporo`, que casa a articulação temporomandibular sem trazer o crânio inteiro.

⚠️ **A migração precisa espelhar a versão de julho, não a de maio.** Reproduzir a lista antiga
reintroduziria em silêncio o defeito que o bugfix corrigiu — e a paridade mediria contra a régua
errada.

### 🔴 Dois defeitos no filtro

**`ILIKE '%tc%'` casa qualquer substring.** Não é `\btc\b` — pega `scan`, `hematocrito`, qualquer
nome com "tc" no meio. A cláusula `RLIKE '\btc\b'` existe **em paralelo**, unida por `OR`, então a
versão frouxa domina e a estrita é inerte.

**`ILIKE '%pe'` e `ILIKE '%pé'`** casam qualquer nome terminado em "pe". Mesmo padrão: há um
`RLIKE '\b(pe|pé)\b'` ao lado que o `OR` torna irrelevante.

Efeito esperado: **entrada inflada**. Consistente com a taxa de 0,37%, a mais baixa do parque.
**Medir quantos laudos entram só por essas duas cláusulas** antes de decidir se corrige na migração
ou depois.

⚠️ O notebook de entrada tem o filtro **duplicado e comentado** logo acima do ativo, e ainda carrega
resíduo de **ateromatose** (tórax, coronária, cálcio, aorta torácica). Ler só o bloco ativo.

---

## 5. 🔴 Não existe gabarito

Não há tabela de conferência para reumatologia — o inventário do schema mostra apenas entrada,
saída, e variantes `dev_`/`tmp` de trabalho. **Mesma situação do ca-cólon.**

Consequência direta: **o critério de aceitação da migração é paridade comportamental, não acerto
clínico.** Não se pode afirmar que a migração melhora ou piora a régua — só que reproduz ou não
reproduz o que já roda.

Qualquer alteração de régua (remover vocabulário estrangeiro, corrigir o filtro, revisar
`nodulo_reumatoide`) é **decisão clínica que exige dono** e vem **depois** da paridade, medida
isoladamente.

---

## 6. O que já está resolvido pelo caminho percorrido

A migração reusa o padrão de ca-cólon e ca-rim:

| item | de onde vem |
|---|---|
| forma da config e cabeçalho | `cancer_rim` — referência da régua de 03/09 |
| `assert` de wheel e `config_version` no backtest | ca-cólon (Lucas) |
| exchange por ambiente | os seis configs existentes |
| medição de paridade por `match_rate` | ca-cólon |
| régua "config não passa com bloco morto" | `.claude/rules/motor-nlp.md` |

---

## 7. Riscos

| risco | efeito | mitigação |
|---|---|---|
| **volumetria 25× a do ca-estômago** | custo e tempo de run inviabilizam iteração ampla | coorte fixa de uma janela curta; nunca rodar a base cheia em bancada |
| **sem gabarito** | não há como afirmar acerto | critério é paridade; acerto vira frente separada com dono clínico |
| **vocabulário estrangeiro** | remover pode mudar saída silenciosamente | A/B com e sem, antes de limpar |
| **filtro de entrada frouxo** | corrigir muda o denominador e quebra a comparação | corrigir **depois** da paridade, medido à parte |
| **divergência entrada × saída** (121.626) | qualquer taxa calculada sobre o denominador errado | apurar antes de publicar número |
| **schema não existe no UC** | bloqueia a subida | é do time da Fábrica; pedir cedo — foi caminho crítico no ca-estômago |

---

## 8. Próximos passos

1. **SPEC de migração** — contrato, escopo, o que não faz. *(este documento a alimenta)*
2. **Config v0** — tradução do `CONFIG` legado, só o bloco da reumatologia.
3. **Coorte de paridade** — janela curta, `record_id` + `exm_laudo_achados` + `fl_relevante` do
   legado como referência.
4. **Medir `match_rate`** — nossa saída contra a do legado, achado a achado.
5. **Pedir o schema `reumatologia`** ao time da Fábrica.

⚠️ Passos 1 a 4 não dependem do schema. O pedido do schema é paralelo e deve sair primeiro por ser
o de maior latência.
