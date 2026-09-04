# SPEC — migração da reumatologia para o `nlp_engine`

> Levantamento em [`briefing-migracao-reumatologia-v0.md`](briefing-migracao-reumatologia-v0.md).
> Escrita em 2026-09-04. Escopo da **V1**.
>
> **Fonte da regua:** `IAAzureDatabricksReumatologia`, branch **`hml`**, commit `b200286`
> (2026-07-23) — a que o job de producao executa. Nao usar a `main` (parada em 2024) nem a copia
> em `fabrica-ia-plataforma` (parada em 2026-05-15).

---

## 1. Objetivo

Reproduzir, na plataforma nova, o comportamento do algoritmo legado de reumatologia — **sem
alterar a régua**.

**Critério de aceitação: paridade comportamental.** Não existe gabarito clínico para esta linha,
então a migração não pode afirmar acerto. Só pode afirmar equivalência.

---

## 2. Entrada

| campo | origem |
|---|---|
| `id_exame` | `record_id` do legado |
| `exm_laudo_texto` | idem |
| `exm_mod` | idem |
| `exm_tipo` | idem |
| `dt_exame` | `exm_data` |

**Filtro de entrada:** reproduzido do legado **sem correção**, via `gold_filter.keywords`.

🔴 **Espelhar a versão de 2026-07-23, não a anterior.** O PR 6893 removeu a região craniana
(`crânio`, `cabeça`, `face`, `intracranian`, `mastoid`) e pôs `temporo` no lugar de `face`.
Reproduzir a lista antiga reintroduz o defeito que o bugfix corrigiu, e a paridade passa a medir
contra a régua errada.

⚠️ Os dois defeitos do filtro (`ILIKE '%tc%'` e `ILIKE '%pe'`, ambos casando substring e ambos
neutralizando um `RLIKE` estrito posto ao lado por `OR`) **sobrevivem ao bugfix** — verificado na
`hml`, 3 ocorrências de cada. São **preservados na V1**: corrigi-los muda o denominador e torna a
paridade não comparável. Viram frente própria, medida à parte.

---

## 3. Saída

Contrato padrão da lib: `fl_relevante`, `confidence_score`, `config_version`, `engine_version`,
`specialty_id`, `findings`.

**Mapeamento de paridade:** `findings` da lib ↔ `exm_laudo_achados` do legado. Os nomes dos cinco
achados são preservados literalmente — `espondilite`, `sacroileite`, `artrite_reumatoide`,
`artrite_psoriasica`, `nodulo_reumatoide`.

---

## 4. A régua

**Traduzida do `CONFIG` legado, apenas o bloco da reumatologia.**

| bloco | conteúdo |
|---|---|
| `organs.reumatologia` | 97 seeds · 19 regex |
| `findings` | 5 achados · 29 termos |
| `negation` | 23 frases · janela 7 |
| `embeddings` | `min_sim 0,65` · `top_k 24` |
| `llm_router` | **`enabled: False`, declarado** |
| `segmentation` | `full_doc` — o legado usa `FORCE_FULL_DOC_FOR = {"reumatologia"}` |

### 4.1 O que NÃO entra

**Os 19 órgãos estrangeiros** (fígado, vias biliares, pâncreas, baço, rins, ureteres, bexiga,
adrenais, útero, ovários, próstata, vasos, peritônio, retroperitônio, pulmão, musculoesquelético,
conclusão, cólon-reto) e **os 4 achados do cólon** (`lesao`, `polipo`, `ulceracao`,
`tumor_ou_massa`).

**Justificativa:** `TARGET_ORGAN = "reumatologia"` já os exclui em runtime no legado.

⚠️ **Isto é hipótese, não fato.** A V1 só pode omiti-los se o A/B provar saída idêntica com e sem.
**Se a paridade falhar, esta é a primeira suspeita.**

### 4.2 Direção da negação

O legado declara `window_tokens: 7` e **nenhuma direção**. A lib exige `direction` explícita.

**A direção efetiva do legado precisa ser medida**, não inferida. Até a medição, a config declara
`left` — o default fixado na `0.11.1` — e a divergência, se houver, aparece na paridade.

---

## 5. O que esta SPEC **não** faz

- **Não corrige a régua.** Vocabulário estrangeiro, filtro frouxo e `nodulo_reumatoide` quase morto
  (2 ocorrências em 2.971, nunca isolado) ficam **como estão**.
- **Não liga o juiz LLM.** O legado não tem juiz. Ligá-lo mudaria o comportamento e invalidaria a
  paridade — e a volumetria de ~3.700 laudos/dia torna o custo uma decisão de produto, não técnica.
- **Não entrega lista ao negócio.** Paridade não é homologação. A régua atual nunca foi validada
  contra gabarito.
- **Não apura a divergência de 121.626 linhas** entre entrada e saída do legado. Fica registrada
  como pendência.

---

## 6. Critério de aceitação

**Coorte:** janela curta e integralmente processada pelo legado — três a cinco dias, ~12.000 laudos.

⚠️ **Nunca a base cheia.** São 914.286 laudos e ~3.700/dia; a iteração precisa de coorte fixa.

| medida | alvo |
|---|---|
| `match_rate` de `fl_relevante` por `record_id` | **≥ 99%** |
| `match_rate` do conjunto de achados | **≥ 99%** |
| divergências | **enumeradas e explicadas uma a uma** — nenhuma "provavelmente ruído" |

**Divergência não explicada reprova.** É a lição do ca-estômago: `to_plain` colava palavras havia
versões, a suíte passava, e o defeito só apareceu ao ler os laudos entregues.

### 6.1 Pré-condição da medição

Antes de ler qualquer `match_rate`: **confirmar que a coorte não é vazia e que os dois lados
cobrem os mesmos `record_id`.** Fila vazia fecha com sucesso — foi exatamente o que aconteceu no
run de ca-estômago de 04/09 (`gold=0`, etapa `input` com SUCESSO, nada medido).

---

## 7. Riscos

| risco | mitigação |
|---|---|
| omitir o vocabulário estrangeiro muda a saída | A/B com e sem, antes de fixar a V1 |
| direção da negação diverge | medir; a divergência aparece na paridade |
| volumetria inviabiliza iteração | coorte fixa; nunca a base cheia |
| schema `reumatologia` não existe no UC | pedir ao time da Fábrica **já** — maior latência do caminho |

---

## 8. Sequência

1. Config v0 — tradução literal, só o bloco da reumatologia.
2. Coorte de paridade extraída do legado (`record_id`, `fl_relevante`, `exm_laudo_achados`).
3. Run na plataforma nova, mesma coorte.
4. `match_rate` e enumeração das divergências.
5. A/B do vocabulário estrangeiro.
6. PR com cabeçalho, changelog e o `match_rate` no arquivo.

⚠️ **O pedido do schema é paralelo ao passo 1** e sai primeiro. No ca-estômago o schema foi o
caminho crítico — não dependia de nenhuma validação e mesmo assim atrasou a subida.
