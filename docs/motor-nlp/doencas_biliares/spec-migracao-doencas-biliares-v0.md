# SPEC de migração — Doenças biliares

> **Estágio:** Research fechado, Plan escrito. **Implement não começou.**
> Levantado em **14/09/2026** (inventário do legado) e **25/09/2026** (filtro e volumetria).
> Dono proposto: **Leandro**.
>
> Research completo: `docs/motor-nlp/_processo/migracao-biliares-e-neuro-inventario.md`.
> Card guarda-chuva: `303791` — *Plano de Migração algoritmos final*.

---

## 1. O que torna esta linha diferente da próstata

A régua é pequena, mas **o volume é 30 vezes maior** e a linha tem os três riscos que já custaram
retrabalho nas migrações anteriores: vocabulário estrangeiro no dicionário compartilhado, expansão
semântica que produção não consegue reproduzir, e segmentação que difere da linha vizinha.

🟢 **O `CONFIG` do legado já está no formato que o `nlp_engine` espera** — chaves `negation`,
`organs`, `findings`, `semantic`. **Migração é tradução de config, não reescrita.**

🟢 **O legado das biliares e o da neuroimunologia são o MESMO motor** — 45 linhas diferem em 5.210.
O que o Leandro construir para a neuro serve aqui com troca de `TARGET_ORGAN`, nomes de tabela e a
query da janela.

---

## 2. A régua — 6 categorias, 41 termos

| categoria | termos | exemplos |
|---|---|---|
| colecistite crônica | 12 | *calculo no ducto cistico*, *fibrose da vesicula* |
| diagnósticos | 8 | *colecistite cronica*, *coledocolitiase*, *ileo biliar* |
| neoplasias | 8 | *polipo sugestivo de malignidade*, *massa sugestiva de neoplasia* |
| sinais inflamatórios | 7 | *espessamento de parede*, *colecao pericolecistica* |
| colelitíase | 3 | *calculo*, *sinal da parede-Eco-sombra*, *sinal WES* |
| coledocolitíase | 3 | *dilacao*, *calculo no coledoco*, *coledoco dilatado* |

**Portão de órgão:** 18 seeds + 5 regex. **Negação:** 23 tokens, janela de 7.

---

## 3. O universo de entrada — medido

Três colunas (`proced_descricao_ajustado`, `dsc_codigo`, `cod_procedimento`), cada uma com a mesma
régua repetida por `OR`:

```
UPPER(trim(tp_procedimento)) IN ('IMG','IMA')
AND (
      (US|USG|ultra...)  AND ( (abd AND (total|sup)) OR (biliar|hipocondr) )
   OR (rm|ressonancia)   AND colangio
   OR (tc|tomogra)       AND abd  AND NOT (aorta|inferior)
)
```

Mais exclusões de legibilidade no próprio filtro: RTF cru, HTML, *"laudo em pdf"*,
*"sistema especialista"*, *"visualização em rtf"*.

**Medido na gold, 30 dias (25/08–23/09):**

| | exames | /dia |
|---|---|---|
| universo do filtro legado | **97.818** | **3.261** |
| com texto legível | 97.695 | 3.256 |

🔴 **3.261 exames/dia é a segunda maior linha da fábrica**, atrás só da hepatologia. Dimensiona
cluster, custo de LLM se o juiz for ligado, e tempo de run — **decidir isso antes de começar**, não
depois do primeiro run estourar.
⚠️ A legibilidade de 99,9% é da **janela corrente**. Em 12 meses a taxa da gold é muito menor
(~15% no total); não transportar este número para janelas antigas.

---

## 4. Os quatro riscos, levantados antes de começar

### 4.1 🔴 O legado expande termos com embeddings — e os nossos não funcionam em produção

O bloco `semantic` do legado gera termos **por laudo**, com embeddings (`min_sim` 0,65,
`top_k` 24). A plataforma casa literais. É a mesma divergência que explicou a queda da
reumatologia.

**Consequência:** a paridade vai divergir por essa via, e **isso não é defeito da migração**. Tem
de ser medido e nomeado separadamente, senão vira ruído no `match_rate`.

ℹ️ Em HML a camada semântica já roda com modelo real (grant concedido em 23/09, zero
`token_overlap` em 65 mil laudos). Em **produção** ainda cai em fallback — card `305810`. Então:
**ligar semântica em dev/hml é possível; contar com ela em prd, ainda não.**

### 4.2 🔴 Vocabulário estrangeiro no dicionário compartilhado

O `organs` do legado carrega **20 órgãos**, e os dois maiores não são desta linha:

| órgão | peso | pertence a |
|---|---|---|
| `reumatologia` | 116 (97 seeds + 19 regex) | outra linha |
| `neuroimunologia` | 109 | a linha irmã |
| `colon_reto` | 58 | outra linha |
| **`doencas_biliares`** | **23** | **alvo** |

E `findings` tem quatro chaves — `colon_reto` (53 termos) e `reumatologia` (29) entram carregados.
**Remover muda resultado: medir, não limpar no olho.** Mesmo padrão do ca-cólon.

### 4.3 ⚠️ A segmentação difere entre as duas linhas irmãs

`FORCE_FULL_DOC_FOR = {"neuroimunologia"}` contra conjunto **vazio** no biliar. Mesma classe do
`mode: auto` da hepatologia, que descarta 86% do laudo e virou causa raiz de um P0.
🔴 **Não clonar uma na outra.** Declarar explicitamente e conferir `segmentation_coverage`.

### 4.4 Defeitos do próprio legado, a não reproduzir

| defeito | efeito |
|---|---|
| **vírgula faltando** entre literais adjacentes | dois termos viraram um só, concatenado |
| **erro de grafia em seed** — `ducto bilar comum` | âncora provavelmente morta |
| grafia da chave `disgnosticos` | sem efeito; mede a deriva do copia-e-cola |

ℹ️ Os dois primeiros estão nas entradas de `doencas_biliares` **dentro do notebook do neuro**, que
não as usa. Efeito prático provável: zero. **Valor real:** provam que as configs derivaram por
cópia, e é a classe de defeito que a migração deve **contar**, não herdar.

---

## 5. O que aproveitar da lib, e o que deixar de fora

| capacidade | usar? | por quê |
|---|---|---|
| `findings` com `exclude` / `unless` | ✅ | descarta achado benigno de forma determinística, sem depender do juiz |
| `document_vet` | ✅ | laudo de US abdominal conclui normalidade com frequência; foi o que resolveu o DII por config pura |
| `negation` com `direction_default` | ✅ | 23 tokens, janela 7 — traduzir do legado, **declarando** a direção |
| `segmentation.mode` | ✅ declarar | ver 4.3 |
| `quantitative_criteria` | ❌ | a régua biliar não tem limiar numérico |
| `ordinal_extraction` | ❌ | não há classificação ordinal nesta linha |
| `embeddings` | 🟡 depois | ver 4.1 — em dev/hml sim, em prd depende do `305810` |
| `llm_router` (juiz) | 🟡 depois | perfil completo é o alvo, mas entra medindo o **delta** |

🔴 **`relevance_mode` e `llm_router.enabled` sempre declarados explicitamente**, mesmo quando o
valor coincide com o default. Config que declara `mode`, `model` e `prompt_system` sem `enabled`
faz o leitor concluir que o juiz roda — e ele não roda.

---

## 6. Critérios de aceite

1. **Filtro de entrada medido nos dois sentidos**, com custo por termo: quantos exames entram,
   quantos saem, e o que se perde por termo removido. Conferir o escape de `\b` por controle
   manual — no literal SQL ele é BACKSPACE.
2. **Paridade contra a saída gravada do legado** por `id_exame`, três dias ou mais, com as
   divergências **classificadas por via**: expansão semântica · vocabulário estrangeiro ·
   segmentação · defeito do legado · defeito nosso. Um `match_rate` sem essa partição não decide
   nada.
3. **Vocabulário estrangeiro medido antes de removido** — rodar com e sem, na mesma janela.
4. **Volumetria e tempo de run** medidos em dev antes do PR: 3.261 exames/dia muda a conversa
   sobre cluster e custo.
5. **Run em dev ponta a ponta** — runner, `gold_filter`, `column_map`, view e envio.
6. **Schema `doencas_biliares` provisionado em dev e prd** pelo time da Fábrica, pelo fluxo
   próprio — **não** na descrição do PR.
7. Config sem bloco morto, com **cabeçalho e changelog no arquivo**.

---

## 7. Passos, em ordem

| # | passo | por quê |
|---|---|---|
| 1 | pedir os schemas `doencas_biliares` (e `neuroimunologia`) em dev e prd | dependência de terceiro com prazo; foi bloqueio na reumatologia |
| 2 | extrair o `CONFIG` do legado **programaticamente**, não à mão | o `CONFIG` já está no formato do motor; transcrever introduz erro |
| 3 | traduzir e medir o `gold_filter` nos dois sentidos | |
| 4 | primeira corrida `rule_only`, sem semântica e sem vocabulário estrangeiro | estabelece o piso |
| 5 | corridas variando **uma coisa por vez**: vocabulário, segmentação, semântica | é o que permite atribuir a divergência |
| 6 | paridade por `id_exame` e classificação das divergências | |
| 7 | PR para `hml` com os 6 arquivos | checklist em `docs/motor-nlp/checklists/checklist-revisao-pr-ds.md` |
| 8 | só depois: ligar o juiz, medindo o **delta** na mesma janela | |

---

## 8. Relação com a neuroimunologia

A neuro está à frente (PR `7428` aberto). **O que sair dela reaproveita aqui quase inteiro** — o
extrator do `CONFIG`, o padrão de paridade, o formato dos 6 arquivos. A diferença é onde está o
peso: no biliar a régua decide e o portão é estreito; na neuro a régua tem 16 termos e o portão
tem 109 entradas.

⚠️ **E as duas servem de base para birads, endometriose e nódulo pulmonar**, que vivem no mesmo
formato de legado.
