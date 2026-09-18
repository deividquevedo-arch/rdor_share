# Ganho de ampliar o `gold_filter` do câncer de estômago — medido

> 17/09/2026 · Fonte: `gold_corporativo_ia.corporativo.tb_gold_mov_exame`
> Janela: **27/08/2025 a 26/08/2026 — 12 meses, 365 dias**

**Uma palavra-chave recupera quase toda a perda, e o ruído é de 213 exames em um ano.**

---

## 1. A exclusão não é intencional

A SPEC de negócio (`spec-negocio-cancer-estomago-v1.md` §2) declara o universo como
**"entram: endoscopia digestiva alta (EDA)"**, e apresenta o `gold_filter` como a *implementação*
desse universo — não como um recorte dentro dele:

> **Entram:** endoscopia digestiva alta (EDA).
> Filtro textual: `gold_filter.keywords: ['endoscop.a digestiva alta', 'eda']`.

O único risco que a SPEC registra é o **oposto**: sem o filtro, a entrada foi de 4.818.237 laudos em
vez de 10.783. Não há, em nenhum ponto do documento, decisão de escopo que exclua subconjunto de
EDA — nem por via de acesso (biópsia, cromoscopia), nem por finalidade.

🔴 **Conclusão: os exames que ficam de fora são lacuna de implementação, não recorte de escopo.**
A régua clínica (§3) decide sobre o *conteúdo do laudo*; o filtro decide sobre a *descrição do
procedimento*. Escrever a keyword pensando no conteúdo filtra o campo errado.

## 2. O ganho, por palavra-chave

Classificação de EDA pelo `exame_nr`; o filtro atua sobre `proced_descricao`.
Legível = removido o boilerplate de ponteiro, sobram ≥ 40 caracteres.

| | exames | /dia | legíveis | /dia |
|---|---|---|---|---|
| **passa hoje** | 79.760 | 218,5 | **38.448** | **105,3** |
| **+ `endoscopia com biopsia`** | **76.533** | **209,7** | **36.991** | **101,3** |
| + `endoscopia com cromoscopia` | 246 | 0,7 | 132 | 0,4 |
| segue fora, sem candidata | 43.641 | 119,6 | 11.870 | 32,5 |

**As duas palavras-chave quase dobram a entrada legível** — de 105,3 para 207,0 laudos por dia.
Uma delas responde por **99,6% do ganho**; a de cromoscopia é marginal e entra por coerência de
vocabulário, não por volume.

ℹ️ Converge com a medição de repositório do mesmo dia (79.968 exames e 38.545 legíveis passando),
com diferença de 0,3% — lá as variações de nomenclatura foram enumeradas uma a uma, aqui a
classificação é por expressão regular.

## 3. O custo, medido no sentido contrário

A régua de filtro de entrada exige os dois sentidos. Do que as duas keywords novas selecionam:

| | exames | /dia |
|---|---|---|
| é EDA e **entra** pela keyword nova | **76.779** | 210,4 |
| **não é EDA** e entra pela keyword nova | **213** | **0,6** |

🟢 **Precisão da ampliação: 99,72%.** O ruído é de 213 exames em doze meses — 0,6 por dia.
O filtro atual já era preciso (51 não-EDA em um ano); ampliá-lo **não degrada essa propriedade**.

## 4. O que a ampliação custa em execução

Não é gratuita, e o número que dimensiona é o de laudos **legíveis**, não o de exames:

- **+101,7 laudos legíveis por dia** entram no motor — praticamente dobra o corpus da linha.
- O juiz LLM da `0.6.9` hoje quase não é chamado (145 de 151 laudos abaixo do piso da banda no
  primeiro dia em produção). A ampliação **não muda a banda**, então a expectativa é que a
  proporção se mantenha — mas isso é extrapolação, não medição.
- ⚠️ **A taxa de relevância medida em produção (3,97%) foi apurada sobre o corpus estreito.**
  Ampliar a entrada invalida a comparação com a janela de homologação até que haja um dia inteiro
  rodado com o filtro novo.

## 5. Encaminhamento

1. **Ampliar para** `['endoscop.a digestiva alta', '\beda\b', 'endoscopia com biopsia',
   'endoscopia com cromoscopia']`.
2. **Rodar um dia em dev com o filtro novo** e comparar contra o mesmo dia com o filtro atual:
   volume processado, chamadas ao juiz, taxa de relevância e tempo de run.
3. **Não levar lista ao negócio** a partir do delta sem a passada do item 2 — a homologação de
   0,600 / 1,000 é contra o corpus estreito.
4. Os **43.641 que seguem fora** (11.870 legíveis, 32,5/dia) exigem levantamento próprio do
   vocabulário; ficam para um segundo ciclo, depois de o item 2 dimensionar o custo.

---

⚠️ **Sem card.** Filtro de entrada é alçada da especialidade (POP-IA-08 §Você edita); a decisão de
ampliar depende do run do item 2.
