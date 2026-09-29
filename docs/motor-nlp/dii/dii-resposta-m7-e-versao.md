# DII — resposta aos dois pontos do plano de medição

> Respostas a **M7 (órgãos)** e à **versão da lib**. Escrito em 2026-09-10.
> O plano de medição está aprovado; estes são os dois ajustes antes de rodar.

---

## 1. M7 — órgãos entra, sim

O documento original listava "órgãos" entre os 5 pontos de divergência e **não o media**. Era
lacuna. O portão de órgão decide relevância, então medir os outros quatro sem ele deixaria o número
final sem explicação.

✅ **O denominador proposto está correto.** Nenhuma das duas configs usa `skip_organ_gate` —
verificado, zero ocorrências nas duas. O portão vale para todos os achados, então
*"laudos com ≥1 achado casado"* é o denominador certo.

### 1.1 A união não é simétrica — e isso muda a leitura

As duas listas não diferem só em tamanho. Diferem em **natureza**:

| ramo | seeds | o que são |
|---|---|---|
| **imagem** | 18 | topografia pura — `colon`, `sigmoide`, `reto`, `ileo`, `ceco`, `perianal`, `mesorreto` |
| **colonoscopia** | 12 | topografia **mais as próprias doenças** — `doenca de crohn` e `crohn` estão entre as seeds; também `intestino delgado` e `jejuno` |

E dos 12 regex da colonoscopia, **6 descrevem achado, não topografia**.

**Consequência prática:** unir leva `crohn` para o ramo de imagem. Ali, **qualquer laudo que cite
Crohn passa a ancorar o portão** — que deixa de filtrar por topografia naqueles laudos.

Se o M7 der `0→1` alto no corpus de imagem, é provavelmente isso. **Separar na contagem quantos
vieram das seeds de doença** (`crohn`, `doenca de crohn`) e quantos vieram de topografia nova. Sem
essa separação, o número existe mas não decide nada.

### 1.2 Armadilha de acento — as duas listas vivem em regimes opostos

Isto pode fazer o M7 dar "sem efeito" quando na verdade a âncora nunca disparou:

- **`seeds`** passam por `norm()` → as de imagem estão **sem acento de propósito**;
- **`regex`** rodam na **sentença crua** (`_organ_spans`, `rule_engine.py:136`) → os de imagem
  receberam **classes de acento adicionadas**, e os de colonoscopia já vinham acentuados.

Uma união ingênua mistura os dois regimes e produz âncora que **nunca casa**, em silêncio.

**O que fazer:** além do `repr()` já previsto, **contar quantas vezes cada âncora unida casou pelo
menos uma vez**. Âncora com zero disparos entra no relatório como **não-medida**, nunca como neutra.

---

## 2. Versão da lib — `0.12.3`

**A plataforma instala por `latest`. Em 10/09 isso resolveu para `0.12.3`** nas quatro linhas de
produção — verificado no campo `engine_version` das tabelas de saída (hepatologia, tirads,
cancer_estomago, transplante_pulmao; 7.037 laudos no dia).

### 2.1 Pinar explicitamente, não usar `latest`

```
nlp-engine==0.12.3
```

A versão em produção mudou **quatro vezes em nove dias** — `0.10.0` → `0.10.1` → `0.11.2` →
`0.12.3` — sem ninguém tocar no job. Se uma versão nova publicar no meio da medição, as duas metades
deixam de ser comparáveis **e não há sinal disso no resultado**.

### 2.2 🔴 O `0.6.6` do harness local não serve para esta medição

O tratamento de texto mudou entre `0.6.6` e `0.12.3`, e isso muda **quais termos casam** — que é
exatamente o que M3, M4, M5 e M7 medem.

| versão | o que corrigiu | por que pega o DII |
|---|---|---|
| **`0.11.2`** | espaço apagado antes de palavra acentuada: `de íleo` → `deíleo`, `com fístula` → `comfístula`, e o achado some | pega `íleo`, `cólon`, `fístula`, `doença`. A função **nunca acertou**: 825 junções em 616 laudos, **zero legítimas** |
| **`0.9.1`** | regra de boilerplate descartava a **linha**; em laudo de uma linha só, o laudo inteiro sumia — texto tratado vazio, `fl_relevante = 0`, em silêncio | laudo legado numa linha só é plausível |

⚠️ **Ter batido dígito a dígito com o runner em 05/08 não protege:** o runner daquela data também
rodava versão antiga. A paridade era com o defeito presente **nos dois lados**.

### 2.3 Se refazer com `0.12.3` custar caro

O mínimo aceitável é rodar a **baseline** (régua própria, sem variante) nas **duas versões** e
reportar o delta:

- **delta zero nos três dias** → o `0.6.6` fica justificado, com número;
- **delta diferente de zero** → o plano inteiro roda na `0.12.3`.

---

## 3. Checklist antes de rodar

- [ ] `nlp-engine==0.12.3` pinado, e a versão **registrada no summary** — foi o que faltou em 05/08.
- [ ] Baseline nas duas versões, se o `0.6.6` for mantido.
- [ ] M7 com a contagem separada entre seeds de **doença** e de **topografia**.
- [ ] M7 com contagem de disparos por âncora unida — zero disparos = não-medida.
- [ ] `repr()` de todo regex conferido depois de carregado.

---

## 4. Uma observação sobre o escopo do relatório

**M1 usa a Gold; M2–M8 usam o legado.** Está correto — é o que isola a régua do filtro de entrada.
Só implica que M2–M8 medem sobre laudos que o `gold_filter` novo talvez nem selecione.

Não invalida nada, mas o relatório precisa dizer com todas as letras: o número final é
**"o que a régua decidiria"**, não **"o que a plataforma entregaria"**. São coisas diferentes, e a
migração da reumatologia mostrou que a diferença cabe num filtro de entrada.
