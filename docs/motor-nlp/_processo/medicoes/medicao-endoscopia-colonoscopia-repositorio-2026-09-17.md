# Colonoscopia e endoscopia no repositório clínico — e o que o `gold_filter` do ca-estômago perde

> 17/09/2026 · Fonte: `gold_corporativo_ia.corporativo.tb_gold_mov_exame`
> Janela: **27/08/2025 a 26/08/2026 — 12 meses, 365 dias**

**O achado que decide:** o `gold_filter` do câncer de estômago deixa de fora **55% das endoscopias
digestivas altas** do repositório, e **mais laudo legível do que traz** — 124 por dia contra 106.

---

# 1. O repositório

| | colonoscopia | endoscopia digestiva alta |
|---|---|---|
| **exames no repositório** | **126.351** — 346/dia | **176.697** — 484/dia |
| **legíveis** | **61.376** · 48,6% — 168/dia | **83.935** · 47,5% — 230/dia |
| **só ponteiro** (PDF / outro sistema) | **32.013** · 25,3% — 88/dia | **49.404** · 28,0% — 135/dia |
| **sem laudo nenhum** | **32.962** · 26,1% — 90/dia | **43.358** · 24,5% — 119/dia |

**Menos da metade dos exames tem laudo legível.** O restante se divide entre apontamento para outro
sistema e ausência total de laudo.

ℹ️ **O repositório cobre 13/03/2025 a 16/09/2026** (553 dias). Na janela cheia de 18 meses os
números são 183.066 colonoscopias e 261.324 endoscopias, com legibilidade de 49,1% e 48,0% — o
volume diário é o mesmo, então a operação é estável.

# 2. O que o `gold_filter` do ca-estômago seleciona

A config declara:

```python
'gold_filter': {'keywords': ['endoscop.a digestiva alta', '\\beda\\b'], 'mode': 'any'}
```

⚠️ **O filtro NÃO lê o texto do laudo.** Cada keyword vira `rlike '(?i)<valor>'` sobre
**`proced_descricao`** — `GoldFilterBuilder.keyword_column`, cujo default é `proced_descricao` e que
o runner **não sobrescreve** (`nlp_ia_02_input.py:492`). Não é o `proced_descricao_ajustado` nem o
laudo.

| grupo | exames | /dia | legíveis | /dia |
|---|---|---|---|---|
| **A.** é EDA e **passa** o filtro | **79.968** | 219 | **38.545** | **106** |
| **B.** é EDA e o filtro **NÃO pega** | **96.729** | 265 | **45.390** | **124** |
| **C.** o filtro pega e **não é** EDA | 21 | 0 | 20 | 0 |

🔴 **O filtro é preciso e estreito demais.** O grupo C tem **21 exames em um ano** — ele quase não
traz ruído. O problema é inteiramente de recall.

# 3. O que está sendo perdido — 94% em três descrições

| exames | descrição |
|---|---|
| **61.902** | `endoscopia com biopsia e/ou citologia` |
| **19.314** | `endoscopia` |
| **12.009** | `endoscopia com biopsia e teste urease` |
| 1.204 | `endoscopia com biopsia e/ou citologia + polipectomia por endoscopia` |
| 882 | `(40202038) endoscopia com biopsia e/ou citologia` |
| 520 | `(40202615) endoscopia com biopsia e teste urease` |
| 293 | `biopsias ou citologia (endoscopia alta ou baixa)` |
| 199 | `endoscopia com cromoscopia e biopsia e/ou citologia` |

São endoscopias digestivas altas que **não escrevem "digestiva alta" na descrição** — usam a
nomenclatura de faturamento TUSS. E o `\beda\b` também não as alcança, porque a sigla não aparece
nesse campo.

# 4. 🔴 O que a linha de fato processa

Os **219/dia** que passam o filtro batem com os **233/dia** que o ca-estômago processa em produção
(medido em 11 dias com run, 03 a 17/09).

**Mas dos 219, apenas 106 têm texto legível.** Os outros 113 por dia são ponteiro ou laudo vazio — o
motor executa sobre eles e não há o que decidir.

⚠️ **Correção de uma leitura anterior:** a convergência entre "230 legíveis/dia no repositório" e
"233/dia processados" foi apresentada como validação cruzada. **Não é.** Os 233 correspondem ao que
o filtro seleciona, legível ou não; a coincidência com os legíveis do repositório inteiro é acaso.

# 5. Como cada número foi construído

**Classificação** pelo `exame_nr` — `regexp_replace(lower(coalesce(proced_descricao_ajustado,
cast(cod_procedimento as string), '')), '[   ]', ' ')`.

✅ **O vocabulário foi medido antes, não suposto**, e revelou que a maior parte de "endoscopia" no
repositório **não é digestiva**: naso-sinusal, faringo-laríngea, coluna, ureter, colo vesical,
transesofágica, anestesia e rubricas de faturamento (`procedimentos
hemodinamica/endoscopia/anatomo - microdata`). Todas excluídas por regra explícita. São **583**
variações de colonoscopia e **719** de endoscopia alta.

**Legível** = removido o boilerplate conhecido, sobram ≥ 40 caracteres.

⚠️ **O limiar é irrelevante, e isso foi verificado:** a distribuição do resíduo é bimodal e a faixa
40–79 tem **62 casos em 440 mil**. Na faixa 20–39, **70.496 de 70.583** são literalmente
`laudo gerado por sistema especialista` — resíduo de ponteiro cujo texto não terminava em ponto.

**Só ponteiro** = tem texto, mas nada sobra após remover o boilerplate, **e nenhum outro laudo do
mesmo exame é legível**. Exame com laudo real que menciona PDF no rodapé conta como legível.

Os ponteiros, por frequência: `laudo gerado por sistema especialista. para visualizar o laudo acesse
o viewer` (70.354) · a mesma frase com o rodapé do RTF (50.968) · `para visualizar, acesse a
'imagem'` (27.459) · **`laudo em pdf` (19.453)** · `liberado por um sistema especialista` (1.750).

## Fora da conta

Retossigmoidoscopia (**11.477** no repositório, exame distinto) e procedimentos terapêuticos por
endoscopia — CPRE, gastrostomia, mucosectomia, corpo estranho, hemostasia, ligadura de varizes,
papilotomia — cerca de 4 mil.

# 6. Encaminhamento

1. 🔴 **Ampliar o `gold_filter` do ca-estômago.** Acrescentar `endoscopia com biopsia`,
   `endoscopia com cromoscopia` e o `endoscopia` puro recuperaria ~94% da perda.
   ⚠️ **Medir o custo em volume antes** — a régua de filtro de entrada exige os dois sentidos, com
   número. Entrar 265 exames/dia a mais muda tempo de run e consumo de LLM.
2. 🟡 **A medição já estava pedida e não foi feita.** O cabeçalho da `0.2.0` registra: *"o `\beda\b`
   estava com BACKSPACE literal na origem, então a sigla nunca casava. Reescrito correto: pode
   ampliar levemente a captação — medir quantos entram só pela sigla."* O problema é bem maior que
   a sigla.
3. 🟡 **Verificar as outras linhas.** O `keyword_column` default é `proced_descricao` para todas;
   qualquer `gold_filter` escrito supondo que filtra o laudo tem o mesmo desvio.
4. ℹ️ **É a mesma classe do achado do TI-RADS** — lá o `gold_filter` não seleciona punção e 67
   exames citando TR4 nunca chegam ao motor. Aqui a ordem de grandeza é outra: **45.390 laudos
   legíveis em 12 meses**.

---

⚠️ **Sem card.** Filtro de entrada é alçada nossa (POP-IA-08), e a decisão de ampliar depende da
medição de custo do item 1.
