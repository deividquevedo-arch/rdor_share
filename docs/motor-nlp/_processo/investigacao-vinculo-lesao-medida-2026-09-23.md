# Vínculo lesão↔medida — investigação da causa, e o que corrige

> **A correção do defeito original FUNCIONA — 117 rebaixamentos, 5 de 5 adjudicados corretos.**
> **O que a quebra são 65 entregas falsas, e a causa dominante é a LEGENDA do ACR.**

- **Data:** 2026-09-23 · **Card:** `306034` — *[NLP Engine] TI-RADS entrega a medida do nódulo
  errado: não existe vínculo*
- **Base:** A/B pareado em dev, 14.939 laudos, janela 08–22/09, `0.14.0` × `0.15.1`.
  Única variável entre os braços: a versão da lib — provado por diff dos parâmetros.

---

## 1. O quadro

| | 15 dias | por dia |
|---|---|---|
| ✅ rebaixamentos legítimos | **117** | ~7,8 |
| 🔴 entregas falsas introduzidas | **65** | ~4,3 |

Adjudicação de 5 + 5 por leitura do laudo: **rebaixamentos 5/5 corretos**, **promoções 5/5 falsas**.

## 2. As três causas, separadas por medição

| causa | evidência | alcance |
|---|---|---|
| **menção de LEGENDA usada no vínculo** | 48 das 65 promoções ocorrem em laudo com menção `TR4` **dentro** do bloco de recomendações | **74%** |
| **medida de LOBO** alcançada pela janela de sentença | `Categoria ACR TI-RADS: TR4` e `Lobo esquerdo mede: 4,5 x 1,7 x 1,3 cm` em linhas adjacentes | o restante |
| **filtro de legenda incompleto** | a corrida exige passo ±1; a legenda repete cada categoria | 16 laudos mudam de `maxcat` |

🟢 **Zero rebaixamentos ocorrem em laudo com legenda** — corrigir a legenda **não custa nada** do lado
que está certo.

## 3. 🔴 A causa raiz é reuso errado, não lógica errada

**A lib já calcula quais menções são legenda** — `legend_indices` e `legend_mentions`, no
`ordinal_summary`, desde a `0.10.1`. O TI-RADS declara `aggregation_legend_filter: {enabled: True}`.

**O vínculo consumiu `ordinal_mentions` cru**, que contém as menções excluídas. O filtro existe, é
calculado, é correto — e foi ignorado.

## 4. O que a lib já tem, e que o desenho deve reusar

| peça | o que sabe | onde |
|---|---|---|
| `legend_indices` | quais menções são enumeração, não achado | `ordinal_extraction` |
| `_ignored_section_ranges` | faixas de caracteres de seções que não descrevem achado | `rule_engine`, via `findings_ignore_sections` |
| `anchor.text` do critério | o substantivo da lesão (`n[oó]dul`) | config da especialidade |
| `_span_gap` / `finding_organ_max_chars` | proximidade por caracteres dentro da sentença | `rule_engine` |

⚠️ **O que a lib NÃO tem:** os spans dos achados com offset de documento. `process_rule_based`
calcula por sentença e **descarta** — exatamente como o `OrdinalMention` descartava a posição antes
da `0.15.0`.

## 5. O formato do laudo decide o desenho — medido em 1.326 laudos com TR4 efetivo

| formato | laudos | % |
|---|---|---|
| fluido / lista com marcador | ~1.100 | **83%** ← o vínculo funciona |
| estruturado (`Dimensões:` / `Medidas:` em campos) | **189** | 14% |
| legenda do ACR no corpo | **84** | 6,3% |
| medida de lobo presente | **1.237** | **93%** |

🔴 **Nenhuma régua de vocabulário simples serve:** a linha correta `- Nódulo sólido **no lobo
direito**, medindo 0,4 cm, TI-RADS 4` contém `lobo`. Excluir por `lobo` mataria a maioria dos casos
certos.
🔴 **E a âncora sozinha também não:** `Dimensões: 0,4 x 0,4 x 0,3 cm` não contém `nódulo` (o
cabeçalho está linhas acima), e a legenda `"Para nódulos de diâmetro maior igual a 1,5 cm"` contém.

## 6. O desenho proposto — três camadas, em ordem de custo

**(1) Excluir menção de legenda do vínculo.** Passar `legend_indices` ao `step_measure` e filtrar.
Reuso puro, sem lógica nova. **Cobre 74% do dano, custo zero no lado correto.**

**(2) Fechar o filtro de legenda para categoria repetida.** A corrida passa a aceitar passo `0`, e o
comprimento passa a contar categorias **distintas** — senão `TR4 TR4 TR4 TR4` viraria legenda.
Medido sobre as sequências reais de 2.661 laudos: **16 mudam de `maxcat`, ZERO entregas removidas**,
com dois controles passando — o conjunto removido é **superconjunto** do atual, e nenhum laudo piora.

**(3) A medida tem de ser de LESÃO, não da glândula.** A unidade da medida precisa conter o
substantivo que o critério já declara em `anchor.text`. Resolve o lobo no formato fluido.
🔴 **Não resolve o formato estruturado (189 laudos, 14%)**, onde o substantivo está no cabeçalho do
bloco. Esse caso exige o conceito de **bloco de lesão** — do cabeçalho (`Nódulo N`, `N1:`) até o
próximo — e é decisão de escopo.

## 7. ⚠️ Duas afirmações minhas RETIRADAS nesta investigação

1. **"A legenda entrega 50 laudos em produção"** — falso. O teste usou `ordinal_mentions.start`,
   que **não existe em produção**: os offsets entraram na `0.15.0` e prd roda `0.12.3`. Em Spark
   `null < x` é `null`, o filtro devolveu zero para todos e a leitura inverteu. Os dois laudos
   conferidos à mão têm **TR4 real**.
2. **"O extrator sub-reporta a medida em 11,5%"** — não se sustenta como estava. A diferença entre
   os braços foi atribuída a sub-reporte do LLM, mas a origem do valor maior era a **legenda** e o
   **lobo**. Precisa ser remedida depois das correções, excluindo as fontes conhecidas.

🟢 **O que pegou as duas foi o mesmo reflexo:** 100% e 0% são gatilho, e número que confirma o
esperado é o que menos se confere. A reimplementação fiel do filtro de legenda foi validada contra
o motor — **2.661 de 2.661** — antes de qualquer conclusão sair dela.

## 8. O que fica pendente de decisão

- **Escopo do formato estruturado** (189 laudos, 14%): entra agora ou fica declarado?
- **`CA6` por terceiro:** a adjudicação foi feita por quem escreveu o código.
