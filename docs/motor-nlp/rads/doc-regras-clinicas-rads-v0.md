# Referência Clínica — Sistemas xxRADS (v0)

**Escopo:** documentação de referência das regras clínicas dos sistemas xxRADS relevantes para o NLP Engine.
**Uso:** insumo para configuração de YAML (`relevance_policy`, `promote_categories`, `aggregation_exclude`) e para revisão clínica de divergências no piloto.
**Fonte:** ACR (American College of Radiology) / SBR (Sociedade Brasileira de Radiologia).
**Vinculado a:** `doc-plano-implementacao-rads-extraction-v0.md`, `notas/00-linha-evolucao-xxrads-nlp-engine-v0.md`

---

## 1. Princípios comuns a todos os sistemas

- São **escalas ordinais de risco** — quanto maior a categoria, maior a suspeita de malignidade.
- A **decisão clínica** (biópsia, seguimento, alta) é exclusivamente do médico; o RADS estratifica, não diagnostica.
- Categorias altas **não garantem** malignidade — são probabilísticas.
- Quando um laudo contém **múltiplas menções** (ex.: dois nódulos com categorias diferentes), a conduta deve considerar o achado de **maior risco** — base para a política `max_category` no motor.
- **Negação invalida a menção:** *"BI-RADS 4 descartado após avaliação"* não configura suspeita — o motor trata via `negation_window`.
- **Contexto de normalidade:** frases como *"ausência de achados BI-RADS ≥ 3"* são negativas.
- **Nenhuma categoria fora da faixa declarada é válida** — deve ser auditada como `invalid_candidate`, nunca silenciada.

---

## 2. BI-RADS (mama — ACR)

**Aplicável a:** mamografia, ultrassonografia mamária, ressonância mamária.


| Categoria | Risco de malignidade | Conduta recomendada                                                |
| --------- | -------------------- | ------------------------------------------------------------------ |
| 0         | Inconclusivo         | Necessita imagem complementar                                      |
| 1         | 0%                   | Negativo; seguimento de rotina                                     |
| 2         | 0%                   | Achado benigno; seguimento de rotina                               |
| 3         | < 2%                 | Provavelmente benigno; controle em 6 meses                         |
| 4         | 2%–95%               | Suspeito; **biópsia recomendada**                                  |
| 4A        | > 2% a ≤ 10%         | Baixa suspeita                                                     |
| 4B        | > 10% a ≤ 50%        | Suspeita intermediária                                             |
| 4C        | > 50% a < 95%        | Moderadamente alta suspeita                                        |
| 5         | ≥ 95%                | Altamente sugestivo de malignidade; **biópsia obrigatória**        |
| 6         | Confirmado           | Malignidade confirmada por biópsia; acompanhamento pós-diagnóstico |


**Regras específicas:**

- **Categorias 4, 4A, 4B, 4C, 5 e 6** devem promover `fl_relevante = 1`.
- **Categorias 1, 2 e 3** são negativas para o programa — não promovem.
- **Categoria 0** é inconclusiva — não deve promover sem complementação.
- **BI-RADS 9 não existe** — qualquer número fora de 0–6 é inválido (`invalid_candidate`); isso inclui o "9" que aparecia no legado como artefato de OCR.
- **Subcategorias 4A/4B/4C** requerem normalização de espaço e capitalização: `"4 A"`, `"4a"`, `"4B "` devem todos resolver para a categoria canônica.

**Config YAML de referência:**

```yaml
rads_extraction:
  systems:
    bi_rads:
      aliases: ["BI-RADS", "BIRADS", "BI RADS", "ACR BI-RADS", "categoria"]
      categories: ["0","1","2","3","4","4A","4B","4C","5","6"]  # ordem = ranking p/ max
      patterns:
        - '(?:BI[- _]?RADS|BIRADS|categoria)[\s:.°-]*(\d(?:\s?[ABC])?|iv|vi|v|i{1,3})'
      normalization:
        roman_to_arabic: true
      relevance_policy:
        promote_categories: ["4","4A","4B","4C","5","6"]
```

---

## 3. LI-RADS (fígado — ACR)

**Aplicável a:** TC e RM de abdome em pacientes com **risco de CHC** (carcinoma hepatocelular): cirrose, hepatite B crônica ou outra condição predisponente.

> ⚠️ **Restrição de uso:** LI-RADS **não deve ser aplicado** em fígados sem fator de risco documentado. Uso em fígados normais gera classificações inválidas — o motor não valida o contexto do paciente, apenas extrai a categoria declarada no laudo.


| Categoria | Significado                                                      | Conduta                                                    |
| --------- | ---------------------------------------------------------------- | ---------------------------------------------------------- |
| LR-1      | Definitivamente benigno                                          | Sem seguimento específico                                  |
| LR-2      | Provavelmente benigno                                            | Seguimento de imagem                                       |
| LR-3      | Indeterminado                                                    | Seguimento ou investigação complementar                    |
| LR-4      | Provavelmente CHC                                                | Alta suspeita; considerar biópsia ou tratamento            |
| LR-5      | Definitivamente CHC                                              | Diagnóstico sem biópsia em contexto apropriado; tratamento |
| LR-M      | Provavelmente/definitivamente maligno, **não específico de CHC** | Biópsia necessária para tipagem                            |
| LR-TIV    | Tumor em vaso (trombose tumoral)                                 | Sinal de malignidade avançada; investigação urgente        |


**Regras específicas:**

- **LR-4, LR-5, LR-M e LR-TIV** promovem `fl_relevante = 1`.
- **LR-M e LR-TIV** ficam **fora do ranking numérico** (`aggregation_exclude`) porque não são comparáveis ordinalmente com LR-1 a LR-5 — mas ainda devem promover relevância quando presentes.
- A extração deve tolerar variações OCR: `"lirads:4"`, `"LR 4"`, `"LI RADS 3"` → normalizar para categoria canônica.
- Captura sem prefixo: `"4"` dentro de contexto LI-RADS → resolver para `"LR-4"` (lookup de sufixo).

**Config YAML de referência:**

```yaml
rads_extraction:
  systems:
    li_rads:
      aliases: ["LI-RADS", "LIRADS", "LR"]
      categories: ["LR-1","LR-2","LR-3","LR-4","LR-5","LR-M","LR-TIV"]
      patterns:
        - '(?:LI[- ]?RADS|LIRADS|LR)[\s:.-]*((?:LR[- ]?)?[1-5]|M|TIV)'
      aggregation_exclude: ["LR-M","LR-TIV"]
      relevance_policy:
        promote_categories: ["LR-4","LR-5","LR-M","LR-TIV"]
```

---

## 4. PI-RADS (próstata — ACR)

**Aplicável a:** RM multiparamétrica de próstata (mpMRI).


| Categoria | Probabilidade de câncer clinicamente significativo | Conduta                                     |
| --------- | -------------------------------------------------- | ------------------------------------------- |
| 1         | Muito baixa                                        | Sem indicação de biópsia                    |
| 2         | Baixa                                              | Sem indicação de biópsia                    |
| 3         | Intermediária                                      | Decisão individualizada; considerar biópsia |
| 4         | Alta                                               | Biópsia guiada recomendada                  |
| 5         | Muito alta                                         | Biópsia guiada indicada                     |


**Regras específicas:**

- **Categorias 4 e 5** promovem `fl_relevante = 1`.
- **Categoria 3** é zona cinza — pode promover dependendo da decisão clínica configurada no YAML.
- Apenas 5 categorias (1–5); qualquer valor fora desse intervalo é inválido.

---

## 5. TI-RADS (tireoide — ACR)

**Aplicável a:** ultrassonografia de tireoide.


| Categoria | Nível de suspeita      | Indicação de PAAF       |
| --------- | ---------------------- | ----------------------- |
| TR1       | Benigno                | Não indicada            |
| TR2       | Sem suspeita           | Não indicada            |
| TR3       | Levemente suspeito     | PAAF se nódulo > 2,5 cm |
| TR4       | Moderadamente suspeito | PAAF se nódulo > 1,5 cm |
| TR5       | Altamente suspeito     | PAAF se nódulo > 1,0 cm |


**Regras específicas:**

- **TR4 e TR5** devem promover `fl_relevante = 1`.
- **TR3** pode promover conforme política clínica — configurável via YAML.
- O tamanho do nódulo (limiar para PAAF) não é extraído pelo motor na V1 — é informação complementar.

---

## 6. Regras transversais para configuração do motor


| Regra                                                       | Impacto no motor                                                                                                          |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Negação de categoria (ex.: "descartado", "excluído")        | `negation_window` + `negation_phrases` no YAML; menção marcada `negated: true`; não entra no `max_category`               |
| Categoria inválida (fora da faixa do sistema)               | Emitida em `invalid_candidates`; nunca em `mentions`                                                                      |
| Múltiplas menções no mesmo laudo                            | `max_category` pela ordem de `categories`; conduta segue o mais grave                                                     |
| Categorias fora do ranking ordinal (LR-M, LR-TIV)           | `aggregation_exclude` — emitidas em `mentions`, fora do `max_by_system` numérico, mas verificadas para `relevance_policy` |
| Promoção de `fl_relevante`                                  | Apenas pelas categorias em `promote_categories` no YAML — nunca hardcoded no Python                                       |
| Variações de separador / OCR                                | Normalização pré-match: `_compact()` remove `[\s:.°º_-]+` antes de comparar                                               |
| Numerais romanos (BI-RADS IV = 4)                           | `normalization.roman_to_arabic: true` no YAML do sistema                                                                  |
| Coexistência de sistemas (BI-RADS + LI-RADS no mesmo laudo) | `mentions` lista todos; `max_by_system` indexado por `system_key` — sem interferência entre sistemas                      |


---

## 7. Decisões clínicas que ficam **fora** do motor

- Determinar se o paciente tem fator de risco para CHC (contexto de uso do LI-RADS).
- Calcular o tamanho do nódulo (TI-RADS).
- Indicar biópsia ou tratamento.
- Resolver divergência entre dois sistemas no mesmo laudo (ex.: LI-RADS 3 e achado suspeito não categorizado).
- Validar se a categoria emitida pelo radiologista está clínica e metodologicamente correta.

Essas decisões pertencem ao fluxo clínico / board médico e não devem ser implementadas no motor.