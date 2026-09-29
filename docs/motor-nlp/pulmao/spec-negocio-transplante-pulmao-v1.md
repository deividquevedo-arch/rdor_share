# SPEC de negócio — Linha de cuidado Transplante de Pulmão (V1)

**Fonte:** spec fornecida pelo usuário (2026-07-14) + arquivo "Palavras chave e exames - Transplante de
Pulmao.docx". Branch de trabalho: `feature/transplante_pulmao`.

## Objetivo
Identificar pacientes **elegíveis para Transplante de Pulmão** a partir do **resultado de exames**, para
**captação/tratamento nas linhas de cuidado** (consultas, exames, procedimentos). Rastreio por
**limiares numéricos** em medidas de função pulmonar (não por presença de achado textual).

## Exames (universo)
- Espirometria
- Pletismografia (Prova de Função Pulmonar completa)
- Ecocardiograma
- Cateterismo direito

## Critérios clínicos (universo completo)
| Doença | Exame | Critério |
|---|---|---|
| Pulmonar obstrutiva crônica (DPOC) | Espirometria | **VEF1 < 30%** predito |
| Supurativas | Espirometria | **VEF1 < 40%** predito (adultos) · **< 50%** (crianças / < 18 anos) |
| Intersticiais | Prova de Função Pulmonar completa | **CVF < 70%** **OU** **DLCO < 40%** predito |
| Circulação pulmonar | Ecocardiograma | **PSAP > 35 mmHg** **e** **FAC do VD < 35%**. **Excluir: FEVE < 40%** (contraindicação) |
| Circulação pulmonar | Cateterismo direito | **PMAP > 25 mmHg** **associada a** **RVP > 3 Wood** |

## ★ ESCOPO V1 (o que ESTE algoritmo cobre) ★
Somente os **3 primeiros** critérios (função pulmonar):
1. **DPOC** — Espirometria: **VEF1 < 30%** predito.
2. **Supurativas** — Espirometria: **VEF1 < 40%** (adultos) · **< 50%** (< 18 anos).
3. **Intersticiais** — Prova de Função Pulmonar completa: **CVF < 70%** **OU** **DLCO < 40%** predito.

**FORA do V1 (→ V2):** Ecocardiograma (PSAP/FAC/FEVE) e Cateterismo direito (PMAP/RVP). Já mapeados
acima para quando entrarmos no V2.

## Requisito de OUTPUT (obrigatório)
Cada laudo relevante deve **reportar o valor específico** que disparou a relevância, ex.:
> "distúrbio restritivo grave com **CVF = 43%**".
⇒ o `resultado`/`achados` da homologação precisa conter medida + valor (vem do audit da camada
quantitativa: `value`+`unit`+`evidence`).

## Homologação (mesmo fluxo/formato do TI-RADS)
Arquivo `.xlsx` com colunas: **idExame · data_laudo · descricao_laudo · laudo tratado · resultado ·
achados · Achado Relevante (lista suspensa: Sim; Não) · Observações**. Output REAL do motor,
organizado no formato; conclusão/homologação como no tireoide.

## Mapeamento para o motor (camada `quantitative_criteria`)
Especialidade **quantitativa**: relevância = limiar numérico, `on_met: "promote"` (a medida É a
relevância; não há achado de regra a ancorar). LLM row-level extrai `valor+unidade`; o **código**
aplica o limiar (determinístico). Rascunho de critérios V1:
- `dpoc_vef1` — measure VEF1 (%), `VEF1 < 30` → promote.
- `supurativa_vef1_adulto` — VEF1 < 40 (adulto). *(threshold pediátrico < 50 depende da IDADE — ver gap.)*
- `intersticial_pft` — `any_of`: CVF < 70 **ou** DLCO < 40 → promote.

## GAPS / questões abertas
- **G1 — Idade p/ limiar pediátrico (< 18 → VEF1 < 50%).** A regra supurativa muda com a idade. Temos
  `id_paciente`/idade na entrada? Se não, V1 pode adotar o limiar adulto (< 40%) e anotar a ressalva.
- **G2 — "% do predito".** Confirmar como o laudo expressa (ex.: "VEF1 43% do previsto", "VEF1/CVF",
  valores absolutos em L vs %). O extrator deve pegar o **% do predito**, não o valor absoluto.
- **G3 — Prompt de extração é hardcoded p/ tireoide** (`build_extraction_messages`: "medida da lesão
  nódulo/cisto, ignore lobo/istmo"). Precisa virar configurável (default = texto atual → byte-compat
  tireoide) para não confundir a extração de VEF1/CVF/DLCO. **Única mudança de lib necessária p/ V1.**
- **G4 — Condicionar critério por tipo de exame** (`applies_to_exam_type`): VEF1 só de Espirometria,
  CVF/DLCO só de Prova de Função completa. O campo existe na camada; usar se `tipoexame` vier confiável.
- **G5 — Material anterior de pulmão** (piloto/standard samples/configs `pulmao.yaml`/legado): revisar e
  reconciliar com esta spec antes de construir (pode haver base de ground-truth reaproveitável).
  RESOLVIDO: material anterior é rastreio de NÓDULO em TC de tórax (ortogonal) → construir do zero.
- **G6 — Laudos HTML/wate+base64** (~58%): RESOLVIDO pelo `to_plain` genérico da lib (limpa HTML/base64).
  Nuance config-in: valores vêm entre colchetes `[76]% previsto` nos wate → tratar no `extraction_hint`.
- **G7 — Termos por extenso / sinônimos / variantes:** a medida pode vir por SIGLA e/ou NOME COMPLETO
  (às vezes só o nome): VEF1 = "Volume Expiratório Forçado no 1º segundo" = FEV1; CVF = "Capacidade
  Vital Forçada"; DLCO = "Difusão do Monóxido de Carbono" = DCO. O `anchor.text` (gate) e o
  `extraction_hint` devem enumerar os sinônimos/variantes. Config-in, sem mudança de lib.
- **Wiring/escopo:** relevância do pulmão = SÓ `quantitative_criteria` (promote), sem embeddings/llm-router.
  O motor já roda a camada quantitativa independente de perfil; só falta o `base_url` do LLM chegar ao
  extrator — resolver com ajuste mínimo no RUNNER (injetar base_url quando há `quantitative_criteria`),
  sem tocar a `fabrica-ia-lib`. Pulmão roda `rule_only` + quant. Depende do wheel `nlp_engine 0.4.0`.
