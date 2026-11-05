---
tags: [biopsias, motion-lis, volumetria]
---

# Biópsias (Motion LIS) — estômago, próstata, tireoide e rim

> v1 · 05/11/2026 · confere se os dados das próximas linhas de biópsia existem no Motion antes de datar o trabalho.
> Mesmo método da [[lista-colon-biopsias-como-funciona-v2|lista de cólon]]: órgão pelo texto do material,
> biópsia × cirurgia pelo texto do material, paciente e unidade pela gold. Média mensal abr–out/2026.

## Resultado

Pacientes/mês com `neoplasia maligna primária`, em unidades navegadas:

| linha | biópsia | peça cirúrgica | situação |
|---|---|---|---|
| Ca de Estômago (endoscopia) | **~52** | ~15 | mesmo formato do cólon |
| Próstata | **~58** | ~175 (prostatectomia) | mesmo formato do cólon |
| Rim | **~4** | ~40 (nefrectomia) | biópsia renal é quase toda de nefrologia; decidir se a peça entra |
| Tireoide | PAAF: **~19 Bethesda V + ~22 Bethesda VI** (pedidos) | ~103 (tireoidectomia) | punção fica no exame citopatológico, sem categoria estruturada |

## Tireoide — punção (PAAF)

- No Motion: procedimento `exame citopatológico`, ~551 punções de tireoide/mês. Sem `codificação da doença` (7 de
  3.859); o resultado é o **Bethesda**, escrito em `conclusão citologia`.
- Leitura por regex da categoria ("categoria X", "Bethesda X", "X de Bethesda"): **99,2%** lidas (32 sem leitura).
  Distribuição (pedidos/mês): I 64 · II 375 · III 53 · IV 14 · **V 19 · VI 22**.
- **Decisão:** regex no SQL no primeiro corte; o `ordinal_extraction` do motor (config pura, como o PI-RADS) só se a
  homologação mostrar erro de legenda, negação ou exame anterior.
- A decidir pelo negócio: relevante = VI, ou V + VI.
- Números da PAAF são de pedidos — ainda sem cruzamento com a gold e as unidades navegadas.

## Limites

- Biópsia × cirurgia usa a regra de texto em homologação no cólon (`provavel_peca_cirurgica`); ~10–15% dos pedidos
  ficam "indefinido" pelo texto em estômago, próstata e rim.
- Estômago: ~14,6 mil pedidos/mês de biópsia gástrica (volume de endoscopia); a categoria filtra para ~56/mês.
