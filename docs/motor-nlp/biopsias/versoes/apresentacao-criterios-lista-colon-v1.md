---
tags: [biopsias, motion-lis, cancer-colon, apresentacao]
---

# Lista de Ca de Cólon (biópsias) — critérios

> Apresentação para a navegação · 05/11/2026 · regra da lista `lista-colon-biopsias-homologacao-v4.xlsx`.
> Consulta: `consulta-lista-colon-biopsias-v2.sql`.

## Entra na lista quando

| # | critério | como é verificado |
|---|---|---|
| 1 | é **exame anatomopatológico** (AP) no Motion | procedimento do pedido |
| 2 | o resultado é **neoplasia maligna primária** | categoria do laboratório (`codificação da doença`) |
| 3 | a amostra é de **cólon ou reto** | texto do **material**: cólon, sigmoide, ceco, ascendente, transverso, descendente, flexuras, reto |
| 4 | o pedido **existe no Repositório Clínico** | cruzamento pelo nº do pedido (traz nome, CPF, unidade) |
| 5 | a unidade é **navegada** | mesma lista de unidades das demais linhas de captação |
| 6 | o AP foi **emitido no período** | 05/10 a 03/11/2026 |

## Fica de fora

| o quê | por quê |
|---|---|
| outras categorias (benigna, metastática, in situ, suspeita…) | só câncer primário entra |
| **apêndice** | não é cólon |
| AP que não está no RC (11 casos) | sem nome, CPF e unidade — vão em aba separada para conferência |
| unidades não navegadas (26 casos) | fora do escopo da navegação |

## Marcado para decisão (não excluído)

| marcação | regra | por quê |
|---|---|---|
| **Recorte = reto** | material cita reto e não cita cólon | reto entrar na linha de cólon é decisão do negócio |
| **Provável peça cirúrgica = Sim** | material cita colectomia ou outra "-ectomia" (exceto polipectomia e mucosectomia), amputação, ressecção não endoscópica, linfonodos, omento, anastomose | paciente já operado não segue na navegação |
| **Provável peça cirúrgica = Não** | material cita pólipo, polipectomia, mucosectomia, biópsia, colonoscopia, fragmento | paciente a navegar |
| **Provável peça cirúrgica = Indefinido** | nenhum dos dois | a enfermagem decide |

## Em aberto

1. Reto entra?
2. Paciente já em navegação com novo AP maligno: reenviar, ignorar ou anotar?
3. Um envio por paciente ou por pedido?
