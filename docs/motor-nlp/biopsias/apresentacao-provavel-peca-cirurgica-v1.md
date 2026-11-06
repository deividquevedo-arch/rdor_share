---
tags: [biopsias, motion-lis, cancer-colon, apresentacao]
---

# "Provável peça cirúrgica" — o que é e como foi calculada

> Apresentação para a navegação · 05/11/2026 · coluna AA da planilha `lista-colon-biopsias-homologacao-v4.xlsx`.

## 1. Por que existe

Paciente cuja amostra veio de **cirurgia** (ex.: colectomia) já foi tratado e **não segue na navegação**.
A lista precisa separar isso — e o **Motion não tem campo** que diga se a amostra é cirúrgica.

## 2. Por que não usar o modelo de laudo "peça"

O Motion tem dois modelos de laudo: **biópsia** e **peça**. Parecia que "peça" = cirurgia. **Não é.**

| AP de cólon com neoplasia primária (abr–out/26), modelo "peça" | |
|---|---|
| citam cirurgia (colectomia, linfonodos, anastomose…) | 136 |
| citam **pólipo** ou procedimento endoscópico | **110** |
| total no modelo "peça" | 376 |

Excluir pelo modelo perderia os **pólipos malignos retirados na colonoscopia** — justamente quem a navegação quer.

## 3. A regra: ler o texto do material

O campo **material** (coluna X) é o texto do laboratório dizendo o que recebeu. A sugestão lê esse texto:

| valor | o material cita | exemplo *(ilustrativo)* |
|---|---|---|
| **Sim** | colectomia ou outra "-ectomia" (exceto polipectomia e mucosectomia), amputação, ressecção não endoscópica, cirurgia, linfonodos, omento, anastomose | "produto de colectomia direita com linfonodos" |
| **Não** | pólipo, polipectomia, mucosectomia, biópsia, endoscopia, colonoscopia, fragmento | "pólipo de cólon sigmoide" |
| **Indefinido** | nenhum dos dois | "lesão de ceco" |

Se o texto cita os dois (ex.: pólipo **e** colectomia), vale **Sim**.

## 4. Na lista (118 pedidos)

| recorte | Sim | Não | Indefinido |
|---|---|---|---|
| cólon · biópsia (38) | 0 | 37 | 1 |
| cólon · peça (43) | 21 | **18** | 4 |
| reto · biópsia (11) | 1 | 9 | 1 |
| reto · peça (25) | 23 | 2 | 0 |
| reto · outro (1) | 1 | 0 | 0 |
| **total** | **46** | **66** | **6** |

## 5. O que pedimos na homologação

- Conferir se a sugestão está certa — principalmente **Sim** e **Indefinido**.
- Se discordar, marcar no **MOTIVO** (ex.: "Peça cirúrgica (já operado)" numa linha marcada como *Não*).
- Com o retorno, medimos o acerto da regra; se acertar, ela vira o **filtro** da linha em produção.

⚠️ É uma **sugestão automática**, não um dado do laboratório. A decisão é da enfermagem.
