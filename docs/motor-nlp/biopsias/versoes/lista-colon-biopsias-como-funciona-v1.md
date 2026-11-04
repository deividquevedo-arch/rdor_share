---
tags: [biopsias, motion-lis, cancer-colon, homologacao]
---

# Lista de Ca de Cólon (biópsias) — como funciona

> v1 · 04/11/2026 · explicação da lista de pacientes relevantes de Ca de Cólon entregue para homologação
> (`lista-colon-biopsias-homologacao-v2.xlsx`, fora do git por conter dado de paciente). Regra e escopo em
> [[biopsias-definicao-envio-v2]]; consulta em `consulta-lista-colon-biopsias-v2.sql`.
> Card `393825` — *[Tabia][Biopsias][Estudo] Estudo e mapeamento dos dados do Motion LIS disponíveis no Repositório
> Clínico*.

## 1. A ideia em uma frase

Encontrar, entre os exames de laboratório, os pacientes com **câncer de cólon confirmado** na análise da amostra, e
entregá-los à navegação **no mesmo formato** dos envios de laudo.

## 2. Biópsia × anatomopatológico

| | o que é | exemplo |
|---|---|---|
| **biópsia** | o **procedimento**: retirada de um fragmento de tecido | pólipo retirado na colonoscopia |
| **anatomopatológico (AP)** | o **exame de laboratório** que analisa esse fragmento | "o pólipo é câncer?" |

O resultado está no **AP**. A lista parte dele.

## 3. De onde vêm os dados

```
 MOTION LIS (laboratório)                 GOLD (cadastro dos exames)
 +---------------------------+           +---------------------------+
 | nº do pedido              |---------->| nº do pedido              |
 | categoria do resultado    |  mesmo nº | nome, CPF, telefone       |
 | material (de onde veio)   |           | unidade, convênio         |
 | laudo do AP               |           | médico solicitante        |
 +---------------------------+           +---------------------------+
   diz SE é câncer e DE ONDE              diz QUEM é o paciente e ONDE
```

O Motion não tem unidade nem dados do paciente; eles vêm da gold, pelo número do pedido.

## 4. O funil (AP emitidos de 05/10 a 03/11/2026)

| passo | pergunta | pedidos |
|---|---|---|
| — | AP emitidos no laboratório | 43.158 |
| ① | o resultado está classificado? | 39.670 |
| ② | a classificação é **neoplasia maligna primária**? | 2.017 |
| ③ | o material é de **cólon ou reto**? (apêndice excluído) | 155 |
| ④ | o pedido existe na gold? (paciente e unidade) | 144 |
| ⑤ | a unidade é **navegada**? | **118** |

- **② câncer:** o laboratório classifica o resultado num campo fixo (`codificação da doença`). Não há inteligência
  artificial nem leitura de texto nesta etapa.
- **③ cólon:** o campo **material** diz o que foi enviado ao laboratório (ex.: "pólipo de cólon sigmoide", "produto de
  colectomia"). A busca é por cólon, sigmoide, ceco, segmentos, flexuras e reto.
- **⑤ unidades:** as mesmas 90 unidades que recebem as demais linhas de captação.

## 5. As duas colunas de decisão

As linhas fora do escopo estrito vêm **marcadas, não descartadas**, para a homologação decidir.

**Recorte** — de onde veio a amostra:

| | linhas |
|---|---|
| cólon | 81 |
| reto | 37 |

**Provável peça cirúrgica** — paciente já operado não segue na navegação:

| valor | significado | linhas |
|---|---|---|
| **Não** | pólipo, biópsia, polipectomia, colonoscopia — **paciente a navegar** | 66 |
| **Sim** | colectomia, ressecção cirúrgica, linfonodos, omento, anastomose — **já operado** | 46 |
| **Indefinido** | o texto não permite concluir; avaliação da enfermagem | 6 |

**Por que a coluna é pelo texto do material:** o laboratório tem um modelo de laudo chamado "peça", mas ele não é
sinônimo de cirurgia. Dos 43 laudos de cólon nesse modelo, **18 são pólipos retirados na colonoscopia** — justamente
os casos de interesse. Excluir pelo nome do modelo perderia esses pacientes.

## 6. Resultado

| | linhas |
|---|---|
| lista completa (para decisão) | 118 |
| **escopo estrito** — cólon e provável peça cirúrgica ≠ Sim | **~56** |

Cerca de **2 pacientes novos por dia útil**.

## 7. A planilha

`lista-colon-biopsias-homologacao-v2.xlsx`, duas abas:

- **Lista** — as 22 colunas dos envios de laudo, mais quatro de apoio (recorte, material do AP, data de emissão do
  AP, ID do hospital). Onde seria "achado do modelo" vai a **categoria do AP**; onde seria o laudo do exame vai o
  **laudo do AP**; "Evidência do Achado" traz a conclusão do AP.
- **Leia-me** — as regras e as contagens.

**Para ver só o escopo estrito:** filtrar *Recorte* = cólon e *Provável peça cirúrgica* ≠ Sim.

## 8. Decisões da homologação

1. **Reto entra?**
2. **Os 6 indefinidos:** confirmar caso a caso.
3. **Paciente já em navegação** que recebe novo AP maligno: reenviar, ignorar ou anotar?
