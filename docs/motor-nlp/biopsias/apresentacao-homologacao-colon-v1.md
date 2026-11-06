---
tags: [biopsias, motion-lis, cancer-colon, homologacao, apresentacao]
---

# Lista de Ca de Cólon (biópsias) — homologação

> Apresentação para a navegação · 05/11/2026 · planilha `lista-colon-biopsias-homologacao-v4.xlsx`

## 1. O que precisamos

Para cada linha da planilha: **este paciente deveria ser enviado à navegação?**

As respostas mostram se a regra está certa antes de entrar em produção.

## 2. De onde vêm os dados

### No Motion LIS (laboratório)

| o Motion tem | o que traz |
|---|---|
| **categoria do resultado** | classificação do laboratório — usamos **neoplasia maligna primária** |
| **material** | texto com o que foi enviado ao laboratório (ex.: "pólipo de cólon sigmoide") |
| **laudo do AP** | macroscopia, microscopia e conclusão |
| **nº do pedido** | identificador do exame |

O Motion **não tem** nome, CPF, unidade, nem campo de órgão.

### No Repositório Clínico (RC)

Cruzando pelo **nº do pedido**, o RC completa: **nome, CPF, idade, unidade**.

### O caminho até a lista (AP de 05/10 a 03/11)

| passo | o que fica | pedidos |
|---|---|---|
| todos os AP do Motion | — | 43.158 |
| categoria = neoplasia maligna primária | câncer primário | 2.017 |
| material cita cólon ou reto | órgão | 155 |
| encontrado no RC | paciente e unidade | 144 |
| unidade navegada | escopo da navegação | **118** |

Cerca de **2 pacientes novos por dia útil**.

## 3. As colunas da planilha

### Para identificar o paciente e o exame

| coluna | origem |
|---|---|
| B · nº do pedido (idPredicao) | Motion — **use para abrir o caso no Motion** |
| D Nome · E CPF · F Idade | RC |
| I Hospital · J UF | RC |
| P Data do exame · Y Data de emissão do AP | RC · Motion |
| S Tipo do exame | RC |

### Para avaliar o caso

| coluna | o que traz |
|---|---|
| T **Laudo** | o laudo do AP completo |
| U **Achados do Modelo** | a categoria do AP (neoplasia maligna primária) |
| V **Evidência** | a conclusão do AP |
| X **Material do AP** | texto original do laboratório: de onde veio a amostra |

### Sugestões da equipe de dados — **conferir**

| coluna | o que é | por quê |
|---|---|---|
| W **Recorte** | cólon ou reto + modelo de laudo (biópsia / peça) | o Motion não tem órgão; lemos no material |
| AA **Provável peça cirúrgica** | Sim / Não / Indefinido | paciente já operado não segue na navegação |

⚠️ O modelo de laudo **"peça" não quer dizer cirurgia**: dos 43 de cólon nesse modelo, **18 são pólipos** retirados na
colonoscopia. Por isso a sugestão lê o texto do material (colectomia, linfonodos → Sim; pólipo, biópsia → Não).

## 4. Como preencher

Só as **duas colunas amarelas**:

| coluna | preencher |
|---|---|
| AB **VEREDITO** | *Relevante - enviar* · *Não relevante - não enviar* · *Dúvida* |
| AC **MOTIVO** | obrigatório em *Não relevante* ou *Dúvida* — peça cirúrgica, já em navegação, não é cólon, não é primário, laudo incompleto, dados incorretos, outro |

**Por onde começar:** filtrar AA = *Sim* e *Indefinido* — é onde a sugestão mais precisa de confirmação.

## 5. A aba dos 11 casos

AP de cólon/reto que existem no Motion mas **não estão no RC** — sem nome, CPF nem unidade.
Abrir pelo **nº do pedido** no Motion e preencher **unidade** e **unidade navegada?**.

## 6. Para decidir hoje

1. **Reto entra** na linha de cólon? (37 das 118)
2. **Paciente já em navegação** com novo AP maligno: reenviar, ignorar ou anotar?
3. **Prazo** de devolução da planilha — o desenvolvimento começa em **09/11**.

## 7. O que fazemos com a resposta

- quantos casos a regra acertou;
- onde a sugestão de peça cirúrgica erra;
- se faltou alguém — cruzando com as planilhas de controle da navegação **por CPF**.
