# Template de briefing de negócio — v0

> **O documento de ENTRADA.** Nasce da conversa com o negócio, antes de existir régua, config ou
> qualquer medição. É a transcrição estruturada do que foi pedido — normalmente um Word, um e-mail
> ou uma reunião.
>
> **Nome:** `briefing-negocio-<linha>-v0.md`, na pasta da especialidade.
>
> **Não confundir com a SPEC.** Este documento **congela**: é o registro do que o negócio pediu, e
> não deve ser reescrito conforme a régua evolui. A [SPEC](template-spec-especialidade-v0.md) é que
> é viva, e nasce **daqui mais as medições**. Quando a SPEC v1 for publicada, este briefing passa a
> `status: superado`, mas continua no repositório — é a única prova do que foi pedido originalmente.

**Para o agente:** este é o contexto mínimo para propor uma primeira régua. Se um campo está vazio,
ele é **pergunta ao negócio**, não suposição.

---



# Briefing de negócio — Linha de cuidado NOME

**Status:** rascunho | validado com o negócio | em desenvolvimento | superado pela SPEC vN
**Data:** AAAA-MM-DD · **Origem:** Word / e-mail / reunião — com autor e data
**Anexos:** caminho dos arquivos originais

## 1. O pedido, como veio

> Transcrição **literal**. Sem parafrasear, sem organizar, sem corrigir. A paráfrase da equipe já é
> interpretação, e é exatamente onde a régua começa a divergir do negócio.



## 2. Objetivo da linha

Que paciente se quer encontrar, e **para quê** — captação, navegação, tratamento. O "para quê"
decide casos que a lista de palavras não resolve: paciente já em acompanhamento normalmente não é
captação nova.

## 3. Exames citados


| exame | modalidade | como aparece no laudo |
| ----- | ---------- | --------------------- |


> ⚠️ Nome que o negócio usa raramente é o nome no lake. Confirmar antes de virar filtro (§10.1).



## 4. Palavras-chave citadas


| termo | é achado, órgão ou qualificador? |
| ----- | -------------------------------- |


> Copiar a lista **inteira**, mesmo os termos que parecerem redundantes ou fora de escopo. O que for
> descartado depois vira decisão registrada na SPEC, com custo medido — não some em silêncio.



## 5. Parâmetros e condições citados


| parâmetro | condição | unidade |
| --------- | -------- | ------- |


> Limiares numéricos, faixas etárias, condicionais por tipo de exame, combinações exigidas
> ("A **e** B"). Registrar a **unidade** — comparação de medida falha em silêncio sem ela.



## 6. O que o negócio disse que NÃO quer

> Tão importante quanto a lista de inclusão, e quase sempre esquecido na conversa. Se não foi dito,
> **perguntar**: é a pergunta mais barata do projeto e a que mais evita retrabalho.



## 7. Capacidade de absorção

**Quantos casos por dia/mês a operação consegue absorver?**

> Pergunta obrigatória, e é de operação, não de clínica. Escopo já foi cortado por capacidade neste
> projeto — na tireoide só TR5 e TR4 ≥ 1 cm entraram, e a razão foi essa. Sem esse número, toda
> discussão de escopo vira opinião.



## 8. Quem decide e quem valida


| papel                                   | quem |
| --------------------------------------- | ---- |
| dono de negócio (decide escopo clínico) |      |
| PO (escopo de entrega e priorização)    |      |
| PMO (prazo, agenda, coordenação)        |      |
| quem valida os lotes                    |      |
| apoio clínico consultado                |      |


> Nomear o **dono**. Quando duas vozes clínicas divergirem — e vão —, é isso que resolve.



## 9. Lacunas do pedido


| #   | o que a conversa não respondeu | bloqueia o quê |
| --- | ------------------------------ | -------------- |




## 10. Primeiros passos

Ordem deliberada: **tudo que é barato e pode invalidar o resto vem antes de escrever régua.**

1. **Confirmar os nomes de exame no lake** e transformar em filtro textual.
  ⚠️ A plataforma lê **somente** `filters.gold_filter.keywords`. Filtro ausente ou declarado no
   lugar errado não dá erro — traz a base inteira.
2. **Volumetria bruta** da janela: quantos laudos o filtro seleciona por dia.
3. **Gabarito** — existe? quem anota, com que critério, e sob qual versão do escopo?
  Se não existe, definir **como** será feito **antes** de prometer métrica.
4. **Schema provisionado?** É do time da Fábrica, e o pedido vai antes do primeiro run.
5. **Rascunho da régua** a partir das §4 e §5 — e só agora.
6. **Primeiro run em dev**: volumetria de relevantes por dia, contra a capacidade da §7.
7. **Lote de validação** para quem valida na §8.



## 11. Transição para a SPEC

Este briefing vira `spec-negocio-<linha>-v1.md` quando os passos 1, 2, 3 e 6 estiverem fechados —
ou seja, quando houver **filtro confirmado, volumetria medida, gabarito definido e um primeiro run**.

Antes disso não há SPEC: há briefing e hipótese.


| passo                       | status | data |
| --------------------------- | ------ | ---- |
| filtro de exames confirmado |        |      |
| volumetria bruta medida     |        |      |
| gabarito definido           |        |      |
| primeiro run em dev         |        |      |
| **SPEC v1 publicada**       |        |      |


---



# Anexo — roteiro da reunião de discovery

> Para conduzir **ao vivo**. As seções acima são o formato de registro; isto é a ordem em que a
> conversa funciona. Cada pergunta alimenta uma seção, e traz **o que fazer se a resposta não
> fechar** — que é onde a reunião costuma passar batido.
>
> **Duas regras de higiene:** transcrever **literal** na §1, sem arrumar a frase na hora; e terminar
> lendo as decisões de volta em voz alta, para o negócio confirmar antes de sair da sala.



### 1. Abertura — *"Que paciente vocês querem encontrar, e para quê?"* → §2

O **para quê** decide os casos que a lista de palavras não resolve. Se a resposta for só o nome da
doença, insistir: captação nova? navegação? acompanhamento de quem já está na linha?

**Não fecha se:** não der para dizer o que acontece com o paciente depois que ele aparece na fila.

### 2. *"Quais exames?"* → §3

Pedir o nome como o negócio usa **e** como aparece no laudo. Perguntar se há mais de um tipo de exame
com regras diferentes.

**Não fecha se:** ninguém souber dizer se o exame X e o exame Y seguem a mesma régua.

### 3. *"Que achados fazem o laudo ser relevante?"* → §4

Deixar falar sem interromper, anotar tudo — inclusive o que parecer redundante ou fora de escopo.

**Não fecha se:** vierem só categorias amplas ("lesões suspeitas") sem os termos que o laudo usa.

### 4. *"Tem número, medida ou condição?"* → §5

Limiar, faixa etária, tamanho mínimo, combinação exigida ("A **e** B"). **Sempre perguntar a
unidade** — comparação de medida falha em silêncio sem ela.

**Não fecha se:** houver limiar sem unidade, ou "A e B" sem saber se é conjunção mesmo.

### 5. *"E o que vocês NÃO querem ver na fila?"* → §6

**Pergunta obrigatória, e quase nunca surge sozinha.** É metade da régua. Se a resposta for "nada",
provocar com exemplos comuns e benignos da modalidade.

**Não fecha se:** a resposta for só "o que não for da doença".

### 6. *"Quantos casos por dia vocês conseguem absorver?"* → §7

Pergunta de **operação**, não de clínica — se quem responde é clínico, buscar quem opera a fila.

**Não fecha se:** não sair um número. Sem ele, discussão de escopo vira opinião, e escopo já foi
cortado por capacidade neste projeto.

### 7. *"Quem decide o escopo? Quem valida os lotes?"* → §8

Nomear **uma** pessoa para decidir escopo. Deixar explícito que apoio clínico baliza, mas não decide.

**Não fecha se:** ficarem duas pessoas com a mesma autoridade — é o que produz régua contraditória.

### 8. *"Já existe alguma lista, planilha ou revisão de laudos?"* → §10.3

Se existir, perguntar **quem anotou, quando, e sob qual critério**. Anotação feita antes de uma
decisão não é gabarito para a régua de depois.

**Não fecha se:** não der para datar a anotação nem dizer o critério vigente na época.

### 9. Fechamento — ler de volta

Repetir em voz alta: objetivo, exames, o que entra, o que não entra, capacidade, quem decide.
Anotar em §9 tudo que ficou sem resposta, com quem vai responder e até quando.

**A reunião não termina sem:** um número de capacidade, um nome de quem decide, e a lista do que
**não** entra.