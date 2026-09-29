# Nota técnica — envio de texto clínico ao LLM

**Data:** 2026-08-23 · **Para:** MLOps (Diego, Gabriel, João), gestão (Fabio, Monique, Natan) e, na
sequência, compliance/DPO.
**Origem:** item bloqueante da revisão do PR 7102 · card `283646`.

> Esta nota **não decide** se o envio é permitido — isso é do DPO. Ela entrega o que só nós
> conseguimos entregar: **o que sai, quanto, e o que dá para reduzir.**

---

## 1. Resumo

O motor NLP envia excerto de laudo a um modelo de linguagem para resolver casos ambíguos.
Medimos a exposição real em produção e ela é menor do que a discussão sugeria — **1,1% dos laudos** —,
mas **enviamos mais texto do que o necessário**, e isso é corrigível por nós.

**Quatro medidas propostas.** Duas têm risco zero e vão nesta semana. As outras duas reduzem o
volume enviado e exigem execução comparativa antes de serem prometidas.

---

## 2. O que é enviado — medido em produção

**Hepatologia, catálogo de produção, 21/08:**

| | |
|---|---|
| corpus do dia | 107.043 laudos |
| **enviados ao LLM** | **1.173 — 1,1%** |
| identificador de paciente no payload | **nenhum** |
| CPF | **0** |
| campo `Nome:` · data de nascimento | 16 · 16 (1% cada) |
| **nome de médico** | **1.024 — 87%** |
| tamanho médio do excerto | 1.969 caracteres |
| modelo | `databricks-claude-haiku-4-5`, servido pelo próprio Databricks |

**Câncer de estômago, dev, mesmo perfil:** 342 de 10.783 (3,2%), sem identificador, sem CPF, 61% com
nome de médico.

> **Correção — 2026-08-26.** Esta nota afirmava que o `transplante_pulmao` não rodava em produção.
> **Está errado.** Verificado hoje: as **três** especialidades produzem saída em prd, com engine
> `0.9.4`. A superfície de envio é de **três linhas, não duas**.

Em produção hoje: **hepatologia** (juiz ligado), **tirads** (juiz desligado) e
**transplante_pulmao** (extrator quantitativo — ⚠️ chama o LLM mesmo com o juiz desligado).

### Como o payload é montado

`llm_router_backend._build_messages` monta a mensagem com três partes: o **excerto do laudo**, os
**critérios clínicos** da especialidade e o **prompt de sistema**. Nada mais — nenhum campo da base
acompanha. Truncado em `max_input_chars` (8.000).

### O gate que limita o volume

Só 1,1% chega ao LLM porque o motor tem duas travas: a **banda de incerteza** (só casos ambíguos) e,
na camada quantitativa, o **`anchor.text`** — sem o termo âncora no laudo, o modelo não é chamado.

⚠️ **Desligar o juiz não interrompe o envio.** O extrator quantitativo não depende de
`llm_router.enabled` — está documentado na docstring de `step_measure`.

---

## 3. De quem é cada parte

| | responsável |
|---|---|
| decidir **se** o laudo vai ao LLM | **lib** |
| decidir **o que** vai no payload | **lib** |
| **reduzir** o que vai | **lib** |
| **registrar** o que foi enviado | **lib** |
| **endpoint, credencial e modelo** | plataforma |
| **contrato com o fornecedor** (retenção de prompt, tenant) | plataforma |
| registro do tratamento | plataforma / privacidade |
| **autorizar o uso** | **DPO** |

Resumindo: **nós não conseguimos responder se pode; vocês não conseguem responder o que sai.**
A validação precisa das duas metades.

---

## 4. O que propomos — e o que já estamos fazendo

### Medida 1 · Avisar quando o juiz está desligado — **risco zero, feito nesta semana**

Hoje a lib assume `enabled: False` na ausência da chave e **não emite nada**. O comportamento é o
correto (falha fechada), mas o silêncio é o problema: foi por isso que a plataforma precisou de um
contorno para ligar o juiz. Passa a registrar.

### Medida 2 · Registrar o que foi enviado — **risco zero, feito nesta semana**

Acrescentar ao blob de saída `llm_input_chars` e `llm_input_scope`. **Sem armazenar o texto.**
Cria trilha auditável do que saiu — hoje só existe `llm_called` e o nome do modelo.

### Medida 3 · Remover identificação profissional antes do envio — **endereça os 87%**

Reusar o `text_pipeline`, que já remove boilerplate, para retirar as linhas de cabeçalho com nome de
profissional. Não são achados clínicos.
⚠️ **Muda o prompt** — exige execução comparativa antes de ser prometida como sem impacto.

### Medida 4 · Enviar só a janela da evidência — **a redução real**

O motor **já sabe onde está a evidência** quando chama o juiz: a etapa de regra roda antes e o
estado carrega os trechos casados. Mesmo assim, envia o documento inteiro.

Proposta: `llm_router.input_scope: 'evidence'` (default `full`, compatível), enviando as frases que
motivaram o achado mais uma janela de contexto.

**Isto depende do card `283648`** (impedir que o juiz decida sem evidência de regra): com a cascata
fechada, todo laudo que chega ao juiz tem evidência localizada **por construção** — sem ela, não há
o que recortar. **A correção de arquitetura e a de privacidade são a mesma correção.**

---

## 5. O que precisamos do time

1. O contrato com a Databricks cobre **dado de prompt e retenção**?
2. O endpoint está **no tenant**?
3. **Nome de profissional de saúde** está no escopo da preocupação, ou apenas dado de paciente?
4. Isso foi validado quando a **hepatologia** subiu para produção?

---

## 6. O que esta nota não afirma

**Não afirmamos que o dado não sai do perímetro.** O modelo ser servido pelo Databricks sugere isso;
quem confirma é o contrato.

**Não afirmamos que não há dado pessoal.** Laudo clínico é dado sensível de saúde por natureza,
mesmo sem nome, e reidentificação por cruzamento é risco que cabe ao DPO avaliar.

**Reconhecemos a minimização.** Enviamos 1.969 caracteres em média quando o critério clínico costuma
caber em uma ou duas frases. É o ponto mais frágil da nossa posição, e é o que as medidas 3 e 4
corrigem.

---

## 7. Achado colateral

A mesma medição encontrou **73 laudos entregues em produção com a coluna de achado vazia** na
hepatologia — promovidos pelo juiz sem nenhuma evidência de regra. É o card `283648`, que deixa de
ser risco teórico. Corrigido no ca-estômago; pendente nas demais.
