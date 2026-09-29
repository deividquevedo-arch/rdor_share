# DII — parecer sobre a avaliação 01

> 15/09/2026. Resposta ao pedido de **ok para abrir o PR → `hml`**, sobre a branch
> `doenca_inflamatoria_intestinal/feature/migracao-config-motor`, commit `3e6362e`, config
> `0.2.1-doenca_inflamatoria_intestinal`.

---

# 1. Veredito

**Ok para abrir o PR**, com **quatro ajustes de config**. Três são de legibilidade e higiene e não
alteram decisão. O quarto — **ligar a camada semântica e o juiz** — altera decisão, é o que falta
para a linha sair do estágio de desenvolvimento, e não se resolve com edição: exige remedir na mesma
janela já medida.

**A sequência pedida em 14/09 foi executada inteira**, e o resultado inverteu a dúvida que a
motivava: o caminho de config pura resolveu, e a feature na lib deixou de ser necessária.

| o que foi pedido | o que veio |
|---|---|
| depurar o denominador — tirar os FP do legado | ✅ **4 dos 5 não marcados são fuzzy do próprio legado** |
| classificar os 5 laudos | ✅ resolvidos pela régua nova; sobra 1, variante de escrita |
| **M2d — `document_vet`** | ✅ **funcionou** — `soft_findings: ['recomendacao_colonoscopia']` + 13 frases |
| os 12 regex da auditoria, medidos um a um | ✅ 7 entraram |
| config único, `full_doc`, janela 6 | ✅ os três |
| lib pinada, não `latest` | ✅ `0.12.3` |

---

# 2. O que sustenta o parecer

**A régua melhorou em tudo, não só em recall:** F1 **0,754 → 0,958**, MCC **0,771 → 0,958**, sobre
18.480 laudos em 3 dias.

✅ **O `gold_filter` foi medido nos dois sentidos, com custo** — `+218 exames, todos IMG, 0
laboratório`, e `entero` solto **descartado por trazer +3.600 culturas**. É exatamente o que a régua
de filtro de entrada pede, e é raro vir assim.

✅ **A plataforma foi separada da bancada:** 17.494 laudos em comum, **3 decisões diferentes
(0,02%)**, todas atribuídas à diferença de texto entre a Gold e o `laudo_tratado`. Isso responde a
pergunta que a validação local nunca responde.

✅ **Variantes de critério medidas** — "indicação não conta" e "+enterocolite não conta" dão F1
0,910 e 0,885, e **a ordem não muda em nenhuma**. É teste de robustez da conclusão, não do número.

---

# 3. Os quatro ajustes

## 3.1 🔴 O `document_vet` precisa de explicação no cabeçalho

```python
'document_vet': {'enabled': True,
                 'soft_findings': ['recomendacao_colonoscopia'],
                 'normality_phrases': [... 13 frases ...]}
```

**Funciona, e é a solução certa.** Mas é **uso não óbvio da chave**: `document_vet` e
`normality_phrases` nomeiam *enunciado de normalidade*, e aqui carregam **boilerplate de laudo de
colonoscopia**. Quem abrir a config daqui a três meses lê "frases de normalidade" e não entende.

⚠️ **O cabeçalho tem 96 linhas e não menciona `document_vet` nenhuma vez.**

**Ajuste:** um parágrafo no cabeçalho dizendo o que o bloco está fazendo — que o termo
`colonoscopia` é achado legítimo em laudo de imagem e ruído em laudo de colonoscopia, e que o
`document_vet` é o mecanismo que separa os dois pelo conteúdo do próprio laudo.

ℹ️ **É condição que acompanhava a proposta desde o início** — o M2d foi proposto com a ressalva de
que, se vingasse, subiria com cabeçalho explicando, ou a generalização da chave viraria item da lib.

## 3.2 🟡 Remover o bloco `runtime`

Presente com `profile` e `llm_router`. **Decisão de 15/09:** o bloco sai de todas as configs — ele
existia para sobreposição via widget, e nenhum dos 15 widgets do runner o alimenta.

Enquanto os dois lados declaram `False`, remover tem **delta zero**.

🔴 **Mas deixa de ser inócuo junto com o §3.4.** O `runtime` **sobrepõe** o `nlp` no carregamento
(`ntb_ia_loader.py:104-113`, `self.llm_router.update(runtime_llm)`), e é o resultado que o motor lê.
Ligar `nlp.llm_router.enabled: True` e **deixar o `runtime` com `False`** mantém o juiz desligado —
sem erro, sem log, com o run passando normalmente. **Os dois ajustes vão no mesmo commit.**

## 3.3 🔴 Ligar a camada semântica — não remover o bloco

`use_embeddings: False` com `decision_mode: 'hybrid'`: hoje o bloco é inerte e `hybrid` faz quem lê
concluir que a semântica participa. Pela régua de 03/09 isso não passa — mas **a saída é ativar, não
remover**.

**A régua de entrega é dura neste ponto:** o alvo de toda especialidade é o fluxo completo — regra →
híbrido calibrado → juiz. `rule_only` é estágio de **desenvolvimento**, não configuração de entrega,
e nenhuma lista vai ao negócio a partir de perfil parcial.

São **três chaves, em dois arquivos**, e as três precisam andar juntas:

| onde | chave | de | para |
|---|---|---|---|
| config | `nlp.embeddings.use_embeddings` | `False` | `True` |
| config | `nlp.llm_router.enabled` | `False` | `True` |
| `jobs/definicoes/<linha>-batch.json` | `embedding_enable` | — | `"true"` |

🔴 **O widget é o passo que passa despercebido.** Ele decide se a wheel instala o extra
`[embeddings]`. Sem ele, `use_embeddings: True` **não instala a biblioteca** e a semântica cai em
`token_overlap` sem erro e sem log — o perfil medido não seria híbrido. Medido em dev, TI-RADS, 30
dias: **4.543 de 17.896 laudos (25,4%) com `FALLBACK:ModuleNotFoundError`**.

🔴 **E a branch do DII não traz definição de job.** São só 4 arquivos — 3 navegações e a config.
Sem `jobs/definicoes/` e `jobs/clusters/`, não há onde declarar o widget e a linha não roda pelo
runner. É o que a ateromatose levou no mesmo PR da config.

✅ **O `embedding_model` já aponta para o lugar certo** — `gold_fabrica_ia_hml`, o Volume do
workspace novo, igual ao `cancer_rim`. Em prd o caminho não existe ainda (card `305810`, com a
plataforma), e isso não bloqueia o PR: valida-se em dev e acompanha-se a correção.

⚠️ **`ambiguity_band: [0.3, 0.7]` é inerte aqui.** A chave só é lida em `decision_mode: 'fallback'`
(`decision_pipeline.py:558`). Em `hybrid` o que decide é `similarity_threshold`, e a promoção é
direta: `fl == 0` com `semantic_score >= threshold` **vira `fl = 1`**.

🔴 **`similarity_threshold: 0.80` é frouxo para ligar assim.** O `cancer_rim` usa **0.92**,
declarado como conservador justamente para que nenhum candidato seja promovido sem validação humana.
A 0.80 a semântica entra promovendo — e o PR já carrega 17 falsos positivos contra 4 do legado.
**Medir antes de fixar**, com o `semantic_score` na telemetria.

## 3.4 🔴 O bloco do juiz está incompleto para ser ligado

```python
'llm_router': {'enabled': False, 'mode': 'llm', 'provider': 'openai_compatible',
               'api_key_env': 'DATABRICKS_TOKEN', 'fallback_policy': 'keep_current',
               'json_response_format': False}
```

Faltam `model`, `uncertainty_band`, `max_input_chars`, `prompt_system` e `specialty_context`.
Trocar `enabled` para `True` com o bloco assim **roda o juiz sem prompt de especialidade e na banda
default `(0.35, 0.65)`** — que é o que a lib assume quando a chave falta
(`llm_router_backend.py:346`).

⚠️ **A banda default provavelmente não serve.** No `cancer_rim`, com embeddings ligados, a banda
teve de ir para `[0.75, 0.95]`: o modo `hybrid` **recalcula o score** (peso default `0.7` régua /
`0.3` semântica) e desloca a escala, então a banda precisa cobrir a faixa em que os positivos caem.
Ligar o juiz na banda errada dá `llm_called = 0` — habilitado e nunca acionado.

**Referência de formato:** `ntb_ia_cancer_rim_config.py` na `hml`, que traz os dois blocos
completos, com o valor de cada número comentado ao lado. **Formato, não conteúdo** — o prompt de lá
é a régua de oncologia renal.

🔴 **E o prompt não se escreve antes dos deltas.** O juiz existe para resolver a ambiguidade que
sobra e vetar o que seria errado sinalizar — e o que é ambíguo nesta linha está nos dados, não na
suposição. Duas invariantes: **o juiz só filtra** (a lib não o deixa promover sem evidência de
regra, então inclusão de escopo vira achado, nunca instrução) e **o prompt descreve o resíduo**, não
a régua em prosa.

**A maior parte dos 17 FP não é matéria do juiz:** 8 são negação a distância (régua — §4.1), 3 são
anatomia fora do trato (organ gate / `exclude`), 2 são recomendação de RM (`soft_findings` +
`document_vet`, mecanismo já em uso). Sobram os **4 de referência ao passado** — temporalidade é o
que a régua expressa pior — mais o que a camada semântica promover, que é população **inexistente**
em `rule_only`.

**Ordem:** corrigir a régua → rodar híbrido sem juiz → isolar as promoções pelo delta contra a
corrida `rule_only` (⚠️ `decision_source` sai como `hybrid` em todos os laudos que passam pela
camada, não isola) → ler os casos e escrever o prompt com as condições que eles mostram → ligar o
juiz e medir o que ele remove. O procedimento detalhado está em `dii/orientacao-dii-leandro.md`
§1.5.

ℹ️ **Precedente:** no ca-estômago a régua sozinha dava precisão 0,267, e o que levou a 0,929 foi
alinhar o prompt à regra de negócio — seis versões medidas, três revertidas.

---

# 4. A verificar antes do merge

## 4.1 ⚠️ `negation.direction_default: None`

A chave está **presente com valor `None`**. O carregador faz:

```python
if "direction_default" in negation:
    direction["_default"] = negation["direction_default"]
```

Como a chave existe, o `_default` é setado **para `None`** — não cai no default da biblioteca, que
desde a `0.11.1` é `left`.

🔴 **E há um sintoma que pode ser isto:** dos 17 falsos positivos, **8 são "negações a distância"**.
Se a direção default não está valendo, o negador pode estar sendo procurado no lado errado.

**Verificar** se `None` é deliberado. Se não for, remover a chave ou declarar `'left'` — e remedir
os 8.

## 4.2 🟡 O gabarito de 206 é derivado da própria comparação

As métricas usam como referência as **101 divergências auditadas**, com o critério *"régua do legado
aplicada corretamente"*. É um gabarito construído a partir do confronto entre as duas réguas, não
independente delas.

⚠️ **Não invalida o resultado** — o critério está declarado, e as variantes de sensibilidade foram
medidas. Mas o F1 de 0,958 é **contra esse gabarito**, e isso precisa estar dito onde o número
aparece, para ninguém o ler como acerto clínico.

ℹ️ O encaminhamento dos **96 laudos que o atual marca e o legado não** para revisão do negócio é o
caminho certo — é ali que o ganho de recall se confirma ou não.

## 4.3 🟡 17 FP contra 4 do legado

O F1 sobe porque o recall salta (127 → 205), mas o atual marca errado **4× mais**. Em rastreio, FP
tem custo operacional.

A classificação está feita: 8 negações a distância · 3 anatomias fora do trato · 4 referências ao
passado · 2 recomendação de RM. **A janela 6 é a alavanca mais direta** para os 8 — vale medir
janela 7 ou 8 e ver o custo em recall antes de aceitar os 17 como definitivos.

---

# 5. 🔴 Um achado transversal: o mesmo grant bloqueia três frentes

> *"falha do run: só na view de exportação — usuário sem `USE CATALOG security` (grant)"*

**Este mesmo grant aparece em dois PRs abertos hoje.** O PR 7275 (ateromatose) reporta que *"as
cinco colunas de identificação saíram CIFRADAS no envio: a descriptografia exige `USE CATALOG` no
catálogo `security`"*.

⚠️ **E pode ser a causa de um terceiro caso, medido hoje:** a view de exportação do TI-RADS entrega
`nome_paciente` e `medico_solicitante` em **base64 em 89 de 89 linhas**, em prd e em hml — enquanto
a da reumatologia entrega em claro.

**Não são três pendências — é uma.** Vale tratar como item único junto ao time de plataforma, em vez
de cada frente pedir o seu.

---

# 6. Sobre o pedido

✅ **Ok para abrir o PR**, com os quatro ajustes da §3 e a verificação da §4.1.

⚠️ **Dois deles mudam número e exigem nova medição, não edição:** ligar a semântica e o juiz (§3.3 e
§3.4) e o `direction_default` (§4.1). A remedição vai na **mesma janela dos 18.480 laudos**, e ao
negócio vai só o conjunto de **discordâncias** contra a corrida `rule_only` — o que a semântica
acrescenta e o que o juiz remove. Re-homologa-se o delta, não o todo.

As pendências que a avaliação lista (grants, `id_linha_navegacao`, revisão dos 96 pelo negócio,
eixo de data) estão corretamente atribuídas e nenhuma bloqueia a abertura.

ℹ️ **O eixo de data** — plataforma por `dt_dia_exame`, legado por `dt_liberacao_laudo` — é nota de
plataforma que vale para todas as linhas e **ainda não tem card**. É irmão do fuso UTC da janela de
datas, já registrado como bug conhecido pelo time.
