# DII — direcionamento depois das medições M1–M8

> Resposta ao `dii-resultado-medicao.md` (10/09). Traz a **decisão clínica que faltava**, três
> variantes a medir, e o que bloqueia a homologação. Escrito em **2026-09-11**.

---

## 1. A decisão clínica saiu — e muda a direção

**Numa TC, a indicação de correlacionar com colonoscopia já é achado.** Confirmado com o negócio
em 11/09.

Consequência direta: **remover o termo `colonoscopia` perde achado legítimo.** Os 25 laudos que a
régua de imagem com `full_doc` marca por causa dele — **11 relevantes no legado hoje** — não são
ruído, são entrega correta. A opção *"remover"* sai da mesa.

E como mantê-lo inunda a colonoscopia (**+771**, zero no legado), o termo passa a ser
**condicional**. É o único ponto que exige condição — todo o resto a medição já fechou.

### 1.1 A conta vira a favor de uma config

| opção | total de relevantes do legado não marcados |
|---|---|
| uma config **com o termo condicionado por tipo de exame** (M8d) | **20** |
| duas configs, ambas `full_doc` | **22** |
| uma config **com regex de recomendação** (M2c) | **25** |
| uma config **sem** o termo (M8c) | **31** |

✅ **Os quatro números são medidos**, na mesma janela de 3 dias, sobre 131 relevantes do legado.

⚠️ **Correção.** A versão anterior desta tabela trazia `0` na coluna de colonoscopia para as
opções de config única, e totais de `29` e `~18`. Estava errado: **numa config única o corpus de
colonoscopia continua sendo processado**, e os 2 relevantes do legado não marcados ali permanecem.
Os totais corretos são `31` e `20`.

### 1.2 E confirma o `full_doc` por um segundo caminho

Sob `auto` o termo pega **9** laudos; sob `full_doc`, **25**. A diferença é a conclusão — onde a
recomendação mora, e que o `auto` descarta. A resposta clínica torna o `full_doc` **requisito**,
não escolha de custo-benefício: sem ele, o achado que o negócio acabou de validar não é visto.

---

## 2. Três variantes a medir — todas no harness que já está montado

### M8d · união com o termo condicionado à imagem 🔴 fecha o número

**Rodar:** união `full_doc`, janela 6, **com** o termo `colonoscopia` ativo **apenas** no corpus de
imagem.

**Contar:** o formato de sempre, nos dois corpora, mais a linha "relevantes do legado não marcados".

**Decide:** troca o `~18` derivado por número medido, e com ele a comparação "uma config × duas"
deixa de ser estimativa.

---

### M2c · o regex de recomendação — pode dispensar a feature

**Rodar:** em vez do termo solto, um padrão de recomendação —
`(correlacionar|sugere|sugerimos|complementar|considerar)[^.]{0,80}colonoscopia` — nos **dois**
corpora.

**Contar:** quantos dos 25 da imagem ele recupera, e quantos dos 771 da colonoscopia ele ainda traz.

**Decide:** se recuperar a maior parte dos 25 e trazer pouco na colonoscopia, **resolve em config,
sem tocar na lib**. Se não discriminar, a condição por tipo de exame é necessária.

⚠️ O risco que você mesmo levantou é o certo: *"sugere-se nova colonoscopia em 1 ano"* é frase
comum em laudo **de** colonoscopia. Por isso a medição tem de rodar nos dois corpora, não só na
imagem.

---

### M3b · remover o `fistula` solto

**Rodar:** M8c sem o termo `fistula` isolado, mantendo os compostos da imagem
(`fistula perianal`, `trajeto fistuloso`).

**Contar:** os 6 falsos positivos da janela 6 saem? algum dos 5 ganhos legítimos sai junto?

**Decide:** custa um minuto e limpa a interação termo × janela que só apareceu no M8c.

---

## 3. 🔴 O que bloqueia a homologação não é o termo

**A régua própria da imagem já deixa de marcar 27 dos 93 relevantes do legado — 29%.** O termo
`colonoscopia`, que era a decisão pedida, vale **11**. O `full_doc` recupera **7**. Sobram **20 sem
causa atribuída**.

E a origem disso é conhecida: a paridade de 05/08 deu **recall 100% medida em UM dia**. É a
armadilha que o próprio plano nomeava — *"um dia só não basta"* — materializada.

**O que se pede:** auditoria dos 27, laudo a laudo, classificando cada um nas quatro causas que
você já nomeou:

| classe | o que é |
|---|---|
| expansão semântica por documento | o legado gera termos por laudo com embeddings; o motor não |
| fronteira de sentença | linha × ponto |
| tokenização | `(dii?)`, `dii/crohn` — a divergência nº 5, já conhecida |
| portão C | "órgão mais próximo" |

**Entregar:** a contagem por classe, e quais são recuperáveis por config contra quais são limitação
da lib. **Isto independe de 1 ou 2 configs** e vem antes da montagem do config final.

---

## 4. Um ponto novo, que não estava no seu escopo

A plataforma aplica um **filtro na view de exportação**, declarado em
`config/exchange/<ambiente>/ntb_ia_<especialidade>_navegacao.py`, bloco `validacao`. Ele decide o
que chega ao negócio **depois** da régua — por unidade, regional ou faixa etária, conforme a linha.

🔴 **Não existe arquivo de navegação para o DII.** E a função que o carrega **falha aberto**:
arquivo ausente → registra um aviso → devolve `{}` → **view sem filtro nenhum**, em silêncio.

Não muda nenhuma medição — elas medem a régua, não a entrega. Mas entra no escopo da migração:
**o DII precisa do arquivo de navegação antes de entregar ao negócio**, com as colunas e o filtro
definidos com quem recebe.

---

## 5. Ordem sugerida

1. **Auditoria dos 27** — é o que destrava a homologação.
2. **M2c** — pode eliminar a necessidade de feature na lib.
3. **M8d** — fecha o número da decisão "uma config × duas".
4. **M3b** — um minuto, limpa os 6 FP.
5. Montar o config, com o resultado de M2c decidindo se é config pura ou config + feature.

✅ **Executada integralmente.** As respostas estão na §7.

---

## 6. O que o relatório fez bem, e vale repetir

- **Pré-condição impressa em toda linha**, com `não-medida` declarado onde `exercitam = 0`.
- **Delta de versão `0.6.6 × 0.12.3` medido** — 0 decisões diferentes. Justificou o harness com
  número em vez de suposição.
- **Contagem de disparo por âncora**, que achou 3 âncoras mortas, uma delas morta também no legado.
- **M8 pegou interação real** — os 7 ganhos do M7 morrem sob `auto`, só 1 sobrevive. Medição
  isolada nunca mostraria isso.
- **Correção do próprio trabalho anterior**, com o número que a derruba.

ℹ️ As contas do relatório foram reconferidas e fecham: `93 − 27 + 25 = 91`, `38 − 2 + 4 = 40`,
`27 − 7 = 20`, e M8b/M8c reconciliam com o custo do termo.


---

# 7. Decisão, depois das medições — 14/09

## 7.1 As duas configs saem da mesa

**Perdem 22 contra 20 da config única condicionada, e custam manutenção dobrada.** São dominadas
nas duas dimensões: entregam menos e custam mais. Não há cenário em que voltem.

## 7.2 A decisão não é (a) × (c) — é **5 laudos**

O que separa a config pura (25) da config com feature na lib (20) são **5 laudos em 3 dias**. Nada
mais. E esses 5 estão enumerados, com o texto: *"A colonoscopia poderá trazer informações"*,
*"visto na colonoscopia"*.

🔴 **A régua clínica que o negócio validou é `indicação de correlacionar`, não `menção a
colonoscopia`.** Lida contra esse critério, a lista de 5 se parte em duas classes de natureza
oposta:

| texto | é indicação? | consequência |
|---|---|---|
| *"A colonoscopia **poderá trazer informações**"* | **sim** — é recomendação, com verbo modal | o regex é que está incompleto |
| *"**visto na** colonoscopia"* | **não** — é referência a exame já feito | não marcar está **correto** |

**Portanto a config pura não está esgotada.** O vocabulário do M2c —
`(correlacionar|sugere|sugerimos|complementar|considerar)` — não cobre a forma modal, e essa é
**recuperável em regex**, sem nenhuma feature. A distância real entre as duas opções é menor que 5,
e pode ser zero.

⚠️ **Ampliar vocabulário tem custo, e ele não foi reportado:** falta quantos laudos o regex do M2c
traz **no corpus de colonoscopia**. É o número que diz até onde o vocabulário pode crescer sem
reproduzir o `+771` do termo solto.

## 7.3 O caminho: config pura agora, feature só se sobrar necessidade

**Config única, `full_doc`, janela 6.** A discriminação do termo sai em config — por
`document_vet` (§7.8) ou, se ele não bastar, por regex de recomendação ampliado.

ℹ️ **Não é meio caminho.** A feature (`applies_to_exam_type`) é aditiva sobre exatamente esta
config, e o delta entre as duas está **medido na mesma janela**. Quando ela existir, o que vai ao
negócio é o conjunto de discordâncias, não a lista inteira — que é o procedimento que a régua de
entrega exige quando a escalada vem depois da homologação.

## 7.4 A feature é menor do que parecia — `applies_to_exam_type` já existe

⚠️ **Correção ao que esta nota afirmava antes.** `applies_to_exam_type` **não é mecanismo novo.**
Verificado no código:

| onde | o quê |
|---|---|
| `quantitative.py:284` | campo de `Criterion`, já carregado do config (`quantitative.py:366-367`) |
| `quantitative.py:1299` | `exam_type_allowed(criterion, exm_tipo, exm_mod)` — **função pública**, em `__all__` |
| `decision_pipeline.py:55-63` | `exm_mod` e `exm_tipo` estão no `_INPUT_PASS_THROUGH` — chegam ao `st.row` |

**O trabalho é ligar um segundo consumidor a um mecanismo montado** — chamar em
`step_extract_findings` a função que já existe. Não é bump de arquitetura; é card pequeno.

**Quem abre:** não o dono da linha. A `nlp-engine-lib` está no quadro *"Dono do NLP Engine"* da
Figura 2 do POP-IA-08. O requisito sobe com a medição que o justifica e entra na Feature `298598` —
*[NLP Engine] Plano de bumps do backlog técnico — 0.11.1 a 0.15.0*.

🔴 **O que continua valendo:** é **chave nova de config**, e chave nova de config o Ops revisa
**antes** de entrar na lib — regra já quebrada uma vez, com o `waive` na `0.12.3`. O aval está
pedido no item 1.3 da pauta mínima, e `applies_to_exam_type` em findings entra **no mesmo pedido**,
para não abrir uma terceira rodada de mudança de contrato.

ℹ️ **Serve mais de uma linha, que é o critério para entrar na lib:** o ca-cólon tem o mesmo split
— duas réguas divergentes, geral × colonoscopia, com `polipo` só na geral. Unificar aquilo numa
config precisa do mesmo mecanismo.

## 7.5 Os 12 regex da auditoria entram já

São a resposta ao bloqueio real. **Os 27 valem 29% dos relevantes do legado; o termo `colonoscopia`
vale 11.** Adiar os 12 para uma versão seguinte é adiar a homologação, não simplificá-la.

**Condição:** medidos **por regex**, não como bloco. Para cada um: quantos dos 27 recupera, quantos
laudos novos traz em imagem, quantos traz em colonoscopia.

🔴 **Regex com zero disparos não sobe.** Config não passa com bloco morto, e a própria medição
anterior já encontrou **3 âncoras mortas, uma delas morta também no legado** — o padrão existe.

## 7.6 Os 6 falsos positivos do legado: confirmado, não copiamos

**A paridade não é o alvo; o critério clínico é.** Precedente direto: na migração da reumatologia,
**as 57 divergências eram todas falso positivo do legado** — negação, cabeçalho metodológico e
achado de outra doença — e a migração foi aceita como *"remove falso positivo, não perde recall"*.

Exige duas coisas, e as duas são do relatório de migração:

1. **Enumerar caso a caso, com a evidência**, agrupado por via — como a reumatologia fez em três.
   Sem isso o `match_rate` parece regressão para quem revisa.
2. 🔴 **Tirar os 6 do denominador antes de calcular a perda.** Se algum deles está dentro dos 20 ou
   dos 25, a perda real é menor **e a distância entre as opções encolhe**. Isto é anterior à
   decisão, não posterior.

⚠️ **Conferir se estes 6 são os mesmos do M3b** (os falsos positivos da janela 6, do termo `fistula`
solto) ou conjunto distinto. Dois "6" diferentes no mesmo relatório são exatamente o que se funde
depois.

## 7.7 Navegação: valida quem é dono da linha

**Não é da plataforma.** O bloco `validacao` carrega **regra clínica** — hepatologia limitada a
`RJ/SP/BA`, transplante de pulmão a 6–75 anos, listas brancas de unidade. É decisão de negócio,
levada por quem é dono da linha, com o Tech Lead no circuito.

- **Formato de referência: `reumatologia`** — partiu do `cancer_rim`, definido pelo negócio, e
  removeu as colunas vazias. É a versão mais enxuta em produção.
- 🔴 **O filtro tem de ser decidido explicitamente, inclusive para dizer "sem filtro".** A função que
  o carrega **falha aberto**: arquivo ausente → aviso → `{}` → **view sem filtro nenhum**, em
  silêncio.
- ℹ️ **6 arquivos caem para 3** — dev, hml e prd — no instante em que a config única vale.


## 7.8 🔴 M2d — o `document_vet` já expressa a régua, e não foi medido

O bloco `document_vet` (`decision_pipeline.py:831-865`, opt-in) rebaixa `1 → 0` quando **todas** as
categorias que dispararam estão em `soft_findings` **e** o texto contém alguma das
`normality_phrases`. As duas listas vêm do config.

Declarando `soft_findings` com a categoria do termo `colonoscopia` e `normality_phrases` com
boilerplate de laudo de colonoscopia (*"aparelho introduzido"*, *"íleo terminal"*, *"preparo"*):

| laudo | categorias que disparam | boilerplate presente? | efeito |
|---|---|---|---|
| colonoscopia, só o termo | `{colonoscopia}` ⊆ soft | sim | **rebaixa** — é o `+771` |
| colonoscopia com achado real | `{crohn, colonoscopia}` ⊄ soft | sim | **não rebaixa** ✔ |
| imagem com *"correlacionar com colonoscopia"* | `{colonoscopia}` ⊆ soft | não | **não rebaixa** ✔ |

**É a régua clínica validada, em config pura.** E chaveia pelo **conteúdo do próprio laudo**, não
pelo metadado `exm_tipo`, cuja confiabilidade neste corpus ninguém mediu.

**Rodar:** união `full_doc`, janela 6, termo `colonoscopia` ativo nos dois corpora, com
`document_vet` habilitado.

**Contar:** quantos dos `+771` da colonoscopia o vet rebaixa · quantos dos 25 da imagem caem junto
(deve ser zero) · o total de relevantes do legado não marcados, no formato de sempre.

🔴 **A pré-condição é o que decide, e é ela que precisa ser impressa:** dos laudos que o termo solto
acrescenta na colonoscopia, **em quantos ele é a única categoria a disparar?** Onde não for, o vet
não age por construção — e a solução não cobre aquele caso.

⚠️ **Custo declarado: é uso semântico torto da chave.** `document_vet`, `soft_findings` e
`normality_phrases` nomeiam enunciado de normalidade, não tipo de documento. Se o caminho vingar,
sobe com **cabeçalho no config dizendo o que está sendo feito e por quê** — e a generalização da
chave vira item da lib, não dívida silenciosa.

ℹ️ Roda como **último passo**, depois do juiz. Sem LLM neste ciclo, é indiferente.

---

# 8. Ordem de execução

| # | o quê | por que vem antes |
|---|---|---|
| 1 | **Depurar o denominador** — tirar os 6 FP do legado dos 131 e dos 20/22/25 | é barato e pode encolher a distância entre as opções |
| 2 | **Classificar os 5 laudos** contra o critério clínico: indicação × referência ao passado | separa "o regex falhou" de "não marcar está certo" |
| 3 | 🔴 **M2d — `document_vet`** (§7.8), com a pré-condição impressa | **config pura, e pode encerrar a discussão** — se funcionar, os passos 4 e a feature saem |
| 4 | **Ampliar o vocabulário do regex** com as formas da classe *indicação*, e remedir **nos dois corpora** | só se o M2d não bastar |
| 5 | **Os 12 regex da auditoria**, medidos um a um | destrava a homologação |
| 6 | **Montar o config único** — `full_doc`, janela 6 | |
| 7 | **Navegação** — 3 arquivos, formato `cancer_rim`, filtro explícito | antes de qualquer entrega ao negócio |

**A dúvida que sobe para o clínico é uma só:** *"visto na colonoscopia"*, num laudo de imagem, é
achado? Se não for — e a régua validada diz que o achado é a **indicação** —, a classe some e a
feature na lib perde a justificativa que restava.
