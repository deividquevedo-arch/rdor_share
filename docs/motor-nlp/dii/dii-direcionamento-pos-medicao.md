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

| opção | imagem | colonoscopia | total de relevantes do legado não marcados |
|---|---|---|---|
| duas configs, ambas `full_doc` | 20 | 2 | **22** |
| uma config **sem** o termo (M8c, medido) | 29 | 0 | **29** |
| uma config **com o termo condicionado** | ~18 | 0 | **~18** |

⚠️ O `~18` é **derivado** (`29 − 11`), **não medido**. Pode haver sobreposição com os ganhos de M7
e M3. É o que a variante M8d abaixo fecha.

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
