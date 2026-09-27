# DII — as medições que faltam para decidir a migração

> **Para que serve.** A migração do DII precisa escolher entre **uma config** ou **duas réguas**, e
> a escolha hoje está apoiada em análise, não em número. Este documento lista o que medir, o que
> cada número decide, e as armadilhas que já custaram caro nas migrações anteriores.
>
> Escrito em **2026-09-10**. Seis medições; nenhuma depende de decisão de terceiro para começar.

---

## 1. O que está em jogo, em cinco linhas

O legado roda **duas réguas** — imagem e colonoscopia — e une na view. Elas diferem em cinco
pontos: segmentação, achados, termos do `dii_explicito`, negação e órgãos.

Na plataforma nova, **uma config = uma régua para tudo que entra**. A régua léxica (achados,
negação, órgãos) **não recebe o tipo do exame** — verificado: `rule_engine.py` não tem nenhuma
referência a `exm_tipo`. Só critério quantitativo varia por exame, via `applies_to_exam_type`.

E a plataforma **não une duas saídas**: a view de exportação é montada a partir de uma tabela base.
Duas réguas exigiriam uma view manual, o que não cabe num processo diário.

**Então a pergunta é:** unir as duas réguas custa quanto? Se custar pouco, uma config resolve. Se
custar recall, é feature de lib — e aí o escopo dela depende de qual dos cinco pontos realmente
pesa.

---

## 2. As três perguntas que a medição responde

1. **A união muda decisão?** E em quantos laudos, em cada sentido.
2. **Qual dos cinco pontos causa a mudança?** Sem isso, não dá para dimensionar a feature.
3. **Os dois ramos são disjuntos na entrada?** Se forem, metade do problema desaparece.

---

## 3. As medições

### M1 · Os dois ramos se sobrepõem na entrada?

**Rodar:** o `gold_filter` de cada régua legada sobre a mesma janela, e cruzar por `id_exame`.

**Contar:** quantos exames caem nos dois filtros.

**Decide:** se a interseção for **zero**, cada laudo só é visto por uma régua, e os pontos de
divergência que dependem de "qual régua olha este laudo" **deixam de existir na prática**. Se não
for zero, os laudos da interseção são exatamente os casos difíceis — e precisam ser listados.

⚠️ Esta é a primeira porque pode encolher todas as outras.

---

### M2 · O termo `colonoscopia` — o único bloqueio já demonstrado

**Rodar:** contar, no corpus de colonoscopia, quantos laudos contêm o termo que o `dii_explicito`
da régua de **imagem** carrega e a de colonoscopia não.

**Contar:** laudos que casariam o achado **só por causa desse termo**, e quantos deles o legado
marca como relevantes.

**Decide:** confirma ou derruba a hipótese de que a união marca toda colonoscopia. Se confirmar, é
o item que sozinho justifica tratamento — e vale testar se **remover o termo** custa alguma coisa no
ramo de imagem (contar quantos laudos de imagem dependem só dele).

---

### M3 · Achados: 3 da imagem × 1 da colonoscopia

**Rodar:** aplicar os 3 achados da régua de imagem sobre o corpus de **colonoscopia**, e o achado da
colonoscopia sobre o corpus de **imagem**.

**Contar:** quantos laudos passam a casar um achado que hoje não casam.

**Decide:** achado que não casa custa zero. Se o número for baixo, este ponto sai da lista de
divergências e a união fica mais barata.

---

### M4 · Negação: 27 termos × 22 termos

**Rodar:** o corpus de cada ramo com a lista de negação **própria** e com a lista **unida**.

**Contar:** laudos que mudam de relevante para não-relevante, e o contrário.

**Decide:** mais termos de negação tende a **remover** falso positivo, o que é ganho. Se o delta for
só nessa direção e pequeno, a união dos termos é segura.

---

### M5 · Negação: janela 10 × janela 6

**Rodar:** cada corpus com as duas janelas, mantendo todo o resto igual.

**Contar:** laudos que mudam, nos dois sentidos, em cada corpus.

**Decide:** 🔴 **este é um dos dois pontos que a lib não expressa hoje** — a janela é global, só a
direção é por achado. Se o delta for material, a feature é necessária. Se for zero ou próximo,
adota-se uma janela só e o ponto morre.

---

### M6 · Segmentação: `auto` × `full_doc`

**Rodar:** cada corpus nos dois modos.

**Contar:** laudos que mudam, e — separadamente — o `segmentation_coverage` em `auto`.

**Decide:** 🔴 **o outro ponto sem expressão na lib.** ⚠️ Há precedente forte: no `mode: auto` a
hepatologia descarta conteúdo em **86% dos laudos**, e no ca-rim a troca para `full_doc` recuperou
**+25 laudos em 6 dias**. A hipótese a testar é que `full_doc` serve aos dois ramos — se servir, o
ponto morre e a feature encolhe.

---

## 4. Armadilhas — cada uma já custou uma medição refeita

**A fonte é a branch `hml` do repositório legado.** Nem a `main`, nem a cópia local em
`fabrica-ia-plataforma`. Na reumatologia, a cópia local estava desatualizada e a primeira medição
inteira foi descartada.

**Paridade não enxerga o filtro de entrada.** Ela só mede laudo que **chega** ao motor. O que o
`gold_filter` não seleciona é invisível — por isso M1 é medição separada. Na reumatologia, uma perda
de 10 em 92 relevantes atravessou duas medições limpas.

**Coorte sem a população não mede nada.** Se um resultado der zero divergência, a pergunta seguinte
é obrigatória: *quantos laudos exercitaram o caminho medido?* Sem esse número, "zero" pode
significar "seguro" ou "a coorte não tinha o caso". Aconteceu esta semana no TI-RADS: um run deu
zero divergência porque a coorte não continha a população.

**Um dia só não basta.** Medir em pelo menos **dois dias distintos**. Na reumatologia, o custo do
filtro era zero em 25/08 e só apareceu em 27/08.

**Custo por termo, não em bloco.** Quando um conjunto de termos muda o resultado, medir **termo a
termo**. Foi assim que se descobriu que um termo custava 200 laudos de volume para ganhar 1
relevante, e que outro custava 1 para 1.

**Cuidado com escape em regex de config.** Barra invertida em string de aspas simples vira caractere
de controle, e o padrão passa a casar **menos**, em silêncio. Conferir os bytes do padrão depois de
gravar. E acento importa: um padrão com `punc` não casa `punç`.

---

## 5. O que queremos receber

Uma tabela por medição, com **número absoluto e denominador** — não percentual sozinho:

| medição | corpus | laudos | mudam `1→0` | mudam `0→1` | exercitam o caminho |
|---|---|---|---|---|---|

Mais:

- **os dois dias** medidos, com a data;
- para M2, a **lista dos laudos** que o termo sozinho traz;
- para M5 e M6, o número **por ramo**, não somado — é o que dimensiona a feature;
- qualquer resultado zero acompanhado da pré-condição que prova que a coorte tinha o caso.

---

## 6. O que a resposta decide

| resultado | caminho |
|---|---|
| M1 dá interseção zero, e M2 é o único ponto com custo | **uma config**, com o termo tratado. Sem feature |
| M5 ou M6 mostram delta material | **feature de lib**, e o escopo dela é exatamente o que essas duas medirem |
| M3 e M4 mostram custo alto | a união não serve, e a decisão sobe para o negócio — é escopo clínico, não arquitetura |

ℹ️ **A feature, se necessária, já tem forma provável:** levar `applies_to_exam_type` — que já existe,
é testado e o time conhece dos critérios quantitativos — para os achados, mais a janela de negação
por achado, ao lado da direção que já é por achado. Não é vocabulário novo; é a mesma chave numa
segunda camada. Serve o DII **e** o ca-cólon, cujo levantamento registra o mesmo par de réguas
divergentes.

**Nada disso começa antes das medições.** O escopo da feature é o que os números de M5 e M6
disserem, e ele pode ser menor do que parece hoje.
