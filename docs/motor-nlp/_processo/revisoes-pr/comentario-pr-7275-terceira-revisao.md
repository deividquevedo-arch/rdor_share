@ revisado. A série `0.2.0` → `0.2.2` é o padrão que a gente quer ver numa calibração: três
versões medidas, uma reprovada por você mesmo e registrada no changelog, e cada número com a janela
e o lote ao lado.

Dois achados seus valem além desta linha:

**O** `0.2.0` **mostrou o juiz sendo chamado em 6.111 de 7.500 laudos (81,5%), todos sem achado da
régua.** É o mesmo defeito que medimos na hepatologia em 16/09 — card `283648` `[P0-29]`. Duas
linhas, medições independentes, mesma causa: piso da banda abaixo do teto de um laudo sem achado.

**E o** `0.2.1` **é a prova mais forte que temos de um segundo problema:** a semântica promoveu **33 dos
44** laudos sem achado, sem passar pelo juiz. Com a banda estreita a promoção semântica não alcança
o árbitro; com a banda larga o juiz vê tudo e o custo inviabiliza. Não há calibração que resolva —
é a lib que precisa do invariante. Vou ampliar o `283648` com isso.

Por isso **desligar a semântica está certo**. Você refinou duas vezes antes de desligar, e o que
restou não é problema de configuração.

---



## Dois itens antes de aprovar

**1. Sincronizar com a** `hml` — a branch está **32 commits atrás**. Mesmo ponto da revisão anterior.

**2. O** `embedding_model` **da** `0.2.3`

Ele aponta para , e é **inerte hoje**
porque `use_embeddings` é `False` — conferido.`mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm`

O ponto é que **quem resolve esse nome é o** `ConfigLoader` **do PR** `7321` **(João), que ainda não está
mergeado**. Se este PR entrar antes, a config declara um identificador que o loader da `hml` não sabe
resolver: sem efeito enquanto a semântica estiver desligada, e armadilha no dia em que alguém ligar.

🔴 **Ordem de merge: este PR entra DEPOIS do** `7321`**.**

ℹ️ E aproveitando: `decision_mode: 'hybrid'` com `use_embeddings: False` fica como bloco declarado e
não consumido — vale alinhar os dois.

---



## O bloqueio de merge é a paridade, e você já o declarou

`A1_paridade_hml` está em **97,3% contra o alvo de 99%** herdado da reumatologia, com 50 críticas
adjudicadas: 23 fora do escopo de TC, 15 termo sem qualificador, **11 defeito desta config**, 1 grau
leve.

**São os 11 que interessam** — os outros 39 são divergência explicada, não defeito. Fechando esses,
a paridade sobe e o critério de merge se resolve sozinho.

O `match_rate` da janela inteira na `0.2.2` também segue pendente; você marcou a projeção
(97,3% → ~98,2%) explicitamente como estimativa, o que está correto.

---



## Uma correção minha sobre o lote de 44

Na revisão anterior eu disse que *"a precisão de 0,556 contra o gabarito dos 44 é o número que decide
despausar o job"*. **Retiro: esse número não decide nada**, e o seu próprio cabeçalho explica por quê.

O lote são **todos positivos do algoritmo de junho v4** — 6 Sim e 38 Não. Como você escreveu, ele
*"mede se a regra de julho está traduzida; não mede paridade com a produção de hoje"*. É teste de
tradução, e nesse papel funcionou.

Como medida de precisão, ele não sustenta: a régua navega **9 dos 44**, e a diferença entre 0,556
(`5/9`) e 0,625 (`5/8`) é **um laudo**.


|              | valor | IC 95%      |
| ------------ | ----- | ----------- |
| régua        | 0,556 | 0,27 – 0,81 |
| régua + juiz | 0,625 | 0,31 – 0,86 |


Os intervalos se sobrepõem quase inteiros — com esse denominador o lote não distingue 0,55 de 0,85,
nem consegue dizer se o juiz ajudou. E como só há positivos do legado ali, o número não se traduz em
quantos falsos positivos chegariam ao negócio.

**Então o que falta para despausar não é melhorar 0,625 — é uma medida de precisão em população
real, que ainda não existe.** Fica como conversa separada, depois do merge, junto com a leitura das
2 navegações do legado derrubadas pelo juiz (`ateromatose difusa` sem sítio coronariano).

Com o sync, a ordem de merge e os 11 resolvidos, aprovo.