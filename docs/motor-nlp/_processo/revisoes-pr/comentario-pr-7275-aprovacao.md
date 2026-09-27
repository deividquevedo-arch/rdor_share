# PR 7275 — aprovado

@<Lucas> revisado e **aprovado**. Abaixo o que foi conferido e o único ponto que falta.

---

## O que a medição mostra

A `A3_paridade_dia` é o teste que faltava: rodou sobre os **800 laudos que o legado processou no
mesmo dia (17/09)**, e não sobre lote montado. É por isso que ela vale mais que as três medições
anteriores.

| | |
|---|---|
| paridade só com a régua | 96,0% |
| paridade com o juiz ligado | **97,75%** |
| juiz acionado | 29 laudos — só os que a régua navega |
| erros de LLM | zero |
| excesso removido pelo juiz | 14 de 27 |
| divergências novas criadas | **nenhuma** |

**O juiz melhora o resultado e não estraga nada.** Ele só revisa o que a régua já marcou, confirma
ou derruba, e nunca inventa caso novo.

E o limite está registrado com honestidade: o juiz **corta excesso e não recupera o que a régua não
pegou**. As 5 divergências em que o legado navega e o motor não continuam — essas só caem mexendo
em termo e regra, que é trabalho do próximo ciclo, não deste PR.

O critério de aceite também ficou no lugar certo: **paridade acima de 90% com toda divergência
explicada uma a uma**. O 99% da reumatologia é referência daquela linha, não requisito desta.

---

## Higiene da config — conferido item a item

| item | estado |
|---|---|
| blocos que o motor não lê (`catalog`, `monitoring`, `distribution`) | ✅ ausentes |
| bloco `runtime` | ✅ removido |
| `gold_filter.mode` (chave não lida) | ✅ ausente |
| versão da lib | ✅ `"0.12.3"` literal, não variável |
| cabeçalho e changelog no arquivo | ✅ 5 versões documentadas, com a janela e o número de cada uma |
| perfil declarado × perfil que executa | ✅ `use_embeddings: False` **e** widget `embedding_enable: "false"` — coerentes |
| caminho do modelo de embeddings | ✅ Model do Unity Catalog, padrão novo |

**Nenhum bloco morto.** É o que a régua de revisão de config pede, e aqui ela passa limpa.

---

## Exchange — conferido nos três ambientes

| item | estado |
|---|---|
| colunas | ✅ **23 nos três**, igual à reumatologia |
| `colunas_manuais` | ✅ vazio |
| listas suspensas herdadas do legado | ✅ removidas |
| `descriptografia` | ✅ fora dos três — quem decifra é a view |
| destinatários de prd | ✅ negócio no `emailTo`, acompanhamento técnico no `emailCc` |

---

## Job

| item | estado |
|---|---|
| `pause_status` | ✅ `UNPAUSED`, igual às seis da frota |
| `disabled` nas tasks de exchange | ✅ ausente — correto para linha nova (`jobs/README.md` §3.3) |
| versão do motor | ✅ `0.12.3` |

---

## 🔴 A única condição: ordem de merge

**Este PR entra depois do `7321` (João).**

O `embedding_model` aponta para `mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm`. Hoje
ele é inerte, porque a camada semântica está desligada — mas **quem sabe resolver esse nome é o
código que vem no PR do João**. Se este entrar antes, fica um endereço que a `hml` não entende.

---

## Dois avisos, nenhum bloqueia

**O job passa a rodar sozinho às 04:00 em hml, com envio ativo** para `nmedeiros.nnm` — mesmo
destinatário da reumatologia. Vale avisar o Natan que os arquivos começam a chegar.

**A nota do cabeçalho aponta para a `0.1.1`**, versão que não existiu — a config foi para `0.2.3`.
Os 11 casos que ela prometia corrigir continuam abertos; vale reapontar antes que a pendência se
perca.
