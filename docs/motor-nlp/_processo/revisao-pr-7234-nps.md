# Revisão do PR 7234 — NPS na esteira da fábrica

> Registro da auditoria. **Parecer: aprovar com uma correção obrigatória de uma linha.** O objetivo
> declarado é acelerar a entrega, então o que não impede o merge está classificado como débito
> técnico, com o custo de cada item nomeado.
>
> `IAAzureDatabricksNPS` · `nps/feature/esteira-fabrica` → `hml` · 340 arquivos, 330 adições.
> Auditado em **2026-09-10**.

---

## Método

Duas passadas. A **primeira** conferiu o que a descrição pede que se revise. A **segunda** aplicou
as lições que os processos do `nlp_engine` e da `nlp-platform` produziram — cada lição vira uma
pergunta feita a este PR, e três dos achados só apareceram nela.

A tabela da §5 registra qual lição levantou o quê, para o método ser reaproveitável e não depender
de quem revisa.

---

## 1. O que foi verificado e está correto

| item | estado |
|---|---|
| Job diário | `PAUSED` · `max_concurrent_runs: 1` · `usarOpusFallback: false` · cron 07:00 |
| Catálogo por ambiente | `dev`/`hml`/`prd` → `diamond_fabrica_ia_dev`/`_hml`/`diamond_fabrica_ia` |
| Ambiente resolvido por widget | os três notebooks publicados chamam `por_ambiente(<widget>)` |
| Gateway obrigatório | `{host}/ai-gateway/mlflow/v1/responses`, sem caminho direto no código |
| Degradação registrada | `cache_control` e `top_k` recusados degradam **com log**, não em silêncio |
| Pendentes por throttle | há modo `RERUN` que os recupera na execução seguinte |
| Cópias de `enderecos.py` | quatro cópias, e as quatro estão **idênticas hoje** — 17 chaves, mesmo mapa |

**Sobre a acurácia de 93,3% → 89,4%.** A leitura da descrição está correta e a comparação justa é a
que ela mesma oferece: a referência só-Haiku de junho era **89,1%**, e o resultado de hoje é
**89,4%** — acima. Os 93,3% incluíam fallback para Opus, que saiu por decisão do PO. Não há
regressão a explicar.

---

## 2. 🔴 Bloqueante — um item, correção de uma linha

### B1 · `nps/src/eval/acuracia.py` levanta `NameError` na primeira chamada

O arquivo usa `canon` nas linhas 85, 111 e 147 e **não o importa**. A cópia em
`agent_langgraph_mvp/src/eval/acuracia.py` tem `from src.eval.gold_match import canon`; a cópia
publicada perdeu a linha ao ganhar o cabeçalho `# Databricks notebook source`.

Verificado que não há saída alternativa: **zero** ocorrências de `%run`, `sys.path`,
`import gold_match` ou `from gold_match` no arquivo publicado. O `gold_match.py` existe em
`nps/src/eval/` — falta apenas o import.

⚠️ **A suíte de 1.242 verdes não cobre este arquivo.** `tests/test_eval_acuracia.py` importa
`from src.eval.acuracia import ...`, que resolve para a cópia da **bancada** — a que tem o import.
Os testes atestam o arquivo que **não** é publicado.

**Correção:** acrescentar o import na cópia publicada.

---

## 3. 🟡 Esclarecimento antes do merge — três perguntas, minutos cada

Não são defeitos; são decisões que o diff toma e a descrição não declara. Respondidas, o PR segue.

### E1 · `workers: "4"` está commitado e a descrição pede 2

A descrição diz: *"workers 4 no job diário dá ~633 mil tokens/min, 3x o pico medido de 223 mil; E2E
passou com 2. **Sugestão: 2**"*. O `jobs/definicoes/fabrica-ia-nps.json` traz `"workers": "4"`.

Ou o valor muda antes do merge, ou o job nasce com 3x o pico medido e a sugestão vira tarefa de
outra pessoa depois.

### E2 · `timeout_seconds: 0` em uma das tasks

Zero significa **sem timeout**. Se for deliberado, vale um comentário no JSON; se não, é a task que
pode segurar o job indefinidamente. O job tem `max_concurrent_runs: 1`, então uma task pendurada
bloqueia todas as execuções seguintes.

### E3 · A fonte não varia por ambiente — dev e hml leem produção

`enderecos.py` declara `FONTE` como constante **única**:

```python
FONTE = "hfocus_prd.diamond.relatorio1_automidia_avaliacaoerecomenda_vw"
```

O dicionário de `por_ambiente()` devolve a mesma `FONTE` para `dev`, `hml` e `prd`. Todo o resto
varia por ambiente; a fonte, não.

É plausível que seja deliberado — a view pode existir só em produção, e o legado lia `hfocus_hml`.
Mas a diferença entre "a fonte só existe em prd" e "esqueceram de parametrizar" não está escrita, e
é a mesma classe do `catalog` apontando para o ambiente errado, que já derrubou uma subida no
motor NLP.

---

## 4. Débito técnico — documentado, não bloqueia

### D1 · 🔴 Nenhum teste cobre o que a esteira publica

**É a causa-raiz de B1, e sobrevive à correção dele.** Os testes vivem em `agent_langgraph_mvp/` e
importam `src.*`; o que a esteira publica é `nps/`. Enquanto as duas árvores existirem, qualquer
divergência entre elas passa verde.

**Custo de resolver:** um teste que importe os módulos por `nps/` — carregar o arquivo publicado e
verificar que ele importa e expõe o que promete. É pequeno e fecha a classe inteira, não só o caso
do `canon`.

**Custo de não resolver:** o próximo `canon` chega em produção com a suíte verde.

### D2 · Quatro cópias do mapa de catálogos, sem teste de deriva

`enderecos.py` existe em `agent_langgraph_mvp/src/`, em `nps/src/`, e **inline** dentro de
`ntb_ia_nps_entrada.py` e `ntb_ia_nps_exporta_consumo.py`. O terceiro notebook usa `%run` do
arquivo.

Estão idênticas hoje — verificado, 17 chaves e mesmo mapa nas quatro. Trocar um catálogo exige
acertar quatro lugares, e a divergência seria **silenciosa**: o run leria de um catálogo antigo sem
erro.

**Custo de resolver:** um teste que compare as cópias inline contra o arquivo. Transforma deriva
silenciosa em falha barulhenta.

### D3 · `enderecos.py` exporta constantes amarradas em `dev`

```python
_DEV = por_ambiente("dev")
CATALOGO = _DEV["CATALOGO"]
ENTRADA  = _DEV["ENTRADA"]
```

`from src.enderecos import ENTRADA` devolve **a tabela de dev em qualquer ambiente**, sem erro. Os
três notebooks publicados fazem certo — chamam `por_ambiente()` —, então **não há defeito hoje**. A
armadilha fica armada para o próximo, e o teste que "proíbe hive" não a cobre.

**Custo de resolver:** remover as constantes, ou fazê-las levantar em vez de resolver.

### D4 · A descrição diz "429 sem retry" e o código tem retry

O código trata 429/503 com backoff exponencial, jitter, respeito ao `Retry-After` e um teto
**separado** (`max_attempts_throttle`). O que vira pendente é o caso em que esse teto se esgota.

O comportamento parece correto — é a frase que está imprecisa. Como ela descreve um ponto de custo
e de resiliência, quem ler depois pode concluir que não há backoff.

**Custo de resolver:** uma frase na descrição.

### D5 · O modelo é alias, e alias muda sem mudar o repositório

O endpoint `databricks-claude-haiku-4-5` é um alias. Se ele passar a apontar para outra build, o
resultado muda **sem nenhuma alteração neste repositório**, e a homologação de 179 trechos deixa de
valer sem que nada acuse.

É a mesma classe do `nlp_engine_version: "latest"` do motor NLP, onde a versão em produção mudou
**cinco vezes** sem ninguém tocar no job.

**Custo de resolver:** registrar a versão efetiva do modelo na saída de cada run, como o
`engine_version` faz. Sem isso não há como responder "com qual modelo este resultado foi obtido".

### D6 · Publicação e consumo ficam em lugares diferentes durante a transição

A esteira passa a gravar em `diamond_fabrica_ia_<env>.nps`, e o consumidor lê
`diamond_ia_hml.nps`. A descrição registra a ponte como *fora deste PR*.

Enquanto a ponte não existir, o consumidor lê **dado que ninguém mais atualiza** — sem erro, com a
tabela respondendo normalmente. É a lição de que publicar não é ser consumido: os dois lados podem
estar certos isoladamente e o par estar quebrado.

**Custo de resolver:** nada neste PR, porque o job nasce pausado. O débito é garantir que a ponte
e o religar do job aconteçam na mesma janela, e que alguém avise o consumidor.

### D7 · A troca de fonte não foi medida em volume

O legado lia `hfocus_hml`; o novo lê `hfocus_prd`. A paridade de 179 trechos mede **acurácia sobre
o que chega**, não **o que passa a chegar**.

É a lição do reumato: paridade não enxerga o filtro de entrada. Lá, uma perda de 10 em 92
relevantes atravessou duas medições limpas porque o que o filtro não seleciona é invisível.

**Custo de resolver:** contar comentários no intervalo pelas duas fontes e comparar. Uma consulta.

### D8 · O diff carrega tooling e binários

`.claude/` com 65 arquivos, `legado/` com 51, e dois binários de documentação. A descrição já
reconhece os binários. Não impede nada; polui a revisão e o histórico.

---

## 5. As lições aplicadas, e o que cada uma levantou

| lição | pergunta feita ao PR | achado |
|---|---|---|
| Teste verde não prova nada | o que a suíte **não** olha? | **B1** e **D1** |
| Validar o artefato, não o repositório | os testes importam o que é publicado? | **D1** |
| Publicar não é ser consumido | quem lê o que este PR grava? | **D6** |
| Paridade não enxerga o filtro | a medição cobre o que passou a **entrar**? | **D7** |
| `latest` move produção sozinho | há dependência que muda sem mudar o repositório? | **D5** |
| Critério global não decide por ambiente | há valor único onde deveria haver um por ambiente? | **D3**, **E3** |
| Config não passa com bloco morto | há valor declarado que ninguém consome, ou que mente? | **D3** |
| Coorte sem a população não mede | a amostra contém o que a mudança afeta? | ✅ contém |
| Regex em config: armadilhas de escape | há padrão que pode ter sido corrompido na gravação? | ✅ sem sinal |

⚠️ **Três achados só apareceram na segunda passada** — D5, D6 e D7. Nenhum deles é visível lendo o
diff: são perguntas sobre o **entorno** da mudança, e só existem porque custaram caro antes.

---

## 6. Conclusão

**Um bloqueante, de uma linha.** Três esclarecimentos que custam minutos. Oito notas de débito, das
quais **D1 é a que vale priorizar** — ela é a causa-raiz do bloqueante e continua aberta depois de
ele ser corrigido.

O PR está acima do padrão em documentação: cada número com base e data, divergências deliberadas
declaradas, limites conhecidos listados, e critério de GO/NO GO explícito. Os achados desta
auditoria não contradizem isso — eles vêm de perguntas que só se aprende a fazer depois de errar,
e a maior parte é dívida herdada da estrutura de duas árvores, não decisão deste PR.
