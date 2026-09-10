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

## 6. Terceira passada — clean code, arquitetura, PEPs, segurança, LangGraph e ciência de dados

Passada feita lendo o código, não o diff. **Rendeu mais que as duas anteriores**, e separa o que é
deste PR do que é da esteira compartilhada — esta última é decisão de MLOps, não do autor.

### 6.1 O que está bem-feito e vale preservar

**LangGraph — é o ponto mais forte do projeto:**

- grafo linear com dois roteamentos condicionais e **atalhos explícitos para `END`**
  (`materialize_fast_path`, `materialize_empty_finais`) — saída rápida é nó, não `if` escondido;
- nós são **funções puras** `(state, source)`, e o grafo só as conecta. A injeção do `source` na
  construção mantém tudo testável sem tocar em I/O;
- **os reducers estão nos dois campos certos.** `segmentos_pos_fanout` e `llm_calls` são
  `Annotated[list[...], add]` — exatamente os dois que o fan-out escreve. É o erro mais comum em
  LangGraph, e aqui não foi cometido.

**Segurança da credencial.** O token entra **por parâmetro**, não por `os.getenv` nem literal; há
guarda explícita para token vazio; o cabeçalho é montado no ponto de uso. Não há segredo versionado.

**Endereçamento.** `enderecos.py` como fronteira única dos nomes de tabela é bom padrão — o defeito
está nas constantes de módulo (D3), não no desenho.

### 6.2 Segurança — dado de paciente em log

Em falha, o módulo de LLM imprime 500 caracteres da resposta do modelo e a representação da
exceção. O conteúdo é a classificação **do comentário de um paciente**, e a representação de uma
exceção HTTP costuma carregar o corpo da requisição — que contém o comentário.

Vai para o log do job, que tem audiência maior que a tabela e não tem o mesmo controle de acesso.

**Sugestão:** registrar motivo estruturado e o identificador da predição, deixando o conteúdo na
tabela de trace, onde o acesso é governado.

A degradação de cache e de `top_k` também registra corpo de resposta do gateway — risco menor,
mesma categoria.

### 6.3 O determinismo declarado tem uma porta de saída em runtime

A chamada é configurada determinística: temperatura zero, `top_k` em 1, `top_p` ignorado.

**Mas `top_k` é removido do payload em runtime** quando o gateway o recusa com 400. A partir daí o
run inteiro segue sem ele, e a hipótese de determinismo sobre a qual a paridade foi medida deixa de
valer no meio da execução.

Isto é **candidato direto a explicar os três comentários que divergem de junho com prompt idêntico**,
que a descrição atribui à troca de caminho. A degradação é registrada, então dá para confirmar:
basta olhar se aquele run degradou.

A flag que guarda a degradação é **estado global mutável de módulo**, e o job roda com quatro
workers. É a mesma classe do singleton sem lock que a `0.13.0` do motor NLP acabou de corrigir.

### 6.4 Ciência de dados — a amostra não distingue o que a descrição compara

**179 trechos** sustentam a comparação 93,3% contra 89,4% — cerca de sete acertos de diferença. Com
essa amostra os dois números **não são distinguíveis**: o intervalo de confiança de uma proporção
próxima de 0,9 sobre n=179 fica em torno de ±4,5 pontos.

Isso **não invalida a conclusão** — reforça. A descrição já oferece a comparação correta, contra os
**89,1%** só-Haiku de junho, e 89,4% está acima. O que vale ajustar é a leitura de que houve queda:
não há queda demonstrável, há mudança de configuração declarada.

**O gabarito da Ouvidoria não declara versão nem data de anotação.** É a mesma lacuna que já custou
uma conclusão errada no motor NLP: sem isso, meses depois não se distingue régua que mudou de
gabarito que mudou.

### 6.5 Clean code e PEPs

| item | observação |
|---|---|
| **Sem `pyproject.toml`, `setup.cfg` ou `.pre-commit-config.yaml`** | não há lint, formatador nem verificação de tipos configurados. Os 1.242 testes rodam por invocação manual |
| **`AgentState` mistura convenções** | campos em camelCase convivem com campos em snake_case no mesmo `TypedDict`. Os primeiros provavelmente espelham colunas de origem — mas nada no tipo diz isso, e a PEP 8 pede snake_case |
| **`top_p` na assinatura, ignorado** | parâmetro declarado e não consumido, com `noqa` explicando que é compatibilidade. É o mesmo padrão que a régua de config do motor NLP proíbe: quem lê conclui que o valor age |

### 6.6 Para a revisão de Ops — decisões que não são do autor

**Os três valem para todos os projetos da fábrica, não só o NPS.**

**M1 — a esteira publica sem rodar teste nenhum.** O pipeline do NPS apenas estende o template
compartilhado, e o template **não tem `pytest`, `ruff`, `lint` nem stage de teste** — só publicação
e criação de job. Os 1.242 testes verdes são execução **manual**, e nada impede um merge com a
suíte vermelha. Combinado com D1, o defeito B1 desta revisão tinha dois portões abertos em série.

**M2 — as dependências não estão fixadas.** O notebook instala a biblioteca de grafo por **faixa**,
não por versão. O comportamento do agente pode mudar **sem nenhuma alteração no repositório** — é a
mesma classe do `latest` que moveu o motor em produção cinco vezes.

**M3 — o template é referenciado por tag móvel.** Uma tag pode ser movida, e aí a esteira muda sem
que nenhum repositório consumidor mude.

---

## 7. Cruzamento com a revisão de Ops

Chegou uma segunda revisão, pelo lado de Ops: 13 itens, checklist de 5 fases, dimensionamento de
4 a 5 sprints e conclusão *não pronto para produção*. **Toda afirmação cruzada abaixo foi conferida
na árvore publicada da branch** — nenhuma foi aceita pelo texto.

### 7.1 As duas revisões medem contra réguas diferentes, e ambas estão certas

A revisão de Ops mede contra **prontidão para produção**. Este PR tem como alvo a **`hml`**, cria o
job **pausado** e exclui explicitamente do escopo ligar o job, promover para `main` e para prd.

Não há contradição a resolver: *não pronto para produção* e *pronto para entrar na `hml`* são
compatíveis. O que a resposta precisa separar é **o que impede o merge** do **que é backlog até prd**
— e é isso que a lista de ações abaixo faz.

⚠️ O dimensionamento de 4 a 5 sprints é para o conjunto inteiro. Aplicá-lo ao merge confunde as duas
réguas e para uma entrega que já está validada no escopo que declara.

### 7.2 Onde as duas revisões se encontram

| item de Ops | nesta auditoria | o que a verificação mostrou |
|---|---|---|
| `/mnt/` no caminho publicado | — | ✅ **confirmado, e é mais forte que o relatado.** Não é constante residual: `path_controle_carga` alimenta a leitura das linhas 245 e 258 e a marca d'água da 522. É **leitura em runtime** do mount legado, em 3 pontos de `data/ntb_ia_nps_entrada.py` — os outros dois notebooks publicados têm zero |
| colunas de PII na saída | §6.2 (dado de paciente em log) | ✅ **confirmado, com correção.** As 4 colunas atravessam a cadeia inteira — entrada → classificação → `output_schema` → exportação → `contrato_fabrica` (renomeadas para `nm_paciente`, `num_cpf_paciente`). **Mas `telefonePaciente` e `emailPaciente` são `cast(null as string)` na entrada:** sempre nulos. A exposição real é **nome e CPF**, e ela **chega à camada de exportação**, que é a superfície voltada ao negócio |
| duplicação do mapa de catálogos | D2 | ✅ confirmado. Foram encontradas **4** cópias, não 3; conferidas **idênticas hoje** — não há deriva ainda, o risco é de manutenção |
| referências de teste fixas | D1 | ✅ mesma raiz. Nenhum teste cobre a árvore que a esteira publica |
| `workers=4` | E1 | 🟡 **atenuado pela verificação.** A linha 100 lê um **widget** com default `'4'` — é parametrizável, não fixo no código. Segue valendo o esclarecimento de E1 (a descrição pede 2), mas não é bloqueio de código |

### 7.3 O que cada revisão viu sozinha

**Só na revisão de Ops** — e vale corrigir: o `/mnt/` em runtime é o item mais concreto das duas
revisões, porque quebra sem aviso se o mount sair do ar.

**Só nesta auditoria**, e nenhum apareceu do outro lado:

- **B1** — `NameError` na primeira chamada de `eval/acuracia.py`. É o único bloqueante de código, e é
  correção de uma linha;
- **§6.3** — o determinismo tem porta de saída em runtime, guardada por **estado global mutável de
  módulo**, com o job rodando a 4 workers. É o que dá peso real ao número de workers, e provavelmente
  explica as três divergências que a descrição deixou em aberto;
- **§6.2** — conteúdo de comentário de paciente impresso em log de job;
- **§6.4** — a amostra de 179 trechos não distingue os dois números que a descrição compara.

**Localização do item de schema já levantado pela revisão de Ops:** a escrita afetada é a principal
da classificação — `mode('append')` com **`mergeSchema: 'true'`**, linha 538 de
`model/ntb_ia_nps_classificacao.py`. Somada a D1 e à esteira sem etapa de teste, a deriva entra na
tabela de saída **em silêncio**. É a mesma classe da lacuna de contrato que custou três ondas de
correção na `0.12.1` do motor, o que sustenta a recomendação já feita de validar o schema antes de
escrever em vez de absorver a diferença.

### 7.4 Ações para o autor

**A — antes do merge na `hml`** (as três somam menos de um dia):

1. **B1** — corrigir o `NameError`. Uma linha.
2. **`/mnt/` fora do caminho publicado** — trocar a leitura do controle de carga por Volume do Unity
   Catalog ou por tabela. É a única dependência de infraestrutura do workspace antigo no que sobe.
3. **E1, E2, E3** — responder os três esclarecimentos. Sobre `workers`, basta declarar qual é o valor
   pretendido: o widget já permite os dois.

**B — antes de prd, como débito registrado** (não bloqueia a `hml`):

4. **PII na exportação** — confirmar com o DPO se nome e CPF são necessários na superfície de
   consumo. **E resolver as duas colunas sempre nulas:** coluna declarada no contrato e nunca
   preenchida é bloco morto — ou passa a ser preenchida, ou sai do schema.
5. **§6.2** — tirar conteúdo de comentário do log, deixando motivo estruturado e identificador.
6. **§6.3** — trocar o estado global mutável por estado por execução, e registrar na saída se a
   degradação ocorreu. Sem isso, a premissa de determinismo não é auditável depois do run.
7. **`mergeSchema: 'true'`** — fixar o schema esperado e falhar na divergência, em vez de absorvê-la.
8. **D1** — um teste que importe a árvore publicada. Fecha a raiz de B1 e do item de referências fixas.
9. **§6.4** — ajustar a leitura da comparação: não há queda demonstrável na amostra, há mudança de
   configuração declarada. E datar e versionar o gabarito.
10. **D2, D3** — cópia única do mapa de catálogos, resolvida por ambiente.

**C — não é do autor, é decisão de MLOps** (§6.6): esteira sem etapa de teste, dependências sem
versão fixada e template referenciado por tag móvel. Os três valem para todos os projetos da
fábrica, e a decisão pertence a quem revisa por Ops — não cabe pedir ao autor deste PR.

---

## 8. Conclusão

**Um bloqueante, de uma linha.** Três esclarecimentos que custam minutos. Oito notas de débito na
segunda passada e mais seis na terceira, das quais **D1 é a que vale priorizar** — é a causa-raiz do
bloqueante e continua aberta depois de ele ser corrigido.

**Da terceira passada, dois itens sobem de peso:** o dado de paciente em log (§6.2) e a porta de
saída do determinismo (§6.3), que provavelmente explica as três divergências que a descrição deixou
em aberto.

**E três itens saem do escopo do autor** (§6.6): a esteira compartilhada não roda teste, as
dependências não estão fixadas, e o template é referenciado por tag móvel. São decisões de MLOps.

**O cruzamento com a revisão de Ops (§7) não abriu item novo — precisou dois itens e refutou um.**
O `/mnt/` no caminho publicado é leitura em runtime, e por isso sobe para a lista de merge; a PII na
exportação é real, mas duas das quatro colunas são `cast(null as string)`, o que muda a ação
proposta; e o `workers=4` **não é hardcode**, é default de widget. O que esta auditoria acrescenta
está todo em B1, §6.2, §6.3 e §6.4. As duas revisões medem contra réguas diferentes — produção e
`hml` —, e as duas se sustentam.

O PR está acima do padrão em documentação: cada número com base e data, divergências deliberadas
declaradas, limites conhecidos listados, e critério de GO/NO GO explícito. Os achados desta
auditoria não contradizem isso — eles vêm de perguntas que só se aprende a fazer depois de errar,
e a maior parte é dívida herdada da estrutura de duas árvores, não decisão deste PR.
