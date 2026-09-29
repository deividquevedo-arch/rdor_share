# Parecer — o estudo de validação da randomização

> **O estudo tinha por finalidade VALIDAR A BIBLIOTECA — e a validou.** O mecanismo foi conferido
> de forma independente e passa. O que este parecer acrescenta é o que vem **depois** dela.
>
> ⚠️ **Correção ao que este documento dizia antes (22/09):** a §2 estava redigida como crítica ao
> estudo — *"valida o alocador, não o estudo"*. Isso é **injusto**: validar o alocador **era o
> escopo declarado**, e o próprio relatório lista em *"O que a medição não responde"* as mesmas
> lacunas que este parecer levantou. Elas são o **próximo passo**, não omissão.

- **Data:** 2026-09-22
- **Avaliados:** `docs/randomizacao/relatorio_v4.pdf` (backtest de 12 meses) e
  `rededor-ai-lib`, `docs/features/randomizacao.md`, branch `hml`
- **Verificação independente:** reimplementação da fórmula em Python puro, 1.000.000 de CPFs
  válidos, sem usar a biblioteca nem o notebook do estudo

---

## 1. O que foi verificado de forma independente, e passou

A fórmula declarada — `pmod(conv(substring(sha2(cpf + salt, 256), 1, 15), 16, 10), 10000)` — foi
reimplementada do zero e submetida às afirmações que os dois documentos fazem:

| afirmação | resultado sobre 1.000.000 de CPFs |
|---|---|
| fração de controle com corte em 5% | **4,9907%** |
| fração com corte em 10% | **9,9998%** |
| **subir de 5% para 10% não realoca ninguém** | 🟢 **confirmado** — o conjunto de 5% é subconjunto do de 10%; entram 50.091 e nenhum sai |
| determinismo — a mesma chave cai sempre no mesmo braço | 🟢 confirmado |
| `R7` — independência de atributo, testada pelo **dígito regional** do CPF | maior \|z\| entre as 10 regiões: **2,13** — dentro do acaso |

🟢 **Três decisões de desenho merecem registro positivo:**

1. **Hash em vez de `rand()`.** O argumento do documento está correto: `F.rand(seed)` é semeado por
   partição e não sobrevive a mudança de cluster, de particionamento ou a retry de task. Um estudo
   cuja alocação muda quando o cluster é reconfigurado não é auditável.
2. **A monotonicidade do corte.** Não é detalhe: permite **subir o braço de controle no meio do
   estudo sem realocar quem já estava**. Verificado, e é a propriedade que torna a decisão
   "5% ou 10%" reversível para cima.
3. **`sem_chave` sem valor padrão.** Obriga quem chama a decidir o que fazer com paciente sem
   chave, em vez de herdar um default. É política clínica, e está tratada como tal.

---

## 2. 🔴 A crítica central: o estudo valida o ALOCADOR, não o ESTUDO

O requisito `R7` — *"a distribuição de 5% deve ser replicada em qualquer estratificação"* — é
**verdadeiro por construção**. A avalanche do SHA-256 torna o bucket independente de qualquer
atributo do paciente, então testar 31 recortes é, no fundo, **testar que o SHA-256 funciona**.

**Ele funciona. E o teste quase não tem poder de detectar as falhas que importam, porque elas não
estão no hash.** O estudo confirma a parte que já era garantida e não toca as três que não são.

### 2.1 🔴 A taxa de casamento do CPF não foi medida — e é o viés real

A própria documentação da biblioteca chama isso de **"erro número um em produção"**: CPF mascarado,
zero à esquerda perdido no ETL, chave que não casa no *join*. O sintoma que ela descreve é *"menos
gente no controle do que o esperado"*.

**O relatório mede 91.773 pacientes "que a captação tocou" e não informa quantos ficaram de fora
por não ter CPF válido ou por não casar.** Esse número é o que decide se o estudo é válido:

- quem não casa **não entra em braço nenhum** — não é controle nem intervenção, simplesmente some;
- e **quem não tem CPF não é aleatório**: correlaciona com urgência, recém-nascido, estrangeiro,
  cadastro incompleto, atendimento sem documento.

🔴 **É exatamente o tipo de perda que o hash não protege, porque acontece antes dele.** Um sorteio
perfeito sobre uma população já filtrada por um critério enviesado produz dois braços balanceados
entre si e **não representativos** de quem o serviço atende.

### 2.2 🔴 Não há cálculo de poder para o desfecho

O relatório declara **4.591 pessoas no controle** e reconhece que *"grupo pequeno continua sem poder
para medir efeito sozinho"*. Mas **nunca declara o poder do estudo inteiro**: para detectar qual
tamanho de efeito, sobre qual desfecho, em qual janela de acompanhamento.

**Sem isso, a decisão "5% ou 10%" não tem como ser tomada** — é precisamente uma pergunta de poder,
e está sendo tratada como preferência.

⚠️ **E ela tem custo clínico, não só estatístico.** Controle é paciente que o modelo **encontrou** e
que **não é enviado à navegação**. Dobrar o controle dobra o número de pessoas com achado que não
são navegadas. A régua do projeto é explícita: *em rastreio, precisão comprada com paciente perdido
é regressão*. **Subir para 10% precisa ser justificado por poder, não adotado por simetria.**

### 2.3 ⚠️ O desfecho não está definido em nenhum dos dois documentos

Nenhum dos textos declara **o que será comparado entre os braços**, nem a janela de
acompanhamento. Sem desfecho declarado antes da análise, a escolha posterior do que medir é ela
mesma um grau de liberdade.

---

## 3. O que está bem resolvido e não precisa de mais trabalho

- **A aritmética dos grupos pequenos.** *"69% dos grupos não têm nenhum controle"* é resultado
  esperado, não defeito: num grupo de 4 pessoas a chance de nenhuma cair no controle é 81%. O
  documento trata isso corretamente.
- **O desvio de 2,7 σ nos grupos de 1 paciente.** A defesa por múltiplas comparações **se sustenta**:
  com 136 comparações, o próprio acaso produz um máximo em torno de 2,8 a 3,0 σ. Um 2,7 não é
  achado. ✅ Conferido de forma independente.
- **O aviso sobre a base de controle fixo (`R9`).** O documento registra que uma lista enviesada
  quebra o balanceamento. Está certo e está escrito.
- **Um paciente em várias linhas cai no mesmo braço**, porque a chave é o CPF. Isso evita
  contaminação entre linhas de cuidado, e é consequência do desenho — vale registrar como acerto.

---

## 4. ⚠️ Três divergências entre os documentos, a resolver antes de seguir

| # | divergência | por que importa |
|---|---|---|
| 1 | **Chave**: o relatório cita `num_cpf_paciente`; a biblioteca tem `coluna_chave` com padrão `cpf` | se o backtest usou uma coluna e a esteira usar outra, **a taxa de casamento validada não é a que vai valer** |
| 2 | **Branch**: o relatório atribui o commit `2e201f4` à `main`; o arquivo está na `hml` | precisa ficar claro **qual versão da biblioteca foi exercitada** — é a mesma classe de problema que já custou promoção pulada neste projeto |
| 3 | **O elo não está declarado**: nada diz que os 91.773 vieram de *join* contra `massa_cpf_randomizada` | se o backtest aplicou a fórmula direto, ele **testou a fórmula, não a esteira** — e é a esteira que vai rodar |

---

## 5. Conclusão

**O sorteio está pronto. O estudo não.**

O que a biblioteca entrega — alocação determinística, auditável, independente de atributo,
reversível para cima — está correto, verificado de forma independente, e é melhor do que o padrão
usual de `rand()` com semente. **Não há razão para mexer no mecanismo.**

O que falta é tudo o que fica **em volta** dele, e nada disso é resolvido por mais teste de
estratificação:

1. 🔴 **medir a taxa de casamento do CPF** na população real da captação, e caracterizar quem fica
   de fora — é o único viés que o hash não protege;
2. 🔴 **declarar o desfecho e calcular o poder** — é o que decide 5% ou 10%, e hoje a decisão está
   sem base;
3. 🔴 **resolver ética, LGPD e formalização**, que o próprio relatório lista como pendente e que
   **precede** a operação, não a sucede;
4. ⚠️ **fechar as três divergências** da §4.

---

## 6. Próxima medição proposta, e ela é barata

**Contar, na população que a captação toca, quantos pacientes têm CPF ausente, malformado ou que
não casa** — e comparar o perfil desse grupo com o dos que casam, nas mesmas cinco dimensões que o
relatório já usa (regional, unidade, linha de cuidado, quantidade de linhas, mês de entrada).

Se o perfil for o mesmo, a perda é ruído e o estudo segue. Se não for, **a perda é viés e precisa
entrar no desenho** antes de qualquer paciente deixar de ser navegado.

📄 Script de verificação do mecanismo, reexecutável: reimplementa a fórmula em Python puro e testa
fração, determinismo, monotonicidade de 5% para 10% e independência pelo dígito regional.

---

# ADENDO — a medição do CPF, e por que ela NÃO fecha (22/09)

## 7. O que foi medido

Levantamento sobre a população do pipeline novo: **243.279 pacientes distintos** em 554.605 linhas
de exame, das seis linhas em produção. ⚠️ **A janela é de 33 dias (21/08 a 22/09)**, porque é todo
o histórico que a plataforma nova tem — **não** os 12 meses do backtest.

| | |
|---|---|
| pacientes sem CPF recuperável | **111.226 de 243.279 — 45,7%** |
| dos que têm CPF, com 11 dígitos | 132.047 |
| **começando com ZERO** | **16.874 de 132.047 — 12,8%** |
| falham o dígito verificador | 11 de 132.047 (0,008%) |
| CPF repetido entre pacientes distintos | 458 CPFs, envolvendo 921 pacientes |
| CPF de teste (dígitos iguais) | **0** |

🔴 **Os 12,8% que começam com zero são o achado operacional mais acionável.** É exatamente o caso
que a documentação da biblioteca chama de *"erro número um em produção"*: zero à esquerda perdido
no ETL faz o *join* não casar, e o sintoma é **menos gente no controle do que o esperado**.

## 8. 🔴 Mas os 45,7% NÃO estão estabelecidos como "sem CPF"

O número vem de um *join* contra `cubo_corp_vw_dim_paciente`, e há duas hipóteses concorrentes:
**o paciente não tem CPF**, ou **o cubo não cobre aquele paciente**.

**Controles rodados:**

| controle | resultado | o que elimina |
|---|---|---|
| a chave do *join* falha o `cast`? | **0 de 188.612** | ❌ não é artefato de chave |
| o cubo tem CPF para os seus próprios pacientes? | **2.094.214 de 6.458.813 sem CPF — 32,4%** | ✅ ausência de CPF **é fenômeno real** no dado corporativo |
| a taxa varia por especialidade, mês, modalidade? | 41% a 47% — **quase plana** | ⚠️ não sugere efeito clínico nem sazonal |
| **varia por unidade?** | **21% a 80%** | 🔴 **é o único recorte que varia** |

**Nenhum dos dois lados foi eliminado.** O que está estabelecido é que **falta de CPF é real e
grande** no dado corporativo (32,4% do próprio cubo), e que **a variação é por UNIDADE**.

## 9. 🔴 E a conclusão para o estudo é a mesma nas duas hipóteses

| hipótese | consequência para a randomização |
|---|---|
| **é CPF ausente de verdade** | ~45% não entram em braço nenhum, e a perda varia de 21% a 80% **por hospital** — **viés por unidade**, que é justamente a dimensão que o backtest testou e onde declarou estar "dentro do acaso" |
| **é cobertura do cubo** | então **não se sabe** a qualidade de CPF da população, e a premissa central do estudo segue **não verificada** |

✅ **Em ambas, a mesma coisa é verdade: o estudo não pode assumir que a alocação alcança todo
mundo.** E o backtest não reporta esse denominador.

⚠️ E há um detalhe que agrava: o relatório mede **91.773 pacientes que a captação tocou**. Quem
rodou aquele *backtest* **conseguiu CPF para eles** — então o denominador da perda existe do lado
de quem mediu, e é uma pergunta de uma linha: **quantos da captação ficaram de fora por CPF?**

## 10. Outras duas observações

- 🟡 **`massa_cpf_randomizada` não foi encontrada** em nenhum dos catálogos acessíveis
  (`gold_corporativo_ia`, `gold_corporativo`, `diamond_fabrica_ia` e variantes, `mlops_fabrica_ia`,
  `gold_fabrica_ia*`). ⚠️ **Ausência sob permissão não é inexistência** — pode viver em outro
  workspace. Mas a tabela é o centro do desenho descrito na documentação, e vale confirmar onde está.
- 🔴 **A fonte de cobertura completa tem o CPF CIFRADO.** `gold_corporativo.patient` guarda o CPF em
  base64, e decifrar exige `EXECUTE` em `security.prd.rdsl_decrypt` — o mesmo grant que já bloqueia
  três frentes do projeto. **Medir isso direito depende dele.**

## 11. O que este adendo muda no parecer

A §6 pedia *"medir a taxa de casamento do CPF"*. **A medição foi feita e não fecha** — e o motivo
é informação: a fonte acessível não distingue ausência de cobertura.

**O pedido muda de forma:** não é mais *"meçam"*, é **"digam o denominador que vocês já tiveram"**.
Quem rodou o backtest partiu de uma população de 91.773 e precisou de CPF para ela. **Quantos ficaram
de fora, e de quais unidades?**


---

# ADENDO 2 — o percentual, e por que subir e descer não são simétricos (24/09)

Fecha o item 2 da §5 (*"declarar o desfecho e calcular o poder — é o que decide 5% ou 10%"*).

## 12. A biblioteca é aplicável — e o que isso quer dizer, exatamente

| propriedade | verificado | como |
|---|---|---|
| fração entregue bate com a pedida | **4,9907%** no corte de 5%; **9,9998%** no de 10% | 1.000.000 de CPFs sintéticos |
| determinístico | a mesma pessoa cai sempre no mesmo braço | reexecução do sorteio |
| independente de atributo | maior \|z\| = **2,13** entre as 10 regiões | dígito regional do CPF |
| monótono | subir de 5% para 10% **não realoca ninguém** | conjunto de 5% contido no de 10% |

**Reimplementada em Python puro, de forma independente da que roda em produção.** Não é leitura do
código: é a fórmula da doc reproduzida e confrontada com o que ela afirma.

✅ **Nada a corrigir no mecanismo.** É superior ao padrão usual de `rand()` com semente, porque
dispensa guardar a alocação — ela é recalculável a qualquer momento a partir do CPF.

🔴 **O que a biblioteca NÃO cobre, e segue valendo do parecer original:** ela aloca quem tem CPF.
**12,8% dos CPFs começam com zero** e viram armadilha de tipagem, e a taxa de casamento na
população real da captação **continua sem denominador declarado** (§7 a §11). O hash é imune a
viés; a porta de entrada dele não é.

## 13. A análise que define o percentual

**População da onda 1** — `ca_estomago`, `cancer_rim`, `reumatologia`, `tirads`,
`transplante_pulmao` —, medida sobre a saída de produção: **19.077 pacientes encaminhados/ano**
(52,3/dia).

Com α de 5% bilateral e poder de 80%, a precisão relativa é `(z_α + z_β) × √(1/n_c + 1/n_i)`:

| fração | controle | intervenção | precisão relativa | contra o 10% |
|---|---|---|---|---|
| 2% | 382 | 18.695 | 0,1448 | 2,14× pior |
| **5%** | **954** | 18.123 | **0,0931** | **1,38× pior** |
| 7,5% | 1.431 | 17.646 | 0,0770 | 1,14× pior |
| **10%** | **1.908** | 17.169 | **0,0676** | **referência** |
| 20% | 3.815 | 15.262 | 0,0507 | 0,75× (25% melhor) |

**As três leituras que decidem:**

1. 🔴 **5% exige um efeito 37,6% MAIOR que 10%** para ter a mesma chance de ser detectado. Não é
   "um pouco pior": é a diferença entre enxergar e não enxergar um ganho real de tamanho médio.
2. 🟢 **Dobrar de 10% para 20% melhora só 25%.** A curva achata — não há motivo para ir além de
   10%, e o custo (pacientes não navegados) dobra.
3. ℹ️ **7,5% fica a 14% do 10%.** Se o limite for ético e não estatístico, é o meio-termo que
   quase não perde poder.

⚠️ **Os números de 14% e 19% que circularam dependem de uma premissa não medida.** Eles saem de um
coeficiente de variação implícito de **≈ 2,05** no desfecho. **O desfecho não está declarado em
nenhum dos dois documentos** (§2.3), então a variabilidade dele é suposição.
✅ **O que NÃO depende dessa premissa é a comparação entre as opções** — os fatores 1,38× e 0,75×
saem só dos tamanhos de amostra. **A escolha entre 5% e 10% está sustentada; o valor absoluto do
efeito detectável, não.**

🔴 **Nenhuma linha isolada conclui sozinha.** A maior delas, o TI-RADS, detecta **18% mesmo com
10%**. O estudo tem de ser **único e agregado** — se alguém esperar resultado por linha de cuidado,
a expectativa está errada desde o desenho, e isso precisa estar na ata.

## 14. 🔴 Dá para voltar de 10% para 5%? A resposta é sim e não, e a distinção importa

**Tecnicamente, sim — é um número no `WHERE`.** Medido nos mesmos 1.000.000 de CPFs:

| movimento | saem do controle | entram no controle |
|---|---|---|
| 5% → 10% | **0** | 50.091 |
| 10% → 5% | **50.091** | **0** |

O corte é **monótono**: subir só acrescenta, descer só remove. Não há realocação cruzada em
nenhuma das direções, e ninguém troca de braço "por sorteio".

🔴 **Metodologicamente, não.** E a razão não é o sorteio — é o que já aconteceu com as pessoas:

- **Subindo**, os 954 do controle inicial seguem intactos. Os que entram depois têm janela de
  observação menor, o que faz do estudo um desenho **escalonado** — tratável, desde que a análise
  seja por data de entrada.
- **Descendo**, 954 pessoas que já passaram um período **deliberadamente não navegadas** voltam a
  ser navegadas. Viram um **crossover não planejado**: o desfecho delas deixa de ser atribuível a
  um braço só, e elas são metade do controle.

⚠️ **A frase certa não é *"não dá para voltar"*, é *"voltar não desfaz"*.** A não-navegação já
ocorreu e não se reverte mudando um flag. Quem descer tem três saídas, todas piores que não ter
subido: descartar esses pacientes da análise (perde metade do controle), tratá-los como censurados
na data da troca (perde a janela longa), ou manter o desenho e aceitar contaminação.

## 15. Os dois caminhos

### Caminho A — começar em 5% e subir para 10% depois de validar

| | |
|---|---|
| 🟢 a favor | expõe metade das pessoas enquanto o operacional ainda não foi provado; a subida **não realoca ninguém**, então o controle inicial é preservado |
| 🟢 | permite medir a taxa de casamento do CPF **em operação real** antes de comprometer 10% |
| 🔴 contra | o estudo vira **escalonado**: dois grupos com janelas diferentes, e a análise precisa prever isso desde o protocolo |
| 🔴 | enquanto estiver em 5%, o poder é o de 5% — se a decisão for tomada nesse período, ela precisa de um efeito **37,6% maior** |
| ⚠️ | exige definir **agora** o gatilho objetivo da subida (qual métrica, qual valor, quem decide), senão o 5% vira permanente por inércia |

### Caminho B — começar já em 10%

| | |
|---|---|
| 🟢 a favor | poder pleno desde o primeiro dia; janela de observação **uniforme**; análise mais simples |
| 🟢 | não há segunda rodada de aprovação ética para a mudança de fração |
| 🔴 contra | **a decisão é de mão única na prática** — descer depois contamina metade do controle |
| 🔴 | dobra a exposição antes de o operacional estar provado; se a taxa de casamento do CPF for ruim, a perda é sobre 1.908 pessoas e não 954 |

## 16. Recomendação

**Caminho B — 10% desde o início —, com duas condições.**

O argumento que decide: **o risco do caminho A não é menor, é adiado**. Os 954 do controle de 5%
já são pacientes encontrados e não navegados; a diferença entre A e B não é *"expor ou não"*, é
*quantos* e *com que poder*. E A carrega um custo que B não tem — o desenho escalonado — em troca
de uma reversibilidade que, como a §14 mostra, **não existe de verdade**.

**As duas condições, e nenhuma é opcional:**

1. 🔴 **Ética, LGPD e formalização ANTES do primeiro paciente.** Braço de controle é paciente
   encontrado e deliberadamente não navegado. É decisão institucional, não técnica, e precede a
   operação — o próprio relatório já a lista como pendente.
2. 🔴 **O desfecho declarado e o denominador do CPF respondido** antes de ligar. Sem desfecho não
   há poder calculável (o CV de 2,05 é suposição), e sem a taxa de casamento não se sabe quem o
   estudo perde de vista. São os itens 1 e 2 da §5, e **seguem abertos**.

🟡 **Se a alçada ética limitar a exposição inicial, 7,5% é o meio-termo defensável** — perde só
14% de precisão contra o 10%, e preserva a monotonicidade para subir depois.

📄 Verificação reexecutável: `_ferramentas/verifica-sorteio-randomizacao.py` — reimplementa a
fórmula, confere fração, determinismo, independência regional e **as duas direções da mudança de
corte**.
