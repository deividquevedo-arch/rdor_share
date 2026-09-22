# Parecer — o estudo de validação da randomização

> **O mecanismo está certo e foi verificado de forma independente. O que falta não é o sorteio —
> é o estudo em volta dele.**

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
