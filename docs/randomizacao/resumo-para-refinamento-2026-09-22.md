# Randomização — o que levar ao refinamento

> **Veredito: o método está validado e pode ser acoplado ao fluxo.**
> **5% de controle é suficiente para o desfecho declarado.**
> Uma condição operacional, e ela é de monitoria, não de bloqueio.

**Data:** 2026-09-22 · **Base:** `relatorio_v4.pdf` (backtest de 12 meses, Lucas) ·
`rededor-ai-lib`, `docs/features/randomizacao.md` (Diego) · verificação independente e medições
próprias.

---

## 1. O desfecho — definido pelo time em 22/09

> **Impacto dos algoritmos de captação sobre o retorno financeiro para a Rede D'Or**, comparando o
> retorno observado nos pacientes **encaminhados para a navegação** contra os **identificados e
> não enviados**.

✅ É um desfecho **contínuo** (receita por paciente), com braços bem definidos pelo próprio
mecanismo de alocação. **Fecha a lacuna que os dois documentos deixavam em aberto.**

---

## 2. O método está VALIDADO — verificado de forma independente

O sorteio foi **reimplementado do zero**, em Python puro, sobre **1.000.000 de CPFs**, sem usar a
biblioteca nem o notebook do estudo:

| afirmação | verificado |
|---|---|
| entrega 5% | **4,9907%** |
| **subir de 5% para 10% não realoca ninguém** | ✅ o conjunto de 5% é **subconjunto** do de 10% |
| determinismo — a mesma pessoa cai sempre no mesmo braço | ✅ |
| independência de atributo (dígito regional do CPF) | maior \|z\| = **2,13** em 10 regiões — dentro do acaso |

🟢 **Três decisões de desenho corretas:** hash em vez de `rand()` (que não sobrevive a mudança de
cluster nem a retry de task) · **monotonicidade do corte**, que permite subir o controle no meio do
estudo sem realocar ninguém · **um paciente em várias linhas cai no mesmo braço**, o que evita
contaminação entre linhas de cuidado.

**Não há razão para mexer no sorteio.**

---

## 3. 🟢 DECISÃO: 5% é suficiente — com a ressalva do que 10% compraria

**Menor diferença de receita média por paciente que o estudo detecta**, em % da média do controle.
Teste de duas amostras, bilateral, α 5%, 12 meses:

| controle | n no controle | **CV 1,5** | **CV 2,0** | **CV 3,0** |
|---|---|---|---|---|
| **5%** | 4.588 | **6,4%** | **8,5%** | **12,7%** |
| 10% | 9.177 | 4,6% | 6,2% | 9,2% |

*(CV = coeficiente de variação da receita por paciente. Receita hospitalar é assimétrica; 1,5 a 3,0
cobre o intervalo plausível.)*

### A conclusão

✅ **Com 5%, o estudo detecta uma diferença de ~8,5% na receita média por paciente.**
Se a navegação agrega valor, o efeito esperado é de **ordem de dezenas de pontos percentuais** —
um paciente navegado que converte em tratamento vale muito mais que 8,5% a mais. **5% detecta com
folga.**

⚠️ **O que 10% compraria:** sensibilidade de **8,5% para 6,2%** — um ganho de **27%**.
🔴 **Ao custo de 4.589 pacientes a mais encontrados e NÃO navegados.**

**Só vale se o efeito esperado estiver na faixa de 6% a 8,5%.** Se for maior, os 4.589 são custo
sem retorno — e a régua do projeto é explícita: *em rastreio, precisão comprada com paciente
perdido é regressão*.

### 🔴 O que realmente limita: a JANELA, não a fração

| janela | pacientes | no controle | detecta |
|---|---|---|---|
| 3 meses | 22.943 | 1.147 | **17,0%** |
| 6 meses | 45.886 | 2.294 | **12,0%** |
| **12 meses** | 91.773 | 4.588 | **8,5%** |

**Dobrar o tempo ganha mais que dobrar o controle, e não custa paciente nenhum.**
Se houver pressa por resultado, **é aqui que a conversa deve estar** — não em 5% × 10%.

---

## 4. ⚠️ A condição operacional — monitoria, não bloqueio

Medido sobre **243.279 pacientes** do pipeline (janela de 33 dias, todo o histórico disponível):

| | |
|---|---|
| **CPF começando com ZERO** | **12,8%** dos válidos |
| falham o dígito verificador | 0,008% |
| CPF de teste (dígitos iguais) | 0 |

🔴 **Os 12,8% com zero à esquerda são o risco concreto.** É exatamente o que a documentação da
biblioteca chama de *"erro número um em produção"*: o ETL perde o zero, o *join* não casa, e o
sintoma é **menos gente no controle do que o esperado** — sem erro e sem alarme.

**A condição, e é barata:** o acoplamento **monitora a taxa de casamento desde o primeiro dia**.
Se a fração efetiva de controle cair abaixo de 5%, é chave perdida, não acaso.

ℹ️ Medi também 45,7% de pacientes sem CPF recuperável na fonte que consigo consultar, **mas não
consegui separar** "não tem CPF" de "a fonte não cobre". Não trato como bloqueio: **quem rodou o
backtest partiu de 91.773 pacientes com CPF** e tem esse denominador — vale confirmar com eles.

---

## 5. Três divergências de fechamento

| | |
|---|---|
| **chave** | o relatório usa `num_cpf_paciente`; a biblioteca tem `coluna_chave` padrão `cpf`. **Alinhar antes de acoplar** — é o que decide a taxa de casamento |
| **branch** | o relatório atribui o commit `2e201f4` à `main`; o arquivo está na `hml` |
| **a tabela** | `massa_cpf_randomizada` não foi encontrada nos catálogos que acesso. ⚠️ Ausência sob permissão não é inexistência — mas é o centro do desenho; confirmar onde está |

---

## 6. Respostas diretas

| pergunta | resposta |
|---|---|
| **O método está validado?** | ✅ **Sim.** Verificado de forma independente, passa nas quatro afirmações |
| **O Diego pode acoplar ao fluxo?** | ✅ **Sim**, com a monitoria da taxa de casamento desde o dia 1, e a chave alinhada antes |
| **5% ou 10%?** | ✅ **5%.** Detecta 8,5% de diferença na receita média; 10% detecta 6,2% e custa 4.589 pacientes não navegados |
| **O que ainda falta?** | 🔴 **Ética, LGPD e formalização** — braço de controle é paciente **encontrado e deliberadamente não navegado**. Precede a operação |

---

📄 Evidência completa: `parecer-estudo-de-validacao-2026-09-22.md`
🔧 Reexecutáveis: `_ferramentas/verifica-sorteio-randomizacao.py` · `poder-randomizacao.py` ·
`poder-financeiro-randomizacao.py`
