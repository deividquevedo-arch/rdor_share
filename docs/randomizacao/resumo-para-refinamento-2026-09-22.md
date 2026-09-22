# Randomização — o que levar ao refinamento

> **Uma página.** O sorteio está pronto e verificado. O estudo em volta dele, não.
> Quatro decisões a tomar, todas com o número que as sustenta.

**Data:** 2026-09-22 · **Base:** `relatorio_v4.pdf` (backtest de 12 meses) · `rededor-ai-lib`,
`docs/features/randomizacao.md` · verificação independente e medições próprias.

---

## 1. O que está PRONTO e não precisa de discussão

O mecanismo foi **reimplementado do zero** e testado contra 1.000.000 de CPFs, sem usar a
biblioteca nem o notebook do estudo:

| afirmação do estudo | verificado |
|---|---|
| entrega 5% | **4,9907%** ✅ |
| **subir de 5% para 10% não realoca ninguém** | ✅ o conjunto de 5% é subconjunto do de 10% |
| determinismo — a mesma pessoa cai sempre no mesmo braço | ✅ |
| independência de atributo (dígito regional) | maior \|z\| = **2,13** em 10 regiões — dentro do acaso |

🟢 Três decisões de desenho corretas: **hash em vez de `rand()`** (que não sobrevive a mudança de
cluster), **monotonicidade do corte** (dá para subir o controle no meio do estudo), e **um paciente
em várias linhas cai no mesmo braço** (evita contaminação).

**Não há razão para mexer no sorteio.**

---

## 2. 🔴 A crítica central, em uma frase

**O estudo valida o ALOCADOR, não o ESTUDO.**

O requisito testado — *"os 5% se replicam em qualquer estratificação"* — é **verdadeiro por
construção**: o SHA-256 torna a alocação independente de qualquer atributo. Testar 31 recortes é
testar que o SHA-256 funciona. **Ele funciona, e o teste não alcança as falhas que importam.**

---

## 3. As quatro decisões

### 🔴 DECISÃO 1 — Qual é o desfecho?

**Não está declarado em nenhum dos dois documentos.** Sem desfecho definido **antes** da análise,
escolher depois o que medir é grau de liberdade.

**Quem decide:** negócio + clínica. **Bloqueia:** tudo o mais.

---

### 🔴 DECISÃO 2 — 5% ou 10%?

**É pergunta de poder, e agora tem número.** Menor diferença detectável, α 5%, poder 80%:

| controle | base 5% | base 20% | base 50% |
|---|---|---|---|
| **5%** (4.588) | 0,93 pp | 1,70 pp | 2,12 pp |
| **10%** (9.177) | **0,68 pp** | **1,24 pp** | **1,54 pp** |

**Dobrar compra 27% de sensibilidade** — constante nas três bases.
🔴 **E custa 4.589 pacientes a mais encontrados e NÃO navegados.**

**A pergunta que decide:** *existe efeito clinicamente relevante entre 0,68 e 0,93 pp?*
Se o efeito esperado for **maior que 1 pp, 5% já detecta** — e os 4.589 são custo sem retorno.

⚠️ A régua do projeto: *em rastreio, precisão comprada com paciente perdido é regressão*.
**10% precisa ser justificado por poder, não adotado por simetria.**

---

### 🔴 DECISÃO 3 — Quem fica de fora por CPF, e isso enviesa?

Medido sobre **243.279 pacientes** do pipeline novo (janela de 33 dias — é todo o histórico que a
plataforma tem):

| | |
|---|---|
| sem CPF recuperável | **45,7%** |
| **CPF começando com ZERO** | **12,8%** dos válidos |
| falham dígito verificador | 0,008% |

🔴 **Os 12,8% com zero à esquerda são acionáveis hoje.** É exatamente o caso que a documentação da
biblioteca chama de *"erro número um em produção"*: o ETL perde o zero, o *join* não casa, e o
sintoma é **menos gente no controle do que o esperado**.

⚠️ **Os 45,7% NÃO estão estabelecidos como "sem CPF"** — podem ser cobertura da fonte consultada.
Rodei os controles: a chave do *join* está correta (0 falhas em 188.612), e a fonte corporativa tem
**32,4% dos seus próprios pacientes sem CPF**, então falta de CPF **é fenômeno real**. Não consegui
separar as duas hipóteses com o acesso que tenho.

🔴 **E a conclusão é a MESMA nas duas:**
- se é ausência real → ~45% não entram em braço nenhum, **variando de 21% a 80% por unidade**;
- se é cobertura → **não se sabe** a qualidade de CPF da população.

**Em ambas, a premissa central do estudo segue não verificada.**

✅ **O que pedir, e é uma linha:** quem rodou o backtest partiu de **91.773 pacientes** e precisou
de CPF para eles. **Quantos ficaram de fora, e de quais unidades?** O denominador existe do lado
de quem mediu.

---

### 🔴 DECISÃO 4 — Ética, LGPD e formalização

O próprio relatório lista como pendente. **Precede a operação, não a sucede.** Braço de controle é
paciente **encontrado e deliberadamente não navegado** — isso exige aprovação formal escrita.

---

## 4. Três divergências a fechar (rápidas)

| | |
|---|---|
| **chave** | o relatório usa `num_cpf_paciente`; a biblioteca tem `coluna_chave` padrão `cpf` |
| **branch** | o relatório atribui o commit `2e201f4` à `main`; o arquivo está na `hml` |
| **a tabela** | `massa_cpf_randomizada` **não foi encontrada** nos catálogos acessíveis. ⚠️ Ausência sob permissão não é inexistência — mas ela é o centro do desenho, e vale confirmar onde está |

---

## 5. Resumo de uma linha

> **O sorteio está pronto. Faltam o desfecho, o poder, o denominador do CPF e a formalização —
> e nenhum deles se resolve com mais teste de estratificação.**

📄 Detalhe e evidência completa: `parecer-estudo-de-validacao-2026-09-22.md`.
🔧 Scripts reexecutáveis: `_ferramentas/verifica-sorteio-randomizacao.py` e `.../poder.py`.
