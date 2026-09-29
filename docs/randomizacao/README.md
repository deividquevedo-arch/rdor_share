# Randomização

> **Frente nova, aberta em 2026-09-22.** Este índice existe para o trabalho não nascer espalhado —
> o conteúdo ainda não foi produzido.

## O que é esta frente

Randomização de pacientes na captação: **quem entra na lista que vai à mesa de navegação, e por
qual braço**. Toca três coisas ao mesmo tempo — o algoritmo que seleciona, a operação que recebe, e
a mensuração do efeito.

⚠️ **Não confundir com o `gold_filter`.** Aquele decide **qual exame chega ao motor** e é parâmetro
de configuração da especialidade. A randomização decide **o que acontece depois da decisão do
motor**, e é desenho de estudo.

## Material avaliado

📄 **`resumo-para-refinamento-2026-09-22.md`** — **uma página, para levar ao refinamento.**
As quatro decisões com o número que as sustenta.


📄 **`parecer-estudo-de-validacao-2026-09-22.md`** — avaliação crítica do backtest de 12 meses
(`relatorio_v4.pdf`) e da doc da biblioteca (`rededor-ai-lib`, `docs/features/randomizacao.md`).
**Conclusão: o sorteio está pronto, o estudo não.** O mecanismo foi verificado de forma
independente e passa; o que falta é a taxa de casamento do CPF, o desfecho e o cálculo de poder.

🔧 Script reexecutável fora do git: `_ferramentas/verifica-sorteio-randomizacao.py` — reimplementa
a fórmula em Python puro e testa fração, determinismo, monotonicidade e independência regional.

## Cards

| card | título | estado |
|---|---|---|
| `185082` | *[Captação] Randomização de pacientes - [Em refinamento com stakeholder]* | Novo |
| `307254` | *[Captação] Randomização algoritmos* | Planejado |
| `303629` | *[Financeiro] Refinamento Randomização e mensuração de conversão financeira* | Em Refinamento |
| `303791` | *Plano de Migração algoritmos final (randomização e central captação)* | Planejado |

ℹ️ **O `303791` é o card de junção** — ele carrega também a passada única da hepatologia, que é
frente do motor. Ao trabalhar aqui, não puxar a metade de NLP junto sem decisão explícita.

## 🔴 O que precisa ser respondido ANTES de seguir

**Duas das quatro perguntas abertas foram respondidas pelo material avaliado:**

- ✅ **A unidade de randomização é o PACIENTE**, por CPF — e um paciente em várias linhas cai no
  mesmo braço, o que evita contaminação entre linhas de cuidado.
- ✅ **O mecanismo é determinístico e auditável**, e subir o controle de 5% para 10% **não realoca
  ninguém** — verificado.

**Seguem abertas, e agora com o motivo escrito:**

1. 🟡 **Qual é o desfecho?** O **poder foi calculado** (adendo 2 do parecer) e sustenta a escolha
   entre 5% e 10%: **5% exige efeito 37,6% maior**, e dobrar de 10% para 20% só melhora 25%.
   ⚠️ **Mas o desfecho segue sem declaração**, então o efeito detectável em valor absoluto
   (os 14% / 19% que circularam) depende de um CV suposto de ≈ 2,05. **A comparação entre as
   opções não depende disso; o número absoluto depende.**
2. 🔴 **Qual a taxa de casamento do CPF, e quem fica de fora?** É o único viés que o hash não
   protege, porque acontece **antes** dele.
3. 🔴 **Ética, LGPD e formalização** — o próprio relatório lista como pendente, e isso **precede** a
   operação.
4. ⚠️ **Três divergências** entre o relatório e a doc da biblioteca — chave, branch e o elo com a
   tabela de massa. Ver §4 do parecer.

## O que NÃO entra aqui

- Régua clínica de especialidade — vive em `docs/motor-nlp/<especialidade>/`.
- Decisão de versão da lib, pin e contrato — `docs/motor-nlp/ESTADO.md`.
- Filtro de entrada (`gold_filter`) — é config da especialidade.

## Estado — 2026-09-24

✅ **Parecer fechado**, com dois adendos. O mecanismo está validado e **não há nada a corrigir
nele**; o que falta é tudo em volta.

🔴 **Recomendação: começar já em 10%**, com duas condições que não são opcionais — ética/LGPD/
formalização antes do primeiro paciente, e o desfecho declarado mais o denominador do CPF
respondido antes de ligar.

**A assimetria que sustenta a recomendação:** subir de 5% para 10% **não realoca ninguém**; descer
de 10% para 5% devolve à navegação metade do controle, que **já passou um período sem ser
navegado**. Tecnicamente as duas direções são um número no `WHERE` — **voltar não é impossível,
voltar não desfaz**. Ver §14 do parecer.

🟡 Se a alçada ética limitar a exposição inicial, **7,5%** perde só 14% de precisão contra o 10% e
preserva a monotonicidade.

O estado desta frente vive **aqui**, não no `ESTADO.md` do motor — são frentes distintas e
misturá-las foi o que já tornou aquele documento difícil de ler.
