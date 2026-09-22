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

## Cards

| card | título | estado |
|---|---|---|
| `185082` | *[Captação] Randomização de pacientes - [Em refinamento com stakeholder]* | Novo |
| `307254` | *[Captação] Randomização algoritmos* | Planejado |
| `303629` | *[Financeiro] Refinamento Randomização e mensuração de conversão financeira* | Em Refinamento |
| `303791` | *Plano de Migração algoritmos final (randomização e central captação)* | Planejado |

ℹ️ **O `303791` é o card de junção** — ele carrega também a passada única da hepatologia, que é
frente do motor. Ao trabalhar aqui, não puxar a metade de NLP junto sem decisão explícita.

## 🔴 O que precisa ser respondido ANTES de desenhar

1. **Qual é a pergunta do estudo?** Medir o efeito do algoritmo sobre desfecho, sobre conversão
   financeira, ou sobre carga operacional — são desenhos diferentes e amostras diferentes.
2. **A unidade de randomização é o paciente, o exame ou a unidade hospitalar?** Randomizar por
   paciente com a lista chegando por unidade produz contaminação.
3. **Existe braço de controle, e ele é ético aqui?** Em rastreio, não entregar um achado encontrado
   tem custo clínico — isso precisa estar escrito e aprovado, não assumido.
4. **Quem mede, e contra qual base?** A base ouro não tem lugar oficial (dívida registrada no
   `motor-nlp/ESTADO.md`), e sem isso a mensuração fica sem denominador confiável.

## O que NÃO entra aqui

- Régua clínica de especialidade — vive em `docs/motor-nlp/<especialidade>/`.
- Decisão de versão da lib, pin e contrato — `docs/motor-nlp/ESTADO.md`.
- Filtro de entrada (`gold_filter`) — é config da especialidade.

## Estado

Nada produzido. Quando houver, o estado desta frente vive **aqui**, não no `ESTADO.md` do motor —
são frentes distintas e misturá-las foi o que já tornou aquele documento difícil de ler.
