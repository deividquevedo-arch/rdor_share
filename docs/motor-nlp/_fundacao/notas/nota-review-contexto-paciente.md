# Contexto do paciente no motor NLP — nota para a review

> Uma página. O desenho completo está em `doc-desenho-macro-contexto-paciente-v0.md` e na aba
> **"Contexto do Paciente — Fluxo Macro"** do drawio.

---

## O problema, em uma linha

O motor decide **por laudo**. A linha de cuidado precisa saber **por paciente** — e boa parte da
decisão clínica depende de exames que estão em laudos diferentes.

Medido no TI-RADS: de **142 pacientes com T4 livre elevado, 95 tinham TSH normal** — que
clinicamente refuta o achado. Hoje os 95 são encaminhados assim mesmo, porque o motor nunca vê os
dois juntos. Projetado: **~450 envios/mês** que poderiam ser dispensados.

---

## A proposta

O motor **declara o que falta** em vez de consultar a base. Três desfechos por laudo:

```
PROMOVE     evidência suficiente          → encaminha
REFUTA      contraindicação ou exclusão   → não encaminha, registra o porquê
PENDENTE    indício + falta evidência X   → entra na fila
```

**Por que declarar em vez de consultar:** se o motor for ao banco, o mesmo laudo passa a dar
resultado diferente conforme a data em que roda — e caem juntos a base ouro, o harness de
regressão e a validação clínica. É a mesma razão pela qual elegibilidade fica fora da régua.

E há um ganho medido: buscar histórico só para quem tem pergunta em aberto custa **324 exames**
contra **5.624** se carregar para todo o lote. Dezessete vezes menos.

---

## São dois componentes novos, com donos diferentes

A confusão mais fácil é tratar como um só:

| operação | natureza | dono |
|---|---|---|
| buscar · casar por chave · guardar · expirar | mecânico, sem conhecimento clínico | **plataforma** |
| dado o conjunto reunido, confirma ou refuta? | régua | **lib** |

O orquestrador **não decide** — reúne e chama a lib. A lib segue sem fazer I/O.

**Não há trigger próprio nem processo vigiando.** São três passos no fim do run diário que já
existe: fecha (join dos audits de hoje com as pendências abertas), busca (só para as pendências
novas) e expira (scan por data). A "fila" é uma tabela com status, não infraestrutura.

---

## O terceiro desfecho é o de maior valor — e é maioria

Cobertura medida entre modalidades: **~20%** (imagem↔sangue, nas duas direções). Ou seja, para 4 em
cada 5 pacientes com indício, o exame que confirmaria **não existe**.

Quando o prazo vence sem a evidência, o desfecho não é "não relevante" — é **"falta o exame que
fecha"**. Isso é lista de captação, não falha de pipeline. É o único dos três desfechos que gera
receita em vez de custo.

*(Dentro do sangue a cobertura é 77% — lá o cruzamento é régua de verdade, não enriquecimento.)*

---

## O que precisamos do Clinical Data Hub

O hub é exatamente a peça de estado que falta. **Não vamos construir estrutura paralela.** Mas há
uma janela que se fecha: um hub clínico costuma ser modelado como **fatos** (paciente, exame,
resultado), e o que este fluxo precisa guardar é **estado derivado**:

1. **evidência com papel** — indica / confirma / refuta, não só o valor do exame
2. **pendência com validade** — o item da fila, com prazo
3. **proveniência da inferência** — qual versão da régua concluiu o quê

Se os três não entrarem enquanto o modelo é desenhado, viram retrofit caro.

**E é mão dupla.** O hub também recebe a saída do motor — cada run enriquece o estado do paciente.
As colunas `fl_modelo_birads` e `vl_modelo_FIB` na tabela de pacientes mostram que esse caminho de
volta já foi previsto; estão vazias, mas a intenção estava no desenho.

Não é *"precisamos que o hub nos dê X"* — é *"o motor produz evidência que o hub deveria guardar"*.

---

## O que trazemos para decidir

| # | decisão | de quem |
|---|---|---|
| 1 | A saída passa a ser **por paciente-condição**, ou continua por exame com o estado agregado fora? **É a mais cara de reverter** — afeta lib, persister, view e consumo | arquitetura |
| 2 | A **pendência mora no hub** ou no nosso schema? *Recomendação: contrato agnóstico — começa no nosso, migra depois. Exige identificar por `(id_paciente, condição, critério)`, não por chave interna* | arquitetura + hub |
| 3 | O orquestrador roda **no mesmo job** do motor ou separado? | MLOps |
| 4 | A busca vai na **fonte** (não na nossa tabela de entrada, senão só acha o que nossas keywords trouxeram). Isso exige leitura por paciente fora do filtro da especialidade — **provisionar ou esperar o hub?** | MLOps + hub |
| 5 | De onde vem **"já em acompanhamento"**? Se vier do nosso próprio histórico de encaminhamento, é **saída nova do produto**, não entrada | operação |
| 6 | O `incompleto` vira **pedido de exame**? Se sim, deixa de ser classificador e passa a gerar ação | negócio |

---

## O que NÃO estamos pedindo agora

- **Prontuário** — é evidência clínica e resolveria o maior falso-positivo que temos (TSH suprimido
  por levotiroxina), mas é fonte nova, texto livre e LGPD própria. Merece Research separado.
- **Confirmação entre modalidades como régua** — com 20% de cobertura, exigir sangue para aprovar
  imagem custaria 79% da coorte. Entra como enriquecimento ou não entra.
- **Elegibilidade** (vivo, convênio, região) — fica fora do motor por desenho, e região e convênio
  **já saem na view de export**. É filtro de consumo que ninguém ligou, não feature a construir.

---

## Faseamento

| fase | entrega | depende de |
|---|---|---|
| **0** | papel de **exclusão/refutação**, escopo laudo | ⭐ nada — atende o transplante de pulmão V2, que já está especificado e parado |
| 1 | desfecho **PENDENTE** + contrato da demanda | fase 0 |
| 2 | orquestrador + política de prazo | fase 1 · é onde nasce a navegação |
| 3 | avaliação por paciente (sangue, 90 dias) | fase 2 · aceite medido: 95 de 142 refutados |
| 4 | hub substitui o fetch e passa a receber a saída | hub existir |

A fase 0 não depende de nenhuma decisão desta lista e pode começar já.
