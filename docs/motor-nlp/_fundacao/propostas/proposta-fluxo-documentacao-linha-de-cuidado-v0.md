# Proposta — fluxo de documentação de uma linha de cuidado

**Data:** 2026-08-25 · **Para:** PO, PMO, Head, time de plataforma e ciência de dados
**Objetivo:** propor o encadeamento de documentos que sustenta uma linha de cuidado, do pedido do
negócio à entrega — e **alinhar com o time** o que ainda não está definido.

> Esta proposta descreve **etapas, responsabilidades e portões**. Ela não define ferramenta,
> diretório nem formato de arquivo: isso é decisão do time e está listado na §5.

---

## 1. Por que — três casos deste mês

| o que aconteceu | o que faltava |
|---|---|
| Uma métrica foi calculada contra um conjunto de referência anotado **antes** de uma decisão de escopo ser fechada. A conclusão saiu errada e só foi percebida depois. | conjunto de referência com **data, autor e critério vigente** |
| O escopo de uma linha só foi equalizado com o negócio **depois** da primeira versão da régua estar pronta | registro congelado do que foi pedido, antes de implementar |
| Uma documentação técnica desatualizada levou a uma proposta de correção que teria desfeito uma régua já validada | documentação **do consumidor** versionada junto com a mudança |

Três causas diferentes, um padrão só: **o artefato existia no entendimento de alguém, e não num
registro com data e dono.**

Nenhum dos três foi falha de execução. Em todos, o trabalho técnico estava correto e a informação
que faltava era de processo.

---

## 2. O fluxo proposto — seis passos

| # | passo | artefato | quem produz | quem assina | portão para seguir |
|---|---|---|---|---|---|
| 1 | **Discovery** | briefing de negócio | ciência de dados conduz, PO organiza a agenda | PO + dono clínico | o negócio confirma: *"é isso que pedimos"* |
| 2 | **Medição e referência** | filtro de exames confirmado · volumetria · lote anotado | DS mede · dono clínico anota | dono clínico | volumetria cabe na capacidade de absorção declarada no briefing |
| 3 | **Especificação** | SPEC da linha de cuidado | ciência de dados | PO + dono clínico | decisões fechadas registradas, pontos em aberto **nomeados** |
| 4 | **Implementação** | configuração da régua | ciência de dados | code review da plataforma | métricas contra a referência do passo 2 |
| 5 | **Homologação** | relatório + lote homologado | negócio revisa | dono clínico | o negócio aprova o lote |
| 6 | **Entrega** | job em produção, com versão declarada | MLOps | — | — |

**O briefing congela; a especificação evolui.** O briefing registra o que foi pedido e não se
reescreve — se o pedido mudar, nasce uma nova versão. A SPEC acompanha as decisões ao longo da vida
da linha e é o documento de manutenção.

⚠️ **Não existe SPEC antes do passo 2.** Antes de medir há briefing e hipótese. Especificar sobre
hipótese é o que produz decisão de escopo tomada depois da anotação de referência — o primeiro caso
da §1.

---

## 3. Os dois vínculos que tornam o resultado rastreável

**Decisão ↔ implementação.** A especificação versiona **decisão**; a configuração versiona
**implementação**.

| o que mudou | especificação | versão da configuração |
|---|---|---|
| decisão de negócio (entra ou sai achado, muda limiar ou critério) | **sobe** | sobe |
| correção sem mudar decisão (sinônimo, expressão, defeito) | não muda | sobe |
| troca de plataforma sem mudar comportamento | não muda | sobe, com régua idêntica |

Uma especificação pode ter várias configurações ao longo do tempo; **cada configuração aponta para
uma especificação**.

**Referência ↔ decisão.** É o vínculo que hoje não existe, e o que custou o erro de métrica.

Todo lote anotado precisa carregar, além do veredito: **quem anotou, quando, e sob qual versão da
especificação**. Métrica calculada contra um lote anotado sob critério anterior ao vigente não é
métrica — é comparação com outra régua, e não avisa que está errada.

---

## 4. Os dois portões

**Portão técnico — automatizável.** No fluxo de publicação da biblioteca, é possível barrar
automaticamente a subida quando falta o registro que a mudança exige: nota de versão, especificação
do módulo alterado, ou atualização da documentação que o consumidor lê. Verificação mecânica, com
exceção declarada e auditável quando a regra não se aplica.

**Portão de negócio — rito, não automação.** O artefato é humano e não há como validar por script.
O que se pode exigir é a **ordem**: não se abre configuração sem especificação publicada, e não se
publica especificação sem lote de referência datado. Quem opera é o PO, na passagem de etapa.

A assimetria é proposital: automatizar o que é automatizável, e nomear responsável pelo resto — em
vez de supor que um checklist substitui a assinatura de alguém.

---

## 5. O que precisa ser definido com o time

Estes pontos **não** estão decididos, e a proposta não os prescreve. São o motivo desta conversa.

| # | o que definir | quem decide |
|---|---|---|
| 1 | **Quem assina o briefing pelo negócio**, por linha de cuidado. O PO organiza, mas o "é isso que pedimos" precisa de nome. Sem isso o briefing não congela nada. | PO + Head |
| 2 | **Onde os artefatos vivem** e como se versionam — repositório, wiki corporativa, ou o que o time já usa. Hoje cada frente resolve de um jeito. | time, com a plataforma |
| 3 | **Onde vive o lote de referência.** A proposta é que deixe de ser planilha e passe a ser tabela versionada, com autor, data e critério — mas o local e o dono são decisão conjunta. | ciência de dados + plataforma |
| 4 | **Formato mínimo** de briefing e de especificação. Há rascunhos que podem servir de ponto de partida, anexos a esta proposta, e que existem para ser criticados — não para serem adotados como estão. | time |
| 5 | **Se a próxima linha de cuidado entra por este fluxo.** É onde o custo de aplicar é menor e o aprendizado é maior. | PMO + PO |

---

## 6. O que esta proposta não faz

- **Não cria comitê nem etapa de aprovação nova.** Os seis passos já acontecem hoje; o que muda é
  passarem a deixar registro com data e dono.
- **Não retroage.** As linhas em produção não param para gerar documentação. Ganham o registro
  quando forem tocadas.
- **Não define ferramenta.** Diretório, formato e local são o item 2 da §5.
- **Não substitui** o code review nem o fluxo de publicação, que são do time de plataforma.
