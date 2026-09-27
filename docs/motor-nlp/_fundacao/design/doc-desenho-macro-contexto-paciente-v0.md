# Desenho macro — contexto do paciente no fluxo NLP

> **Status:** v0, macro e deliberadamente incompleto. Serve para alinhar **componentes,
> responsabilidades e fronteiras** antes de detalhar contratos. As definições seguem abertas.
>
> **Não substitui** `doc-design-cruzamento-por-paciente-v0.md`, que detalha o contrato **dentro da
> lib** (bloco de config, semântica, audit). Este documento é a camada acima: quem faz o quê.

---

## 1. O que muda, em uma frase

Hoje o motor responde *"este laudo é relevante?"*. Passa a responder também *"este paciente tem a
condição, e o que falta para fechar?"*.

Isso não é uma feature dentro do motor — é um **fluxo com mais de um passo e mais de um dono**.

---

## 2. Os componentes

| # | componente | responsabilidade | dono | existe hoje? |
|---|---|---|---|---|
| 1 | **Seleção** | monta o lote diário a partir da Gold | plataforma | ✅ |
| 2 | **Motor por laudo** | decide com o que está NO laudo; emite o desfecho | lib | ✅ |
| 3 | **Orquestrador de pendência** | busca, casa por chave, guarda e expira | plataforma | ❌ |
| 4 | **Avaliação por paciente** | dado o conjunto reunido, confirma ou refuta | lib | ❌ |
| 5 | **Estado paciente-condição** | guarda o que já se sabe, com proveniência e prazo | hub / plataforma | ❌ |
| 6 | **Elegibilidade** | vivo, convênio, região, já em acompanhamento | operação | ❌ |
| 7 | **Navegação** | contato, pedido de exame | linha de cuidado | ✅ (fora daqui) |

Os componentes **3, 4 e 5** são o que não existe. O 6 existe como dado, mas ninguém aplica.

### ⚠️ 3 e 4 são componentes distintos — a confusão entre eles é o erro mais fácil

"Resolvedor" soa como uma coisa só. São duas, com donos diferentes:

| operação | natureza | dono |
|---|---|---|
| buscar a evidência que falta | I/O | **plataforma** |
| casar por `(id_paciente, id_critério, janela)` | join mecânico | **plataforma** |
| guardar o estado e expirar por prazo | estado + data | **plataforma** |
| **dado o conjunto reunido, confirma ou refuta?** | **régua** | **lib** |

O casamento **não precisa de conhecimento clínico** — é chave contra chave. Mas *qual critério fecha
qual condição* é config de especialidade, e o **veredito** é régua.

Logo: o orquestrador é **plataforma**, e é um **orquestrador, não um decisor**. Ele reúne e chama a
lib para julgar. A lib segue sem fazer I/O.

---

## 3. O fluxo

```
                        ┌──────────────────┐
   lote diário ────────▶│ 2. MOTOR POR     │
   (seleção)            │    LAUDO         │
                        └────────┬─────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
      ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐
      │ PROMOVE       │  │ REFUTA        │  │ PENDENTE          │
      │ evidência     │  │ contraindica  │  │ indício, falta    │
      │ suficiente    │  │ ou exclui     │  │ evidência X       │
      └───────┬───────┘  └───────┬───────┘  └─────────┬─────────┘
              │                  │                    │
              │                  │                    ▼
              │                  │          ┌───────────────────┐
              │                  │          │ 3. RESOLVEDOR     │
              │                  │          │ busca X para ESTE │
              │                  │          │ paciente          │
              │                  │          └─────────┬─────────┘
              │                  │                    │
              │                  │       ┌────────────┼────────────┐
              │                  │       ▼            ▼            ▼
              │                  │   achou X      não achou    prazo venceu
              │                  │       │        (segue)          │
              │                  │       ▼                         ▼
              │                  │  ┌─────────────┐        ┌──────────────┐
              │                  │  │ 4. AVALIA   │        │ INCOMPLETO   │
              │                  │  │ POR PACIENTE│        │ "falta exame │
              │                  │  └──────┬──────┘        │  para fechar"│
              │                  │         │               └──────┬───────┘
              │                  │    promove/refuta              │
              ▼                  ▼         ▼                      ▼
      ╔═══════════════════════════════════════════════════════════════╗
      ║  5. ESTADO PACIENTE-CONDIÇÃO  (com proveniência e prazo)      ║
      ╚═══════════════════════════════┬═══════════════════════════════╝
                                      ▼
                          ┌───────────────────────┐
                          │ 6. ELEGIBILIDADE      │  vivo · convênio · região
                          │    (não muda o        │  já em acompanhamento
                          │     diagnóstico)      │
                          └───────────┬───────────┘
                                      ▼
                          ┌───────────────────────┐
                          │ 7. NAVEGAÇÃO          │  contato ou pedido de exame
                          └───────────────────────┘
```

**A inversão que sustenta o desenho:** o motor **declara o que falta** em vez de consumir
histórico às cegas. Medido no TI-RADS: carregar 90 dias para todo o lote = **5.624 exames**;
carregar só para quem tem pergunta em aberto = **324**. Dezessete vezes menos.

---

## 4. As três saídas, e por que a terceira é a mais valiosa

| desfecho | o que significa | ação |
|---|---|---|
| **promove** | evidência suficiente | encaminha |
| **refuta** | contraindicação ou evidência que derruba | não encaminha, e **registra o porquê** |
| **incompleto** | indício real, exame que fecharia **nunca foi feito** | **pedir o exame** |

O `incompleto` não é falha do pipeline — é lista de captação. E é o desfecho mais frequente:
a cobertura entre modalidades é de **~20%** (imagem↔sangue, medido nas duas direções). Ou seja,
para 4 em cada 5 pacientes com indício, não existe o exame que confirma.

Dentro do sangue a história é outra: **77%** têm 2+ exames. Lá o cruzamento é régua de verdade.

**Consequência de desenho:** confirmação entre modalidades **não pode ser requisito** — se a régua
exigir sangue para aprovar imagem, perde-se 79% da coorte. Entra como enriquecimento, ou não entra.

---

## 5. Fronteiras que não podem ser cruzadas

**Evidência clínica muda o diagnóstico. Elegibilidade muda se agimos.** São camadas diferentes,
com donos diferentes, e misturá-las custa caro:

- o mesmo laudo passaria a dar resultado diferente conforme a data em que roda (o paciente morreu,
  trocou de convênio, entrou em acompanhamento) — **fim da reprodutibilidade**;
- a base ouro deixaria de valer como gabarito;
- o harness golden não conseguiria mais comparar versões;
- a métrica clínica se moveria por motivo comercial, e o médico não teria como validar a régua.

Por isso **6 e 7 ficam fora do motor**. E região e convênio, as duas mais pedidas, **já saem na view
de export** — é filtro de consumo que ninguém ligou, não é feature a construir.

⚠️ A auditoria precisa distinguir os dois "nãos": não encaminhamos porque **não tem a condição** (2/4)
ou porque **não é elegível** (6). São conversas diferentes — a primeira com o clínico, a segunda com
a operação.

---

## 6. O que é DS e o que é MLOps

| | DS | MLOps | clínico | operação |
|---|---|---|---|---|
| régua, papéis, limiares | ✅ | | valida | |
| o que caracteriza pendência | ✅ | | valida | |
| montar lote com histórico | | ✅ | | |
| fila e prazo de resolução | define a política | implementa | | |
| persistir estado paciente-condição | define o contrato | implementa | | |
| elegibilidade | | | | ✅ |
| dedup de encaminhamento | | | | ✅ |

**Regra que resolve a maioria das dúvidas de fronteira:** a lib **não faz I/O**. Ela recebe dict
pronto e devolve decisão. Tudo que é buscar, guardar ou agendar é plataforma.

---

## 6-A. O orquestrador não monitora — não tem trigger próprio

Não há agendador, broker nem processo vigiando. São **três passos no fim do run diário que já
existe**:

```
lote diário → MOTOR (como hoje)
                ↓
         ORQUESTRADOR
           1. FECHA   join: audits de HOJE × pendências abertas
                      casou (paciente + critério)? → chama a lib para o veredito
           2. BUSCA   só para as pendências NOVAS deste run  → 324 exames, não 5.624
           3. EXPIRA  scan por data: prazo venceu → INCOMPLETO
```

**A "fila" é uma tabela com status**, não infraestrutura:
`id_paciente · condicao · criterio_esperado · janela · prazo · status`,
com `status ∈ {aberto, fechado_confirma, fechado_refuta, expirado}`.

Se o run não rodar num dia, nada quebra — no dia seguinte o join pega o acumulado. E o passo 1 é
**grátis**: aquele TSH ia ser processado no lote de qualquer forma; o orquestrador só lê o audit
que já foi produzido.

⚠️ **A busca para trás tem de ir na fonte, não na nossa tabela de entrada.** Se procurar só no que
já ingerimos, ela só acha o que as nossas keywords trouxeram — e a pendência expiraria como "falta
exame" com o exame existindo. Isso implica acesso de leitura por paciente **fora** do filtro da
especialidade.

---

## 7. Encaixe com o Clinical Data Hub

O hub é o componente 5 chegando como iniciativa da organização. **Não construir estrutura paralela**
de vínculo por paciente — mas também **não depender dele para entregar**, porque está no início.

⚠️ **Janela aberta agora:** um hub clínico costuma ser modelado como **fatos** (paciente, exame,
procedimento, resultado). O que este fluxo precisa guardar é **estado derivado**:

1. **evidência com papel** — indica / confirma / refuta, não só o valor do exame;
2. **pendência com validade** — o item da fila, com prazo;
3. **proveniência da inferência** — qual versão da régua concluiu o quê.

Se os três não entrarem no modelo enquanto ele é desenhado, viram retrofit caro — ou reconstruímos
a estrutura paralela que queríamos evitar.

### O que muda, componente a componente

| | hoje, sem hub | com hub |
|---|---|---|
| **busca** | varre a fonte por paciente | consulta o hub por paciente |
| **estado** | tabela nossa, por especialidade | estado derivado no hub |
| **medidas** | reprocessa o laudo histórico | **lê a medida já persistida** |
| acesso à fonte | provisionar por especialidade | não precisa |

O terceiro é o que mais pesa: **se o hub guardar as medidas com papel, a busca deixa de reprocessar
laudo.** A dúvida "reprocessar ou ler o persistido" desaparece — a resposta passa a ser *ler*.

### E o motor não é só consumidor — ele CONTRIBUI

Há um quarto item, de mão dupla: o hub também **recebe** a saída do motor. Cada run enriquece o
estado do paciente, e a decisão seguinte parte de mais contexto.

As colunas `fl_modelo_birads` e `vl_modelo_FIB` na tabela de pacientes mostram que esse caminho de
volta **já foi previsto** por alguém — estão vazias, mas a intenção estava no desenho.

Isso muda a natureza da conversa: não é *"precisamos que o hub nos dê X"*, é *"o motor produz
evidência que o hub deveria guardar"*.

### A decisão que não dá para adiar

**A pendência mora no hub ou no nosso schema?**

| | a favor |
|---|---|
| **no hub** | é estado de paciente, que é a razão de ser dele; outras especialidades terão a mesma necessidade; a navegação consome de lá |
| **no nosso** | o hub não está pronto — esperar bloqueia; e pendência é da nossa inferência, não fato clínico |

**Recomendação:** o contrato nasce **agnóstico de onde mora**. Começa no nosso schema e migra
quando o hub suportar estado derivado. Exige uma coisa só: a pendência ser identificada por
`(id_paciente, condição, critério)` e **não** por chave interna nossa — senão a migração vira
reescrita.

---

## 8. Faseamento sugerido

| fase | o que entrega | depende de |
|---|---|---|
| **0** | papel de **exclusão/refutação**, escopo laudo | nada — atende o pulmão V2 hoje |
| **1** | desfecho **PENDENTE** + contrato da demanda | fase 0 |
| **2** | **fila e política de prazo** | fase 1 · é onde nasce a navegação |
| **3** | avaliação por paciente (escopo sangue, janela 90d) | fase 2 |
| **4** | consulta ao hub substitui o fetch próprio | hub existir |

A fase 0 não depende de nada e já tem demanda escrita e parada. As fases 1–2 são a ponte, pequena.

---

## 9. Pontos em aberto — precisam de decisão antes de detalhar contrato

| # | pergunta | de quem depende |
|---|---|---|
| 1 | A saída passa a ser **por paciente-condição**, ou continua por exame com o estado agregado fora? | arquitetura / DS |
| 2 | De onde vem **"já em acompanhamento"**? Se for do nosso próprio histórico de encaminhamento, é **saída nova do produto**, não entrada | operação + DS |
| 3 | **Prontuário entra no escopo?** É categoria evidência clínica (resolveria o TSH sob levotiroxina), e é Research próprio — fonte nova, texto livre, LGPD | head / clínico |
| 4 | Quem aplica **elegibilidade** — consumidor ou plataforma? Se for o consumidor, sai do nosso caminho crítico | operação |
| 5 | **Prazo de pendência por condição** — 30 / 90 / 180 dias? Medido: 90d captura 136 dos 142 casos de T4; 180d acrescenta 22% de volume para ganhar 6 | clínico |
| 6 | O `incompleto` vira **pedido de exame**? Se sim, muda o produto e precisa entrar no desenho desde já | negócio |
| 7 | A **pendência mora no hub** ou no nosso schema? (§7 — recomendação: contrato agnóstico, começa no nosso) | arquitetura + hub |
| 8 | O orquestrador roda **no mesmo job** do motor ou separado? Dentro é mais simples; separado permite reprocessar a fila sem reprocessar laudo | MLOps |
| 9 | Quem define o **prazo** — clínico (por condição) ou operação (por capacidade da fila)? Provavelmente o menor dos dois, mas é preciso saber qual manda | clínico + operação |

---

## 10. Referências

- Contrato dentro da lib, inventário do que já existe e custo medido do histórico:
  `doc-design-cruzamento-por-paciente-v0.md`
- Régua e pendências clínicas do piloto: `../tireoide/spec-negocio-tireoide-v3.md`
- Demanda de exclusão já especificada e parada: `../pulmao/spec-negocio-transplante-pulmao-v1.md`
  (critérios V2 — PSAP/FAC com **exclusão de FEVE < 40%**)
