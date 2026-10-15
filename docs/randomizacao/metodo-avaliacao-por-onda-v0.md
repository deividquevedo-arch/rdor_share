---
tags: [randomizacao, metodo, ondas, captacao]
---

# Método de avaliação e validação da randomização, por onda

> v0 — 15/10/2026. Base para o cronograma em desenho pelo time de leads. Relatório gerado por
> `relatorio-onda/gerar_relatorio_onda.py`; exemplos em `relatorios/`.
> Ligações: [[proposta-conciliacao-303629-v0]] · [[relatorio-dinamico-e-medicao-v0]].

## 1. O que se avalia em cada etapa

| etapa do cronograma | DoD | o que se avalia | quem |
|---|---|---|---|
| **DEV** | PR para HML | job da linha com as tasks de randomização e relatório; linha ativa na config da `hml` | DS da linha |
| **HML SHADOW** (10 dias úteis) | dados acumulados para gerar o relatório | operação diária, integridade da randomização, volume e carga — **sem envio ao negócio** | Tech Lead DS |
| **RELATÓRIO E IMPACTO** | impacto informado ao negócio | relatório da onda: veredito dos critérios, volume por linha, pacientes retidos no controle, carga de navegação por regional | negócio da Captação |
| **DE ACORDO** | de acordo do Targa | aceite de que a linha passa a reter a fração de controle | Targa |
| **PRD** | linha em produção com envios já randomizados | o mesmo relatório, em PRD, na primeira semana | DS da linha + Tech Leads |

**"Impacto" nesta etapa é operacional, não financeiro.** O relatório informa **o que muda para o
negócio** — quantos pacientes por dia seguem para a navegação, quantos ficam no controle, em que
regionais.

### As três fases de cada onda

| fase | quando | o que entrega |
|---|---|---|
| **1. Validação** | HML shadow → relatório → de acordo | relatório operacional da onda (este método, §2–§3) |
| **2. Monitoria e observabilidade** | contínua, **desde o 1º dia em PRD** | checagens diárias, alerta, relatório semanal (§6) |
| **3. Uplift (impacto financeiro)** | **depois** do de acordo do Targa e com a linha em PRD | conversão e valor por grupo, por onda agregada ([[relatorio-dinamico-e-medicao-v0]] §4) |

O uplift só começa com a linha em PRD e aceita: é ali que a navegação acontece de fato e que o
grupo controle passa a existir para o negócio. Em shadow não há navegação a medir.

## 2. Critérios de aceite (veredito automático do relatório)

| # | critério | regra | escopo |
|---|---|---|---|
| C1 | **execução** | registro em ≥ 90% dos dias úteis da janela, nenhuma execução com falha | por linha |
| C2 | **randomização ativa** | pelo menos um paciente randomizado no período | por linha |
| C3 | **sem CPF** | ≤ 2% dos elegíveis sem CPF | por linha |
| C4 | **controle enviado** | zero | onda |
| C5 | **grupo único** | nenhum CPF em dois grupos | onda |
| C6 | **aderência à base** | grupo registrado = grupo da base pré-gerada | onda |
| C7 | **proporção de controle** | \|z\| ≤ 3 contra a fração vigente | **onda** |

Linha apta = C1–C3 OK e a onda com C4–C7 OK. Os limites ficam no início do gerador; mudar lá
muda o veredito de todas as ondas.

**Por que a proporção é da onda e não da linha:** em 10 dias úteis a 5%, o número esperado de
controles por linha é pequeno demais para leitura isolada.

| onda 2 — linha | pacientes relevantes/dia (prd, 13–15/10) | controles esperados em 10 dias úteis (teto, 5%) |
|---|---|---|
| Doenças biliares | 127 | ~63 |
| DII | 14 | ~7 |
| Câncer de cólon | 12 | ~6 |
| Neuroimunologia | 6 | ~3 |
| Câncer de próstata | 4 | ~2 |
| **Onda 2** | **~163** | **~81** |

Teto porque o sorteio se aplica aos **elegíveis ao envio**, que são menos que os relevantes.

## 3. O relatório

Uma execução por onda e ambiente:

```
python relatorio-onda/gerar_relatorio_onda.py --onda 2 --ambiente hml \
  --linhas doenca_inflamatoria_intestinal,cancer_colon,doencas_biliares,neuroimunologia,pirads \
  --inicio 2026-10-26 --fim 2026-11-06
```

Seções, todas com números agregados:

1. **Veredito** — os critérios C1–C7 com resultado e status.
2. **Resumo por linha, em pacientes** — analisados pelo algoritmo, relevantes, randomizados,
   navegação, controle, % controle, navegação por dia útil, controles por mês (projeção).
3. **Navegação por regional** — pacientes enviados por UF, para dimensionar a carga das navegadoras.
4. **Contaminação** — randomizados também relevantes em outra linha.
5. **O que o relatório não mede** — impacto financeiro e acerto clínico.

Para o e-mail de de acordo, o resumo por linha e o veredito bastam; a apresentação usa também a
página *Visão geral* do painel.

**Primeiro uso — onda 1:**

| ambiente | período | veredito | randomizados | controle |
|---|---|---|---|---|
| PRD | 12–15/10 | ✅ apta | 200 | 9 (4,5%) |
| HML | 30/09–14/10 | ⚠️ pendências: dias sem registro e falha em 08/10 | 345 | 12 (3,5%) |

## 4. Pré-requisitos para uma linha entrar em HML shadow

Checklist do PR da linha (onda 2: as cinco linhas ainda **não** cumprem os dois primeiros itens).

- [ ] `jobs/definicoes/<linha>-batch.json` com `randomizacao_<linha>` entre `motor_e2e` e
      `onedrive`, e `relatorio_<linha>` ao final (`run_if: ALL_DONE`).
      Hoje: DII e cólon têm só o relatório; biliares, neuro e próstata não têm nenhuma das duas.
- [ ] `config/randomization/ntb_ia_randomizacao_config.py` na branch `hml` com
      `{"ativo": True, "dt_inicio": "<1º dia do shadow>"}`. **A config é global**: só chega a PRD no
      merge para a `main` — que deve coincidir com o de acordo.
- [ ] `onedrive_<linha>` com `"disabled": "${disabled?}"` — já presente nas linhas que estão em
      prd; garante que o shadow em hml não envia ao negócio.
- [ ] job da linha **despausado em hml** durante os 10 dias (`jobs/ambientes/hml.json` traz todas
      as linhas como `PAUSED`). Alçada do administrador da plataforma.
- [ ] janela definida (início e fim) e registrada no card da linha.

## 5. Pontos para a agenda

1. ✅ **Decidido em 15/10: a onda 1 já virou.** Reumatologia e TI-RADS estão randomizadas em PRD
   desde 12/10, com o envio ao negócio saindo só para a navegação. O relatório de 19/10 sai de
   **PRD**, janela 12–16/10:
   `python relatorio-onda/gerar_relatorio_onda.py --onda 1 --ambiente prd --linhas reumatologia,tirads --inicio 2026-10-12 --fim 2026-10-16`
2. **HML e PRD não têm o mesmo volume.** Em 05–14/10, o TI-RADS teve 77 elegíveis/dia em hml e 41
   em prd. O relatório de shadow descreve hml; a projeção para o negócio deve usar prd quando a
   linha já roda lá — é o caso das cinco linhas da onda 2.
3. **Fração de controle:** 5% na onda 1. Subir para 10% só aumenta o controle, nunca troca alguém
   de grupo; descer não desfaz. Decidir antes de a onda 2 entrar em PRD.
4. **Contaminação diminui com as ondas:** o grupo é do CPF e vale em todas as linhas randomizadas;
   resta apenas a sobreposição com linhas ainda não randomizadas.

## 6. Monitoria e observabilidade (fase 2)

Começa no primeiro dia de cada onda em PRD e não termina. Entra no planejamento como entrega
própria, com dono e prazo.

| componente | o que faz | base já existente | o que falta |
|---|---|---|---|
| **Checagem diária** | execução, falha, conservação, log × relatório, controle enviado — por linha e dia útil | consulta `relatorio-sql/06_atencao_diaria.sql`, validada em prd e com controle positivo em hml | agendar em dia útil após 08:00 |
| **Alerta** | avisa quando houver dia-linha em ALERTA ou integridade ≠ 0 | consultas `06` e `03` | alerta SQL do Databricks com destinatários definidos |
| **Painel** | visão geral (pacientes por grupo) + primeira linha de atenção | rascunho *Randomização — primeira linha de atenção* | publicar com o filtro fixo em PRD; definir quem acessa |
| **Relatório semanal** | o relatório da onda, rodado sobre a última semana | `gerar_relatorio_onda.py --ambiente prd` | agendar ou incluir no rito semanal |
| **Proporção acumulada** | fração de controle por onda contra a base | consulta `07` | — (vai no painel e no relatório) |
| **Contaminação** | randomizados relevantes em linha não randomizada | consulta `04` | acompanhar a queda a cada onda |
| **Deriva de volume** | elegíveis/dia fora da média recente por linha | relatório já grava `qt_exportados` por dia | regra de alerta (ex.: fora de ±3 desvios da média de 4 semanas) |

**Ao entrar uma nova onda em PRD:** incluir as linhas no filtro do painel e no relatório semanal; a
checagem diária as pega sozinha, porque parte das linhas com registro no log.

**Dono sugerido:** Tech Lead DS para a definição e a leitura; administrador da plataforma para
agendamento, alerta e acesso ao painel publicado (POP-IA-08).

## 7. Uplift — pré-condições (fase 3)

Antes de rodar a primeira medição de uma onda:

- [ ] linhas da onda em PRD e com de acordo registrado;
- [ ] desfecho declarado por escrito (conversão, valor faturado, valor no produto da linha) e
      janelas pré-registradas (30/90/180 dias);
- [ ] chave de cruzamento confirmada — `num_cpf_paciente_d` em `vw_mov_fato_consumo` casou 85,5%
      dos CPFs randomizados em 15/10;
- [ ] lista de unidades com navegadora ativa, para o corte por unidade;
- [ ] período de acúmulo suficiente para o poder da onda — com 5%, cerca do dobro do tempo que
      com 10% para o mesmo efeito detectável.

## 8. Calendário aplicado ao método (proposta, datas em ajuste)

| marco | onda 1 | onda 2 | onda 3 |
|---|---|---|---|
| PR para HML | — (já em PRD) | 16–23/10 | 26/10–02/11 |
| HML shadow | — | 26/10–06/11 | 03–13/11 |
| relatório | 19/10 — **PRD**, 12–16/10 (decidido 15/10) | 05/11 → rodar o gerador com fim em 06/11 | 16/11 |
| de acordo | 23/10 | 11/11 | 19/11 |
| PRD | ✅ desde 12/10 | 17/11 | 24/11 |
| relatório de 1ª semana em PRD | 19/10 | 24/11 | 01/12 |
