---
tags: [randomizacao, uplift, janela, impacto-financeiro]
---

# Quanto tempo esperar e onde está o dinheiro — dados reais

> v1 — 22/10/2026. Complementa [[poder-estatistico-5-vs-10-v1]]. Pergunta: *por que 30 dias? qual janela captura o
> efeito da navegação e o impacto financeiro, que é o que o negócio quer ver?*

## Resposta curta

1. **A janela não é questão de confiança** — é o tempo que a navegação leva para gerar ação. A confiança vem do
   número de pacientes.
2. **A navegação age rápido:** primeiro contato em **5 dias** (mediana) e consulta em até **15 dias** para 3 em cada 4.
   Um indicador de **consulta/agendamento em 14–21 dias** é viável e sai antes de qualquer número financeiro.
3. **O dinheiro chega mais devagar e depende da linha:** na Reumatologia, a infusão sai em **31 dias** (mediana) e 90%
   em até **79 dias** — só **37%** do consumo de 180 dias acontece nos primeiros 30. Na Tireoide, **68%** do consumo de
   60 dias acontece nos primeiros 30.
4. **O consumo total é dominado por internação** (diária global e pacotes: 46% na Reumatologia, 31% na Tireoide),
   que pouco tem a ver com a linha de cuidado. É o principal motivo de o consumo médio variar tanto e demorar.
   **Medir o valor nos produtos da linha** (infusão, medicamento, procedimento) tende a reduzir muito o ruído e a
   mostrar o impacto financeiro **atribuível** mais cedo.

## Prazos da jornada na Central de Navegação

Fonte: `bronze_fabrica_ia_dev.navegacao.vw_central_navegacao_caso_analitico` — **catálogo de DEV**, só Reumatologia
com jornada preenchida (1.335 casos, jan–out/2026). Dias contados **da liberação do laudo**.

| etapa | casos | mediana | 75% até | 90% até |
|---|---|---|---|---|
| 1º contato | 903 | 5 | 9 | 21 |
| admissão | 572 | 3 | 8 | 20 |
| consulta | 493 | 0 | 15 | 37 |
| infusão | 273 | 31 | 43 | 79 |

⚠️ `dias_caso_ate_contato_1` sai **negativo** (contato antes da criação do caso: casos migrados) e
`dias_liberacao_ate_caso` tem mediana de 62 dias — por isso a conta usa a liberação como ponto zero.
⚠️ `vl_consumo` está **zerado** em todos os casos: a Central ainda não traz valor.

## Quando o consumo acontece depois do laudo

Coortes reais de pacientes com laudo relevante (consumo na `vw_mov_fato_consumo`, sem destino `H`).

| dias após o laudo | Reumato: % que voltou | Reumato: % do valor de 180 dias | Tireoide: % que voltou | Tireoide: % do valor de 60 dias |
|---|---|---|---|---|
| 7 | 36% | 13% | 32% | 26% |
| 14 | 48% | 23% | 46% | 39% |
| 30 | 62% | 37% | 62% | 68% |
| 60 | 72% | 55% | 72% | 100% |
| 90 | 77% | 68% | — | — |
| 180 | 85% | 100% | — | — |

Reumato: 434 pacientes com 180 dias completos. Tireoide: 2.536 com 60 dias completos (a coorte começa em julho).
⚠️ Esse retorno inclui o acompanhamento que aconteceria **sem** navegação — é o nível do controle, não o efeito.

## Onde está o valor (consumo após o laudo, por tipo de produto)

| tipo | Reumatologia (180 dias) | Tireoide (60 dias) |
|---|---|---|
| diária global + pacote de diária | 46% | 31% |
| pacotes | 14% | 16% |
| materiais descartáveis | 9% | 8% |
| medicamentos | 9% | — |
| honorários médicos | 6% | 7% |

## O que isso muda na proposta

| para mostrar | indicador | janela | quando sai o primeiro número |
|---|---|---|---|
| que a navegação muda comportamento | consulta/agendamento na Rede | 14–21 dias | o mais cedo |
| retorno | voltou a consumir na Rede | 30 dias | depois |
| **impacto financeiro atribuível** | **valor nos produtos da linha** | 60–90 dias (Reumato) · 30–45 (Tireoide) | a medir: falta o CV desse valor |
| impacto financeiro total | consumo total | 90–180 dias | o mais tarde e o mais ruidoso |

**Próxima medição:** variação (CV) do valor nos produtos da linha — a Central tem `ds_produtos_linha`, e a base de
consumo tem `nme_produto`/`Cd_Tuss`. Se o CV cair de ~3 para ~1,5, o tempo para enxergar o mesmo ganho cai a ~1/4.

## Panorama INFORMATIVO — variação do valor por recorte (22/10)

> ⚠️ **Caráter informativo.** A base de consumo tem semântica incerta (categorias duplicadas por grafia,
> "Medicamentos" da reumatologia dominado por antibiótico hospitalar, Central sem lista de produtos por linha).
> Os números orientam o desenho do estudo; não sustentam decisão sozinhos.

| coorte | recorte | janela | com valor | média | CV | CV com teto p99 |
|---|---|---|---|---|---|---|
| Reumato | total | 90 | 79% | R$ 25.873 | 4,26 | 3,15 |
| Reumato | sem diárias | 90 | 79% | R$ 15.617 | 5,34 | 3,32 |
| Reumato | produtos da linha | 90 | **5%** | R$ 1.012 | 13,2 | 6,56 |
| Tireoide | total | 30 | 60% | R$ 3.100 | 6,38 | 3,71 |
| Tireoide | sem diárias | 30 | 60% | R$ 1.788 | 5,83 | 3,00 |
| Tireoide | produtos da linha | 30 | **13,5%** | R$ 61 | 11,7 | 3,75 |

Produtos da linha, por nome: Tireoide = tireoidectomia, PAAF, punção, cell block; Reumato = imunobiológicos,
metotrexato, ciclofosfamida, pulsoterapia, infusão.

**Leitura:**
1. **A hipótese caiu:** o valor nos produtos da linha **não** é menos ruidoso por paciente — é **raro** (5% na
   Reumato, 13,5% na Tireoide), e evento raro com valor alto varia ainda mais. Tirar as diárias também não reduz o CV.
2. **O caminho que sobra para o financeiro:** medir **se o paciente fez o procedimento da linha** (sim/não) e
   converter em R$ com o ticket médio da linha: *impacto = Δ taxa de conversão na linha × ticket da linha × volume*.
   Proporção é muito mais estável que valor.
3. **Tireoide é viável** por esse caminho: com 10% e 6 meses, enxerga +4,8 p.p. sobre 13,5% (+35%); com 5%, +6,5 p.p.
   (+48%). **Reumato não**: 5% de base e ~200 pacientes/mês exigem efeitos acima de +80% mesmo em 12 meses.
4. **A coorte da Reumato mistura pacientes internados** (consumo hospitalar alheio à linha) — a população do estudo
   e o que conta como "da linha" precisam ser definidos com o negócio antes de qualquer leitura financeira.

### ⚠️ Correção da leitura acima — valor por ATENDIMENTO (22/10)

Duas falhas na primeira leitura do panorama:

1. **"Sem diárias" não é recorte limpo.** Procedimento da linha costuma exigir internação, mesmo curta (hospital-dia):
   as diárias desses atendimentos **são** valor da linha. A internação que não é da linha é a **do exame**
   (paciente já internado por outro motivo), não as internações depois do laudo.
2. **O padrão `infus` da Reumatologia capturou equipo e bomba de infusão hospitalares**, não imunobiológico — o
   recorte "linha" da Reumato media paciente internado.

Remedição: valor de **todo o atendimento** (`Id_Hospital` + `Cd_Atendimento`) iniciado **depois** do laudo que contém
procedimento da linha; o atendimento em curso no dia do exame fica separado.

| coorte | janela | fez procedimento da linha (atendimento novo) | ticket do atendimento inteiro | ticket só do item |
|---|---|---|---|---|
| Tireoide | 30 | 12,8% | R$ 3.551 | R$ 438 |
| Tireoide | 60 | 17,6% | R$ 4.619 | R$ 642 |
| Reumato (só medicamentos da linha) | 30 · 60 · 90 | 0,26% · 0,37% · 0,37% | R$ 31 mil · 36 mil · 52 mil | — |

**Leitura corrigida:**
- **Tireoide:** o ticket da linha por atendimento é **~8× o do item isolado**. O indicador "fez procedimento da linha"
  (12,8% em 30 dias) segue viável, e o impacto em R$ = Δ taxa × **R$ 3,5–4,6 mil** × volume. Quase sem diária
  (0,2 em média): PAAF ambulatorial domina; tireoidectomia é a minoria de valor alto.
- **Reumato:** imunobiológico/medicamento da linha aparece em **5 de 1.342** pacientes em 90 dias. Ou a base de
  consumo não registra esse tratamento (dispensação fora da conta hospitalar, outro nome/código), ou ele começa
  depois de 90 dias. **Definir "da linha" por nome de produto não funciona na Reumato** — precisa de definição do
  negócio (código TUSS, centro de infusão, consulta especializada).
