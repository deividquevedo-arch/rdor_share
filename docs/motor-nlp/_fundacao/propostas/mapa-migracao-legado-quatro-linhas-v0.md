---
tags: [migracao, legado, plano]
---

# Mapa de migração — Mama, Nódulo pulmonar, Nefro e Hepato

> **v0 — 14/10/2026.** Etapa de **Plan** do RPI: transforma a auditoria em trabalho executável.
> Fonte de cada afirmação: `_processo/medicoes/auditoria-legado-quatro-linhas-2026-10-14.md`.
> Card do plano: `303791` — *Plano de Migração algoritmos final*.

## Como ler

Cada componente do legado tem **um destino** e **uma alçada**:

| alçada | quem | o que cobre |
|---|---|---|
| **DS** | Dono do NLP Engine / cientista da linha | config da especialidade, definição do job, navegação, lib (POP-IA-08) |
| **Plat** | MLOps / plataforma | runner, `tools/data_exchange`, view, cluster, orquestração, secret scope |
| **Fáb** | time da Fábrica | schema nos três catálogos, grants |
| **Neg** | negócio / dono clínico | destino, régua, contrato com o sistema de destino |

Uma linha nova na plataforma são **seis arquivos** (`jobs/clusters`, `jobs/definicoes`, três
navegações, config). Tudo que não cabe neles é trabalho de plataforma.

---

## 0. Pré-requisitos comuns — antes de qualquer linha

| # | item | alçada | por quê |
|---|---|---|---|
| P1 | **dono e definição do orquestrador** do workspace `bi-hml-02` que dispara `ia-birads-v2`, `ia-pulmao`, `ia-rim` e `ia-hepatologia-envio-clone` | Plat | sem ele não há como **desligar** o legado; o job aparece pausado e roda assim mesmo |
| P2 | **decisão de destino por linha** — manter Mule / Salesforce / e-mail, ou convergir para a Central | Neg | define se a linha é config pura ou exige adaptador novo |
| P3 | **envio à Central restabelecido** (incidente `371813` — *[Captação] Todas as linhas indisponíveis*) | Plat | qualquer linha que convirja para a Central herda o bloqueio |
| P4 | **configurações em `/mnt/` copiadas para Volume/UC**, com inventário | Plat + DS | a plataforma não lê `/mnt/`; os dicionários são parte da régua |
| P5 | **protocolo de corte**: janela em paralelo, comparação por `id_exame`, desligamento no orquestrador, **sem envio duplo** | DS + Plat | o `360625` já registra três linhas enviando dos dois lados |

---

## 1. Mama (BI-RADS) — régua pronta, esforço no envio

**Legado:** `ia-birads-v2`, `IAAzureDatabricksBiRads@hml` (`75a5898`), diário. ~43 mil exames em 44
dias; 1.647 relevantes pela definição do monitoramento.

| componente do legado | destino na plataforma | alçada | estado |
|---|---|---|---|
| filtro `mam[oaá]` em 4 colunas, IMG/IMA, sem biópsia, sem `RICHET%` | `gold_filter` da config — **lê só `proced_descricao`** | DS | 🟡 medir a perda contra as 4 colunas antes ([[paridade-nao-enxerga-o-filtro]]) |
| 2ª fonte PACS (`ia.vw_enave_df_historico_birads`, `hive_metastore`) | sem equivalente | Plat + Neg | 🔴 medir quanto o PACS acrescenta; se for marginal, **sair de escopo** é a opção barata |
| janela móvel de 30 dias reprocessada | runner processa D-1 e deduplica por `id_exame` | Neg | 🟡 a janela existe por laudo liberado tarde ou pelo contrato da Mule (`dataInicial`/`dataFinal`)? |
| régua regex + `replace` de legenda | `ordinal_extraction` · `systems.bi_rads` · `aggregation_legend_filter` | DS | ✅ **config pura, zero lib** — bancada em `rads/notas/Relatorio-homologacao-birads-bancada-v1.md` |
| saída com a categoria de **todo** exame (−1 a 6) | view entrega relevantes | Plat + Neg | 🔴 contrato diferente: ou a view expõe todas as categorias, ou o destino aceita só relevantes |
| validações (CNPJ 14, regional 2, nome ≤ 250) | `validacao` da navegação | DS | ✅ mecanismo existe |
| GET de disparo na Mule + espera do log até 4 h | **não existe** — a API da plataforma é `POST /api/ingestao/inferencias` | Plat | 🔴 adaptador novo **ou** Central (P2) |
| notificação Teams | `tools/data_exchange` | Plat | ✅ existe |
| CPF, nome, telefone descriptografados | view | Plat | ✅ padrão |
| schema `birads` nos três catálogos | — | Fáb | 🔴 pedir |

**Sequência:** schema → config + bancada reexecutada sobre a gold atual → run em dev → decisão de
destino → adaptador ou Central → paralelo em hml → corte.
**Card relacionado:** `304135` — *[BI-RADS] Levantamento para Migração* (cobre o processo no ADF).

---

## 2. Hepato — o motor está lá, falta o ciclo

**Legado:** `ia-hepatologia-envio-clone` (motor + board, diário) e
`ia-hepatologia-retorno-navegacao-clone` (dias úteis 07:00, **agenda própria**),
`IAAzureDatabricksHepatologia@hml` (`a6a33ec`).

| etapa | legado | plataforma hoje | alçada | o que falta |
|---|---|---|---|---|
| motor | `algoritmo_v2` | ✅ `hepatologia` em prd | DS | a fila da passada única: `segmentation.mode: full_doc` primeiro (`300202`), banda, `fallback_policy`, blocos mortos |
| **1. board médico** | Excel por unidade, lista de complexidade | ✅ **existe** — navegação com `complexidade` (Alta, Baixa, Transplante, Oncologia…) e task `onedrive_hepatologia` | DS | conferir colunas contra a extração do legado; **a do legado roda sem a deduplicação do PR 6646** |
| **2. retorno** | Logic App busca planilhas no OneDrive → `retorno_v2` + histórico, pastas sucesso/erro | 🔴 **não existe** | Plat | componente de captura de retorno + tabela no schema `hepatologia` |
| **3. navegação por fluxo** | duas listas (`transplante`; `onco_e_baixaComplexidade`) a partir do retorno | 🔴 **não existe** | Plat + DS | exchange alimentado pela **view do retorno**, não pela saída do motor; duas navegações |
| histórico do retorno | 5.836 linhas em `retorno_v2` | — | Plat | migrar o histórico, senão a navegação perde quem já foi avaliado |
| URL do Logic App com SAS no código | — | — | Plat | 🔴 secret scope, e **rotacionar** a assinatura |

**Sequência:** retorno (2) é o caminho crítico — sem ele, (3) não tem entrada. A passada única do
motor pode correr em paralelo, mas **muda a lista que vai ao board**: entrar com ela antes do corte
evita dois rebaselines com os médicos.

---

## 3. Nefro — decisão de arquitetura antes de esforço

**Legado:** `ia-rim`, `IAAzureDatabricksRim@hml` (`df21725`), diário. ~1,27 milhão de exames de
creatinina em 44 dias, 4.512 relevantes (definição do monitoramento: clearance ≤ 35, idade ≤ 80).

🔴 **Não é NLP:** cálculo de TFG (CKD-EPI) sobre valor de laboratório e cadastro. O runner da
plataforma é construído em volta do `nlp_engine`; **nenhum dos seis arquivos serve**.

| componente | destino | alçada | estado |
|---|---|---|---|
| **onde a linha vive** | (A) job próprio no repositório da plataforma, fora do runner · (B) legado reescrito em UC, fora da plataforma | Plat + DS | 🔴 **decisão que destrava o resto** |
| entrada | produção lê FHIR (`observation`, 5 dias); a versão em UC (`ntb_ia_rim_motor`, parada em 13/05) lê `tb_gold_mov_exame` | DS | 🟡 a versão UC é a base natural, mas **trocou a fonte**: A/B das duas entradas por `id_exame` antes de adotar |
| cálculo | CKD-EPI sem o termo `α`, fórmula adulta abaixo de 18 anos, sexo ausente → masculino | Neg (clínico) | 🔴 validação clínica da fórmula |
| relevância | três definições: motor ≤ 60, monitoramento ≤ 35 e idade ≤ 80, envio desconhecido | Neg | 🔴 escolher **uma** |
| saída | `vw_gold_creatinina`, valor arredondado a inteiro | DS | 🟡 corrigir o `cast` na migração |
| envio | **nenhum no código**; a view é lida de fora | Neg + Plat | 🔴 **descobrir o consumidor** (sem acesso ao histórico de consultas) |
| exclusões | AMIL por regex; 44 unidades por ID literal no monitoramento | DS | 🟡 viram config declarada |
| schema `nefrologia` | catálogo `diamond_nefrologia` existe, mas preso ao workspace antigo | Fáb | 🔴 pedir nos catálogos da Fábrica |

**Sequência:** consumidor do envio → decisão (A)/(B) → A/B de entrada → validação clínica → porte.

---

## 4. Nódulo pulmonar — projeto, não porte

**Legado:** `ia-pulmao`, `IAAzureDatabricksPulmao@hml` (`c0ee4d3`), diário — **falhou em 12 e 13/10**.
~52,9 mil exames em 44 dias; tabela do Salesforce com 1.288 linhas no período.

| componente | destino | alçada | estado |
|---|---|---|---|
| filtro TC/RM de tórax, 3 colunas, 30 dias por liberação | `gold_filter` | DS | 🟡 mesma medição da mama |
| idade do cadastro | contrato de entrada do motor não tem | Plat | 🔴 |
| spaCy + `EntityRuler` com **14 CSV** | parte vira `findings`/`exclude`/negação; o resto não tem destino | DS | 🔴 inventariar os 14 CSV (P4) |
| **extração por nódulo** (tamanho, localização, caracterização, risco) | lib tem categoria, medida e vínculo medida↔lesão (`lesion_linking`, 0.15); **não** tem atributos por lesão | DS (lib) | 🔴 trabalho de lib |
| **evolução** (surgimento, progressão, estabilidade…) e **conduta** | zero na lib | DS (lib) | 🔴 trabalho de lib |
| saída em lista de lesões (`saida_salesforce`, 37 colunas) | plataforma entrega `fl_relevante` + `findings` | Plat + Neg | 🔴 |
| Salesforce | sem adaptador | Plat | 🔴 |

**A pergunta que define o tamanho:** das 37 colunas do Salesforce, **quais o destino usa de fato**?
Se for "nódulo ≥ X mm com risco", cabe em `findings` + critério `dimensional` + `lesion_linking` —
config com pouca lib. Se for a ficha completa por nódulo com evolução, é projeto de lib de várias
versões. **Medir o uso antes de desenhar.**
Enquanto isso, a opção de menor risco é **manter o legado e só estabilizar** (falhas de 12–13/10,
`/mnt` → Volume, caminho de sandbox removido).

---

## 5. Ordem recomendada

| onda | linha | por quê |
|---|---|---|
| **1** | **Hepato — retorno + navegação por fluxo** | metade já está na plataforma; é o que falta para a hepatologia estar de fato em prd; caminho crítico é plataforma |
| **1** | **Mama** — régua e run em dev | régua sem lib e com bancada; corre em paralelo à hepato por ser alçada DS |
| **2** | **Mama** — envio | depende de P2 e do adaptador ou da Central |
| **2** | **Nefro** — decisão e descoberta do consumidor | nada é portado antes disso |
| **3** | **Nódulo** — medição de uso das 37 colunas | define se é config ou projeto |

## 6. Decisões a levar — uma linha cada

1. **Destino** de mama, nefro e nódulo: manter o sistema atual ou convergir para a Central. — Neg
2. **Nefro na plataforma ou fora dela.** — Plat + DS
3. **Relevância da nefro:** ≤ 60, ≤ 35, idade ≤ 80. — Neg
4. **PACS na mama:** mantém (trabalho de runner) ou sai (medir antes). — Neg
5. **Nódulo:** quais colunas do Salesforce são usadas. — Neg
6. **Acesso ao orquestrador** do `bi-hml-02`. — Plat

## 7. Critério de corte, por linha

Uma linha só sai do legado quando:

- [ ] roda na plataforma em **prd** com perfil completo calibrado (régua → híbrido → juiz, onde se aplica);
- [ ] janela em paralelo com o legado, **delta por `id_exame` adjudicado** e levado ao negócio só nas discordâncias;
- [ ] o destino recebe da plataforma — verificado do lado de lá, não pelo `202`;
- [ ] o job do legado **desligado no orquestrador** no mesmo dia em que a plataforma passa a enviar;
- [ ] o espelho e o monitoramento do legado conferidos **na fonte** depois do corte.
