# Levantamento — repositórios git (E01–E18)

> Frente: histórico de commits, código, configs (cabeçalho e changelog), PRs. Somente leitura.
> Levantado em 19/11/2026. Eventos definidos em [[eventos-a-explicar]].
>
> **Confiança:** `confirmado` = está escrito no código, no commit ou na descrição do PR;
> `inferido` = a fonte mostra a mudança, mas a ligação com o número observado é dedução.
>
> ⚠️ **Limite estrutural da frente:** os jobs do legado rodam no workspace antigo a partir de Repos
> (branch `hml`) disparados por orquestrador fora do git. **Data de início e de parada de job, pausa
> e execução manual não ficam no git.** O git dá a data em que o código mudou (merge do PR), não a
> data em que o job passou a rodar com ele.

## Mapa de repositórios

| linha | repositório do legado | observação |
|---|---|---|
| Ateromatose, TI-RADS, PI-RADS (e cópias das demais) | `fabrica-ia-plataforma` — `apps/databricks/<linha>/`, branch ativa `branch-from-versao-alpha` | histórico começa em 22/04/2026; as pastas de outras linhas são **cópias** consolidadas em 15/05 e 28/05 (PRs 6269, 6411) |
| Ca de Cólon | `IAAzureDatabricksColon` | |
| DII | `IAAzureDatabricksDII` | |
| Doenças biliares | `IAAzureDatabricksBiliar` | |
| Neuroimunologia | `IAAzureDatabricksNeuroimunologia` | |
| Reumatologia | `IAAzureDatabricksReumatologia` (clonado em `Projects/`) | |
| Nefrologia (clearance) | `IAAzureDatabricksRim` | segunda cópia em `fabrica-ia-plataforma/apps/databricks/rim` |
| Mama (BI-RADS) | `IAAzureDatabricksBiRads` | |
| Pulmão – Nódulo | `IAAzureDatabricksPulmao` | |
| Fígado (hepatologia) | `IAAzureDatabricksHepatologia` | |
| captação manual PACS (enave) | `IACaptacaoSolucoes` (`rastreioTirads/`, `rastreioPirads/`, `rastreioBirads/`) | ⚠️ notebooks com saída gravada contendo dado de paciente — citar só o caminho |
| plataforma nova (todas) | `fabrica-ia-nlp-platform` — `plataform/config/speciality/`, `jobs/definicoes/`, `jobs/ambientes/prod.json` | produção = `main` |

## Eventos

### E01 — Ateromatose: queda de relevantes jun → jul → ago

| data | o que mudou | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| 23/06/2026 | estreia: PR 6655 — *[ADB][PRD] Algoritmo Ateromatose Versão Final v01* | — | `fabrica-ia-plataforma`, merge `b6a4c42` | confirmado (casa com a estreia de 24/06) |
| 25/06/2026 | PR 6672 — *eliminando filtro de regional base entrada ateromatose*: entrada deixa de ser só SP e RJ | "houve necessidade de adicionar novas unidades … filtro de regional somente SP e RJ que era o escopo inicial" | merge `a9bd276`; `data/ntb_ia_ateromatose_entrada.py` | confirmado (efeito: **aumenta** volume) |
| 03/07/2026 | PR 6747 — *filtro de idades de 45 a 70 anos* na entrada (`AND idade_anos BETWEEN 45 AND 70`) | "previsto para afetar os envios de segunda feira" (comentário no notebook) | merge `4666fbc`; `data/ntb_ia_ateromatose_entrada.py:26` | confirmado |
| 15–20/07/2026 | PR 6844 — *Ajuste das capturas exigidas (ateromatose difusa ou importante)*: regra coronariana passa a exigir **coronária + (difusa \| importante)**; seed solto "ateromatose difusa" deixa de valer sem vínculo coronariano; calcificação sai do escopo de TC de tórax (commits `47d0e42`/`e55f52d` de 15/07) | "fazer o notebook implementar exatamente a regra ateromatose + (difusa\|importante) + coronária" | merge `61d43f2` (20/07); `model/ntb_ia_ateromatose_algoritmo_log.py` | confirmado no código; a ligação com a queda é **inferida** |
| 23/07/2026 | PR 6889 — *Ajustes nas capturas de estenose*: nova captura de Doppler de carótidas/vertebrais (estenose ≥ 50%) e recuperação de variantes de redação coronariana | tasks 197217 e 197189 | merge `f363cc9` | confirmado (efeito: aumenta um pouco) |
| 11/11/2026 | (plataforma) PR 7930 — *Rollback de volume ateromatose - remove exigência de difusa* | — | `fabrica-ia-nlp-platform` | confirmado; fora da janela, mas confirma que a exigência de difusa/importante é a restrição de volume |

**Leitura:** a queda jun → jul → ago é compatível com duas restrições em sequência — faixa etária
45–70 (03/07) e exigência de "difusa/importante" com vínculo coronariano (20/07). As mudanças de
regra foram feitas no notebook `_log`; **qual notebook o job executa não está no git** (inferido que
seja o `_log`, que é o único com as alterações de julho; o `ntb_ia_ateromatose_algoritmo.py` só
mudou em 20/07 e 04/09).

### E02 — Ca de Cólon

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 13/02/2026 | PR 5691 — *atualizacao funcoes e atualizacao algoritmo v4*; PR 5692 — *eliminacao do filtro head mil* | `IAAzureDatabricksColon` `4bbb51c`, `b3cb454` | confirmado |
| 19/02/2026 | PRs 5707 e 5711 — *Start inicial pipeline diario com trigger* → fev parcial (138 relevantes) | `f9dfea4`, `8a7f047` | confirmado (casa com a estreia de 20/02) |
| 16/03/2026 | PR 5900 — *Ajuste query balizadora* (entrada) | `38acea5` | confirmado |
| 26/03/2026 | PR 5972 — *adicionando colonoscopia*: "foi incluído os exames de colonoscopia, testado e por último, inserido dados da tabela de entrada antiga para a nova" (cria `ntb_ia_colon_algoritmo_colonoscopia`, `entrada_v2`, `extracao_v2`) | `891c8db` | confirmado (casa com 30/03) |
| 13/07/2026 | PR 6806 — bugfix: laudo positivado pela indicação "rastreamento" | `ec42b8b` | confirmado |
| 11/08/2026 | PR 7021 — regra v4.6 (FP de polipectomia; precisão homologada 89%, antes ~21%) | `80b949e` | confirmado |
| 24/08/2026 | PR 7109 — regra v5.0 (lesão plana/LST ressecada não navega salvo sinal de gravidade) | `694c4a8` | confirmado |
| 21/08/2026 | inventário da migração registra escopo **só colonoscopia** (`modalidade='COL'`); esteira TC/RM/biópsia = `colon/model/ntb_ia_colon_algoritmo.ipynb`, "fora desta missão; escopo confirmar" | `fabrica-ia-plataforma` `6c458da`, `docs/nlp-engine/cancer-colon/00_inventario_fontes.md:67` | confirmado |
| 16/09 → 28/09/2026 | plataforma: config `cancer_colon` com `gold_filter.keywords: ['colonoscop']` (PR 7191 na hml; `main` em 28/09, PR 7444) | `plataform/config/speciality/ntb_ia_cancer_colon_config.py:12,653` | confirmado |

Pausa do job legado em 02/10 (card `360625`): **não está no git**.

### E03 — DII: relevância 5,9% (jan) e 3,6% (fev) contra ~0,6% a partir de mar

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 15/01/2026 | PR 5525 — *regioes* (entrega por regiões) | `IAAzureDatabricksDII` `33da23a` | confirmado |
| 22/01/2026 | PR 5550 — *filtro por idade*: **retirado** o filtro de idade ("apenas linhas oncológicas fazem sentido essa limitação") | `74d2d86` | confirmado |
| 12/02/2026 | PR 5684 — *atualizado ntbs*: nasce a versão nova do algoritmo (`model/ntb_ia_dii_algoritmo_novo.ipynb`, `entrada_novo`, `extracao_novo`). O `CONFIG` do algoritmo antigo é comentado no mesmo dia (`ebb2af7`). Algoritmo antigo: `min_sim_seed_vocab: 0.85`, `top_k_per_seed: 8`, `min_token_len: 3` | `2034797`, `ebb2af7` | confirmado |
| 20/02/2026 | *Alteração de palavras-chaves*: saem três seeds de sinais clássicos ("sinal do halo gorduroso", "vasos sanguíneos dilatados e tortuosos", "espessamento parietal com estratificação") | `6bfe6e0` | confirmado |
| 24/02/2026 | PR 5750 — *Alteração de valores dos parâmetros*: "parâmetros de sensibilidade do algoritmo de DII" — **`min_sim_seed_vocab` 0,82 → 0,90** (expansão semântica mais restrita) e `min_token_len` 5 → 3 | `e96b9f1`; `model/ntb_ia_dii_algoritmo_novo.ipynb:1838` | confirmado no código; ligação com a queda **inferida** |
| 11–12/03/2026 | PRs 5865/5874 — padronização: o `_novo` vira o `ntb_ia_dii_algoritmo.ipynb` oficial (com 0,90) e o antigo vira `_legado` | `60d5acd`, `01023ef`, `9a81488` | confirmado |
| 12–19/03/2026 | colonoscopia: PRs 5882, 5896 (*finalizando a produtização de dii colono*, 14/03), 5915 (*Finalizando o fluxo de DII*, 19/03), 5925 (filtros, 20/03), 5942/5953 (capturas de TC e query balizadora, 23 e 25/03). Algoritmo de colonoscopia com `min_sim_seed_vocab: 0.85` | `1573206`, `93dab8d`, `0def165` | confirmado (casa com a entrada da colonoscopia em 17/03) |

**Leitura:** a mudança de régua está entre 12/02 e 12/03 — algoritmo novo com seeds reduzidas e
limiar semântico 0,90 (24/02), oficializado em 12/03. Fev intermediário (3,6%) é compatível com
troca no meio do mês. Qual dos dois notebooks o job rodava em fevereiro **não está no git**.

### E04 — Reumatologia: crescimento jan → abr e queda em ago

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 28/01/2026 | PR 5589 — *Inicio do Pipeline Reumatologia*. Entrada com janela `DataLaudoFinal BETWEEN date_sub(current_date(), 30) AND ontem` → jan carrega ~30 dias retroativos | `IAAzureDatabricksReumatologia` `9ab4d57` | confirmado |
| 29/01/2026 | PR 5596 — "devido à mudança de padrão de id das unidades foi necessário eliminar o id no left anti join" | `e657041` | confirmado (ver E17) |
| 16/03/2026 | PR 5899 — *Ajuste query entrada*: filtro de exame passa de **uma** coluna (`dsc_procedimento_integracao`, padrões de sufixo `'%tomografia'`, `'%tc'`, `'%rm'`) para **três** (`proced_descricao_ajustado`, `dsc_codigo`, `cod_procedimento`) | `caed6e3` | confirmado |
| 16/03/2026 | PR 5901 — *Ajuste tomo*: `'%tomografia'` → `'%tomografia%'`, `'%tomo%'`; `tc/usg/rm/rnm` com borda de palavra; `pe/pé` com borda | `b140c5f` | confirmado |
| 25/03/2026 | PR 5970 — *Ajuste query balizadora*: "o trecho NOT LIKE passou a usar COALESCE" (com nulo, `NOT LIKE` descartava a linha) | `c0ae8ad` | confirmado |
| 23/07/2026 | PR 6893 — *Bugfix de exames incorretos*: comentados os filtros de face, crânio e afins (TC, RM, US) | `b200286` | confirmado (casa com a queda de ago) |
| 04–05/09/2026 | ajustes de catálogo/SSL; migração dos objetos para `diamond_fabrica_ia.legado` | `7293729`, `af6b03c` | confirmado |

**Leitura:** crescimento de volume e de hospitais jan → abr é compatível com o alargamento do filtro
de entrada em 16/03 e 25/03 (inferido; o número de hospitais não aparece no código — não há filtro de
unidade na entrada). A janela retroativa de 30 dias explica jan já com volume.

### E05 — Tireoide (TI-RADS)

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| fev/2025 | captação manual a partir do PACS: notebooks de histórico sobre `ia.vw_enave_df_historico_tirads` / `_pirads` / `_birads` | `IACaptacaoSolucoes` `rastreioTirads/notebooks/01.ntb_tirads_historico_save_Full.ipynb` (só caminho; contém dado de paciente) | confirmado (origem da tabela enave) |
| 11/06/2026 | PR 6560 — *[Alpha] pi/ti/ateromatose* ("início ntbs tirads", "tirads v0") | `fabrica-ia-plataforma` `7aec647` | confirmado |
| 11/06/2026 | entrada do legado lê `dt_liberacao_laudo >= date_sub(current_date(), 180)` → **primeira execução traz ~6 meses retroativos** | `apps/databricks/tirads/data/ntb_ia_tirads_entrada.py:247` | confirmado; liga com a carga jan–jun **inferida** |
| 18/06/2026 | PR 6607 — *[Tirads] Compilado de ações*: "trazer exames de um range de 30 dias", monitoria | `7151d74` | confirmado |
| 24/06/2026 | PR 6660 — *[TIRADS] Deduplicagem* de pacientes | `09cb2d8` | confirmado |
| legado | relevante = `tirads in ('4','5')`; filtro de entrada = `IMG/IMA` **e** (US/Doppler **e** tireo/pesco) ou (cintilo **e** tireo) ou (PAAF/punção/biópsia **e** tireo) ou (TC **e** pesco/tireo/laringe/faringe/partes moles, sem angio), sobre 3 colunas | `ntb_ia_tirads_algoritmo.py:822`; `ntb_ia_tirads_entrada.py:250-395` | confirmado |
| plataforma (19/08, PR 7071 `0.8.0-tirads`; config na `main` desde 19/08) | `gold_filter.keywords: ['tireoide','tireóide','pescoco','pescoço']` sobre `proced_descricao`, **sem exigir modalidade nem `IMG/IMA`**. Cabeçalho registra: `pescoço` entrou por recall (+557 exames, +26%, recupera 19 de 25 positivos); sangue saiu da captação em 18/08 (−71% da entrada) | `fabrica-ia-nlp-platform` `plataform/config/speciality/ntb_ia_tirads_config.py:617-660`; commit `15328c5` (18/08) | confirmado; o fator ~3,5× é **inferido** (qualquer exame com "tireoide/pescoço" no nome, de qualquer modalidade) |

### E06 — Próstata (PI-RADS)

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 27/05/2026 | PR 6377 — *[ADB][HML] Inicio envios Pirads* (+ 6379, 6394, 6395, 6398 em 27–28/05) | `fabrica-ia-plataforma` `112f1ee` | confirmado (casa com 27/05) |
| legado | relevante = `pirads in ('4','5')`; entrada `IMG/IMA`, RM **e** (prost \| pelve) sem angio, ou US **e** (prost \| transretal) **e** biópsia, sobre 3 colunas, janela de 30 dias | `apps/databricks/pirads/model/ntb_ia_pirads_algoritmo.ipynb:1389`; `data/ntb_ia_pirads_entrada.ipynb:1030-1130` | confirmado |
| 29/09/2026 | plataforma, config `0.1.1` (PR 7462): **sexo masculino e `IMG/IMA` do legado não entram no `gold_filter`** (decisão de 29/09: vão para o `validacao` da navegação). E2E em dev (25/08–18/09): **motor 6.521 laudos × legado 2.228** (≈2,9×); só no motor: 4.305 exames de mulheres (0 relevantes) e 224 de homens IMG/IMA (11 relevantes). `0.1.1` acrescenta US transretal sem "biópsia" (+31 exames) | `plataform/config/speciality/ntb_ia_pirads_config.py:36-56,187-210` | **confirmado** (explica o ~3×) |
| 09/10/2026 | job `pirads-batch` entra na `main` (PR 7538, sexta-feira; cron `MON-FRI` → 1ª execução 12/10) | `jobs/definicoes/pirads-batch.json` em `a13b42b` | confirmado |

### E07 — Doenças biliares

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 25/03/2026 | criação do notebook | `IAAzureDatabricksBiliar` `2321a2a` | confirmado |
| 02/04/2026 | PR 6038 — *Nova feature* ("incluindo nova feature no modelo"); PR 6040 — *Bug do config_v2*. Saída `tb_diamond_mod_doencas_biliares_saida` | `88ff44c`, `080ae83` | confirmado (casa com 03/04) |
| 10–14/04/2026 | PR 6083 — notebooks V2 (`entrada_v2`, `algoritmo_v2`) → saída `_saida_v2`; PRs 6097, 6103 | `ac63ec0` | confirmado |
| 24/04/2026 | PR 6165 — notebook de monitoramento | `ad84f61` | confirmado |
| 04/05/2026 | PR 6207 — retirado o filtro `tipoexame NOT IN` "pronto socorro": exames de PS passam a entrar | `21b60d3` | confirmado |
| 16 e 23/06/2026 | PRs 6605 e 6651 — V3: "refina os parâmetros de sensibilidade, singletons, palavras-chave (seeds e findings), palavras e janelas de negação"; saída `_saida_v3` | `596880c`, `60d2fd7` | confirmado |
| 25/06/2026 | PR 6668 — *Bugfix tabela de saída*: o V3 (`.py`) passa a gravar em **`_saida_v2`** | `6ff97b8`; `model/ntb_ia_doencas_biliares_algoritmo_v3.py:108` | confirmado |
| 28/07/2026 | PR 6919 — *Retirando o filtro por UF* (monitoramento contava só `SP`, `RJ`) | `dd8949f` | confirmado (afeta a **contagem do monitoramento**) |
| 09/10/2026 | plataforma: job entra na `main` (PR 7538) → 1ª execução 12/10 | `a13b42b` | confirmado |

Os 79% de relevância da 1ª versão e a parada do job em 06/10: **sem fonte no git**.

### E08 — Neuroimunologia

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 25/03/2026 | notebooks criados | `IAAzureDatabricksNeuroimunologia` `3ecefda` | confirmado |
| 16 e 22/04/2026 | PR 6128 (melhorias), PR 6147 (`dv_achado` da extração) | `a04037c`, `fc1889a` | confirmado |
| 30/04/2026 | PRs 6196, 6197, 6200 — monitoramento e renome das tabelas de entrada/monitoramento | `f61f7a5`, `727e8da`, `2cf5c46` | confirmado (casa com 29/04, inferido) |
| 03/07/2026 | PR 6746 — correção do bug da negação | `c7375f6` | confirmado |
| 28/07/2026 | PR 6920 — *Tirando o filtro de regionais* | `64a02ef` | confirmado |
| 28/09/2026 | plataforma: PR 7428 (hml) e PR 7444 (`main`, 28/09) → 1ª execução 29/09 | `fabrica-ia-nlp-platform` `c096942` | confirmado |

Parada do legado em 01/10: **não está no git**.

### E09 — Nefrologia (clearance)

**Regra no código — não é clearance < 30.**

| item | o que o código faz | fonte | confiança |
|---|---|---|---|
| entrada | FHIR `observation_exames`, TUSS `40301630` (creatinina), janela de **5 dias** (`date_sub(current_timestamp(), 5)`) | `IAAzureDatabricksRim` `model/ntb_ia_rim_modelo.py:118-170` (branch `hml`) | confirmado |
| cálculo | `calc_cleareance_without_weigth` (fórmula da tarefa #9383, fator `0.9938 ** idade`); existe Cockcroft-Gault/Schwartz no código, não usada na UDF | `ntb_ia_rim_modelo.py:42-75,267-275` | confirmado |
| saída | `MERGE` em `ia.tb_diamond_nefrologia_clearance_saida` de todo exame com `valorClearance > 0` — **sem flag de relevante** | `ntb_ia_rim_modelo.py:335-363` | confirmado |
| view de envio (`ia.vw_gold_creatinina`) | `valorClearance BETWEEN 0.1 AND 60`, convênio **não** AMIL, `dataExameRecente <= current_date()` | `ntb_ia_rim_modelo.py:449-482` | confirmado |
| `qtd_relevante` do monitoramento | **`valorClearance BETWEEN 0.1 AND 35`**, não AMIL, `regionalUnidade` não nula; **`idade <= 80` desde 08/04/2026** (PR 6067); **exclusão de 44 `idUnidade` desde 24/04/2026** (PR 6166, *Retirando Unidades*) | `monitoring/ntb_ia_rim_monitoramento.ipynb:137-155`; commits `02c069f`, `53e16b2` | confirmado |
| `qtd_saida` do monitoramento | clearance entre 0,1 e 60 + mesmos filtros | idem :119-135 | confirmado |

**Contagens de ~6/dia (09/02–02/04) para ~62/dia:**

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 19/02/2026 | PR 5718 — *Monitoramento* (primeira versão do modelo e do monitoramento no repo) | `621c208` | confirmado |
| 31/03/2026 | PR 6019 — *Alterando modelo e monitoramento*: "alterar a lógica de update do modelo". No `WHEN MATCHED` do `MERGE`, `dt_execucao = current_date()` foi trocado por `dt_atualizacao = current_date()` | `cec789b`; diff em `ntb_ia_rim_modelo.py` | confirmado |

**Leitura (inferida, alta plausibilidade):** antes de 31/03, cada execução reprocessava 5 dias e
**movia o `dt_execucao` dos exames já gravados para o dia corrente**; o monitoramento agrupa por
`dt_execucao`, então os dias passados ficavam só com o que não foi reencontrado depois (~6/dia).
Depois de 31/03 o `dt_execucao` congela na primeira gravação e o dia passa a contar tudo o que
entrou (~62/dia). O "triplicar" não é aumento de volume real, é mudança de atribuição de data.

Duas implementações: `IAAzureDatabricksRim` (hml até 24/04, `main` em 28/05, PR 6406) e
`fabrica-ia-plataforma/apps/databricks/rim` (PR 6270 *feature/Rim_v1*, 15/05; PR 6627 *[Bug]
Clearance*, 18/06, em `apps/.../diamond/ntb_ia_diamond_modelo_captacao`). A cópia da
plataforma tem a mesma view 0,1–60 e o `MERGE` já com `dt_atualizacao`.

### E10 — Mama (BI-RADS)

| item | o que o código faz | fonte | confiança |
|---|---|---|---|
| modelo | extrai a categoria (`birads` = maior número achado após "birads"/"categoria"; −1 se nenhum). **Não há flag de relevante** na saída | `IAAzureDatabricksBiRads` `birads/model/ntb_ia_predicao.py:313-375` (branch `hml`) | confirmado |
| view de envio | `vw_diamond_mod_birads_complemento`: `num_ranking_laudo = 1` e campos cadastrais não nulos (nascimento, convênio, sexo, nome, telefone, unidade, regional, CNPJ). **Nenhum filtro de categoria** | `birads/serving/ntb_ia_views.py` (bloco "View que será consumida pela API") | confirmado |
| envio | a API Mule (`onco-navegadoras-exp-api…/navegadoras/inicial?dataInicial=…&dataFinal=…&flagIntegracao=false`) é chamada e **lê a view** — todas as categorias do dia | `birads/serving/ntb_ia_envio_api.py:128-170` | confirmado |
| `qtd_relevante` do monitoramento | `vl_proced_birads IN (4,5)` sobre a view de envio — **exclui BI-RADS 6** (não é "≥ 4") | `birads/monitoring/ntb_ia_birads_monitoramento.ipynb:149-160`; PR 5713 (19/02/2026) | confirmado |
| `qtd_envio` do monitoramento | contagem da view (todas as categorias) | idem :137-147 | confirmado |
| entrada | `mam[o\|a\|á]` em 4 campos, exclui biópsia; exclui unidade `RICHET%` | `birads/data/ntb_ia_entrada.py:180-225` | confirmado |

**Leitura:** o proxy "relevante = BI-RADS ≥ 4" deve ser **BI-RADS 4 ou 5** para bater com o
monitoramento. O envio (~20 mil/mês) é a view inteira, por desenho.

### E11 — Pulmão – Nódulo: envio menor que `encaminhamento = 'S'`

| item / data | o que o código faz | fonte | confiança |
|---|---|---|---|
| `exm_encaminhamento_nlp` | `'N'` se a justificativa começa com `EXCLUSAO` ou `CASO ACOMPANHADO`; senão `'S'` | `IAAzureDatabricksPulmao` `pulmao/model/ntb_ia_predicao.ipynb`, célula 102 | confirmado |
| tabela de envio (`tb_diamond_mod_pulmao_saida_salesforce`) | `CREATE OR REPLACE` a partir de `_saida_descripto`, que filtra **`exm_encaminhamento_nlp = 'S'` E `idunidade IN` (lista de 21 unidades de SP)** | idem, células 143 e 145 | confirmado |
| 12/01/2026 | envio restrito à regional SP (comentário: "adicionado em 2026-01-12 … descontinuado em 2026-04-07") | idem, célula 143 | confirmado |
| 21/02/2026 | PR 5730 — *ajustes idade - exames - encaminhamento = S - < 6mm - id unidades* (comentário: "adicionado em 2026-02-21 a pedido") | `99313b5` | confirmado |
| 04–05/03/2026 | PRs 5803 e 5823 — eliminação de unidades (Vivalle, Star, Antônio Afonso) | `406cc15`, `2c756d8` | confirmado |
| 19/03/2026 | PR 5919 — "configurar o envio para o SF de apenas **21 unidades** indicadas pelo time de navegação"; efeito "nos envios de 20-03-26 em diante" | `69601f4`; comentário na célula 143 | confirmado |
| 07–08/04/2026 | PR 6061 inclui unidades RJ (previsto para 08/04) e PR 6066 desfaz no mesmo dia ("no dia 8 foi informado que deveria ser cancelado") | `c35fb93`, `ba384d6` | confirmado |
| monitoramento | `qtd_envio` = linhas da tabela Salesforce com `encaminhamento = 'S'` desde `2026-01-28` | `pulmao/monitoring/ntb_ia_pulmao_monitoramento.ipynb:111-123` | confirmado |

**Leitura:** a diferença entre `encaminhamento = 'S'` (todas as unidades) e o enviado é o filtro de
21 unidades de SP — confirmado no código.

### E12 — Fígado (hepatologia) na plataforma: 21/08 a 14/10

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 19/08/2026 | primeiro deploy da plataforma na `main` (PRs 7068, 7070, 7073, 7077) com três jobs: `hepatologia-batch`, `tirads-batch`, `transplante-pulmao-batch`; `prod.json` com `pause_status: PAUSED` e `nlp_engine_version: latest` | `fabrica-ia-nlp-platform` `1d88643`, `0de0cc2` | confirmado |
| 20/08/2026 | PR 7083 — *alterando local do volumes* ("criado o local específico do volumes para ambiente de PRD") | `3a1098c` | confirmado (casa com 21/08) |
| 28/08 → 31/08/2026 | PR 7143 grava `pause_status: UNPAUSED` literal nas definições; chega à `main` em 31/08 (PR 7151) | `fd6e246`, `abe11fc` | confirmado |
| prd | arquivo de navegação de prd da hepatologia com **um único destinatário, interno do time técnico** em todos os blocos (as outras linhas têm 5 a 12) | `plataform/config/exchange/prd/ntb_ia_hepatologia_navegacao.py:56-74` | confirmado — explica "em paralelo sem entrega" |
| 14/10/2026 | PR 7605 — *incluindo variável para pausar Job*: `pause_status` vira `${job_status_<esp>_batch}`; em `prod.json`, **`job_status_hepatologia_batch: PAUSED`** e as demais `UNPAUSED`. Na `main` no mesmo dia (PR 7606) | `e018762`, `dab37c2`; `jobs/ambientes/prod.json:37` | confirmado |

Motivo da pausa: **não declarado** no PR. Inferido do contexto do time (linha sem o fluxo complementar
após o processamento; legado segue entregando).

### E13 — Ca de Estômago e Transplante de pulmão: plataforma desde 26/08

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 19/08/2026 | `transplante-pulmao-batch` e config `transplante_pulmao` já no primeiro deploy | `1d88643` | confirmado |
| 21–24/08/2026 | config `cancer_estomago` 0.6.2 (PR 7102, hml 24/08) e job `cancer-estomago-batch` na `main` (PR 7110, 24/08 21:26 UTC) | `723cb8d` | confirmado |
| 26/08/2026 | PR 7129 na `main` (filtros na view, regras de negócio, environment da exportação) | `96b6e89` | confirmado |
| até 31/08 | jobs de prd **pausados por padrão** (`prod.json`) | ver E12 | confirmado |
| 01–03/09/2026 | estômago config 0.6.9 (gate da úlcera isolada; PR 7166 hml, PR 7187 `main` 03/09) | `8a8a314` | confirmado |

**Leitura:** com os jobs pausados até 31/08, as execuções de 26/08 em prd foram **disparadas fora do
agendamento** (inferido — execução manual não fica no git).

### E14 — Ca de Rim: plataforma desde 10/09; hospitais 99 → 117

| data | o que mudou | fonte | confiança |
|---|---|---|---|
| 04/09/2026 | config `cancer_rim` (PR 7159 na hml) | `ea2ecd7` | confirmado |
| 10/09/2026 | PR 7251 — *Retirando os e-mails de prd para fazer a primeira carga seca*; PR 7252 leva `cancer-rim-batch` e `reumatologia-batch` à `main` (10/09 14:18 UTC) | `68bd751` | confirmado (casa com 10/09) |
| 11/09/2026 | PR 7256 — e-mail de navegação de rim, `disabled` para HML | `d92c99c` | confirmado |

Crescimento de hospitais set → out: **sem mudança na config de rim** depois de 18/09 (só PR 7321,
embeddings). O PR 7565 (09/10, *Escopo de envio = unidades navegadas*) mexe na view de envio, não
na saída do motor. **Sem fonte no git** — candidato: dado de entrada (gold).

### E15 — Tumor ósseo: plataforma desde 29/09

PR 7379 — *Migrar a captação para a plataforma: config 0.5.3* (hml 22/09); PR 7432 e PR 7442
(destinatários) na `main` em 28/09 → 1ª execução 29/09 (`b67c2c9`, `bc04f43`). Em 28/10, PR 7762 —
*Expansão de unidades: 4 → 82 ids, sem Pernambuco* (task `393668`). Confirmado.

### E16 — Envios

| item / data | o que mudou | fonte | confiança |
|---|---|---|---|
| legado | `qtd_envio` do monitoramento = `COUNT(exm_an) … WHERE fl_relevante = 1` (DII, Cólon, Ateromatose) | `IAAzureDatabricksDII` `monitoring/ntb_ia_dii_monitoramento.ipynb:185-188`; `IAAzureDatabricksColon` `colon/monitoring/…:162-165`; `fabrica-ia-plataforma` `…/ateromatose coronariana/monitoring/ntb_ia_ateromatose_monitoramento.py:48-52` | confirmado |
| 01/10/2026 | commit *Removendo envio de API da central*: tasks `api_` retiradas das 12 definições de job | `fabrica-ia-nlp-platform` `990cdfd` (hml); na `main` com PR 7538 (09/10) | confirmado |
| até 19/10/2026 | **não existia tabela de log de envio** na plataforma | — | confirmado pela data de criação abaixo |
| 19/10/2026 | `ntb_ia_data_saida_log.py` criado — grava `tb_mov_onedrive_saida` e `tb_mov_api_saida` (append, best-effort) — PR 7654 *Serviço de log para o processo de integração com serviços* (hml); `main` em 20/10 (PRs 7658, 7659, 7660) | `757eb09`; `c1c8439`, `23dc7a5` | confirmado |
| 20–22/10/2026 | PR 7666 *Hotfix/camada log*; PR 7690 *#60818 adicionando job_run_id* (hml 22/10; `main` em 29/10, PR 7782) | `5d771df`, `d468197` | confirmado |
| 29/10/2026 | PR 7768 — randomização em PRD para reumato e TI-RADS; `api_tirads` passa a postar a `_intervencao`; `main` em 29/10 (PR 7782) | `bfbd067`, `7c8d525` | confirmado (casa com API TI-RADS desde 30/10) |
| 04/11/2026 | PR 7840 — *Alteração da flag para true [Reumatologia]* — "para manter o fluxo de envios para a API da central"; `main` 04/11 (PR 7851) | `1eed577`, `a4f5f43` | confirmado (casa com reumato API em nov) |

**Leitura:** entre 29/09 e 23/10 não há log porque a tabela só passou a existir no código em
19–20/10; as primeiras linhas em prd (23–26/10) vêm depois do deploy de 20/10. O intervalo 20/10 →
23/10 não tem explicação no git (inferido: primeira execução com envio ligado / correção de 20–22/10).

### E17 — Unidades: códigos antigos até ~25/01/2026

| data | evidência | fonte | confiança |
|---|---|---|---|
| 29/01/2026 | três PRs no mesmo dia, mesmo work item `#49754`: Reumato PR 5596 e Pulmão PR 5595 — "**devido à mudança de padrão de id das unidades** foi necessário eliminar o id no left anti join"; DII PR 5597 — *Ajuste join id e an* | `e657041`, `9ac238c`, `b8ea5fa` | confirmado (mudança na gold antes de 29/01) |
| config de navegação | listas de unidade com pares antigo/novo comentados "cadastro legado parado jan/26" (ex.: 200139/101, 200021/134, 100999/5, 200020/97); lista de 21 unidades do Pulmão com os dois ids por hospital | `fabrica-ia-nlp-platform` `plataform/config/exchange/prd/ntb_ia_tirads_navegacao.py:122-127`; PR 7565 ("cadastros anteriores … parados desde jan/26"); Pulmão célula 143 | confirmado |

Motivo da mudança na gold: **não está em nenhum repo consultado** (a carga da gold é de outro time).

### E18 — Deploys da plataforma em prd

| deploy (`main`) | PRs | linhas que entraram (job novo na `main`) | 1ª execução agendada |
|---|---|---|---|
| 19–20/08 | 7068, 7070, 7073, 7077, 7083 | hepatologia, tirads, transplante_pulmao (jobs pausados por padrão) | — (prd pausado até 31/08) |
| 24/08 | 7110 | cancer_estomago | idem |
| 10/09 | 7252 | cancer_rim, reumatologia (cron `MON-FRI`, `UNPAUSED`) | 11/09 |
| **28/09** (20:18 UTC) | 7431 (reumato 0.2.0), 7432 + 7442 (tumor_osseo), **7444** *Atualização hml-main* | tumor_osseo, ateromatose_coronariana, cancer_colon, doenca_inflamatoria_intestinal, neuroimunologia | 29/09 |
| **09/10** (sexta) | **7538** *Atualização massiva* — "envio da randomização, pirads e doenças biliares" | doencas_biliares, pirads, randomizacao-grupo-controle | 12/10 |
| **13/10** | 7578 (promoção do que estava em hml), 7592 (*alterando importação*), 7595 (PI-RADS 0.1.3) | nenhuma linha nova | — |
| 14/10 | 7606 (pausa por variável; hepatologia `PAUSED`) | — | — |

Fonte: `git log origin/main --first-parent` em `fabrica-ia-nlp-platform` e diff de
`jobs/definicoes/` entre merges.

## Sem fonte no git

| evento | o que falta | onde se procurou |
|---|---|---|
| E02, E03, E07, E08 | datas de parada dos jobs legados (01/10, 28/09, 06/10, pausa 02/10) | logs dos 6 repos legados e de `fabrica-ia-plataforma` (todas as branches); jobs vivem no workspace |
| E07 | 79% de relevância da 1ª versão | commits e PRs de `IAAzureDatabricksBiliar` |
| E13 | quem disparou as execuções de 26/08 com o prd pausado | `jobs/ambientes/prod.json` e definições |
| E14 | crescimento de hospitais 99 → 117 | config e job de `cancer_rim`, PRs de exchange |
| E16 | intervalo 20/10 → 23/10 sem linha de log | PRs 7654–7690 |
| E17 | motivo da troca de códigos na gold | repos de linha; a gold não está nos repos consultados |
| E12 | motivo declarado da pausa de 14/10 | PR 7605 sem descrição além do título |

## Fontes consultadas

- `fabrica-ia-plataforma` (local, todas as branches; 81 PRs listados)
- `fabrica-ia-nlp-platform` (local, `origin/main` e `origin/hml`; 144 PRs listados)
- `IAAzureDatabricksReumatologia` (local, todas as branches)
- clonados só para leitura (sem checkout), fora de `Projects/`: `IAAzureDatabricksAteromatose`,
  `…Colon`, `…DII`, `…Biliar`, `…Neuroimunologia`, `…Rim`, `…BiRads`, `…Pulmao`, `…Hepatologia`,
  `…Rastreio` (só README), `IACaptacaoSolucoes`, `AzureDatabricksFabricaIA` (sem conteúdo relevante)
- `az repos pr list --status all` nos repos acima; `az repos pr show` para descrições de PR
- `fabrica-ia-lib/` (lib legada; nada sobre os eventos), `algoritmos/` (cópias locais dos legados,
  cobertas pelos clones), `plataform/` (runner local antigo; nada sobre os eventos)
