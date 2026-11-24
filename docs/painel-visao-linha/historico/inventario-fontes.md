# Histórico mensal das 16 linhas: inventário de fontes (jan a out/2026)

> Extraído em 19/11/2026 por `extrair_historico.py`, nesta pasta. Os arquivos contêm só agregados
> (contagem por linha, mês e unidade). Nenhum dado de paciente, laudo, CPF ou nome foi lido para
> fora do warehouse.
> Perfis: **NEW** = `nlp-platform` (workspace novo) · **OLD** = `adb-2013197995950192` (workspace antigo).

## Arquivos

| arquivo | conteúdo |
|---|---|
| `mensal-por-linha-unidade.json` | `{linha, fonte, tabela, recorte, mes, id_unidade, exames, relevantes}`: exame **distinto**, contado no **primeiro mês em que aparece** na fonte (o histórico inteiro da tabela entra no cálculo, então um exame reprocessado não é contado de novo) |
| `mensal-monitoramento-legado.json` | contagens do **próprio job legado** (`entrada`, `saida`, `relevante`, `envio`, `navegacao`) somadas no mês, por **nome** de unidade. Não são deduplicadas |
| `enviados-mensal.json` | envios por linha × mês × unidade (detalhes na seção "Enviados") |
| `mensal-consolidado-linha.json` | linha × mês com a fonte escolhida pela regra abaixo, mais as outras fontes que existem no mesmo mês |
| `controles.json` | data mínima e máxima, total de linhas e dias com dado em cada tabela (é o controle contra "zero suspeito") |

## Regras de contagem

- **Exame:** id distinto (`id_exame` na plataforma, `exm_an`, `AN`, `idExame` ou `an` no legado). Ele entra no
  mês da 1ª aparição (data de execução do modelo). A unidade é a da 1ª aparição.
  **Relevante** = o exame foi marcado relevante em qualquer execução.
- **Recorte:** coluna `recorte`. Na plataforma, Cólon e DII separam `colonoscopia` (texto de `exm_mod`/`exm_tipo`
  com colonosc, retossig ou polipect) de `imagem`. No legado, o recorte é a própria tabela (`imagem_tc` ou
  `colonoscopia`; nas biliares, `v1`/`v2`/`v3_snapshot`; `pacs_enave`).
- **Fonte escolhida no consolidado:** plataforma a partir do **1º mês completo** nela, ou seja, o mês seguinte
  ao 1º dia de dado, salvo quando esse dia é 1 ou 2. Antes disso, legado, na ordem de prioridade da linha.
  Linhas sem legado usam o mês parcial da plataforma, marcado `plataforma_prd (mes parcial)`.
  **Exceção: Fígado fica no legado o ano todo.** A plataforma rodou em prd só de 21/08 a 14/10, em paralelo e
  sem entrega (é o motor legado que alimenta a navegação).
- **Unidade:** `id_unidade` é **id numérico como texto** em todas as fontes por exame. O monitoramento traz o
  **nome** da unidade (`unidade_nome`). Exames com unidade nula: 1.667 de 7,3 mi (0,02%).

## Fontes por linha

| linha | fonte (perfil) | id / unidade / relevância / data | dado de → até | papel |
|---|---|---|---|---|
| **Mama (BI-RADS)** | `diamond_birads.birads.tb_diamond_mod_birads_saida` (OLD) | `id_exame` / `id_unidade` / **`vl_proced_birads >= 4`** (proxy, ver abaixo) / `dt_execucao` | jan/24 → 18/11/26 | legado, o ano todo |
| | `hive_metastore.ia.tbl_gold_modelo_birads_saida` (OLD) | — | até ago/25 | antiga, fora do período |
| **Rim (nefrologia)** | `hive_metastore.ia.tb_diamond_nefrologia_clearance_saida` (OLD) | `idExame` / `idUnidade` / **sem flag** / `dt_execucao` | 07/10/25 → 18/11/26 | legado, o ano todo |
| | `diamond_nefrologia.nefrologia.tb_diamond_mod_rim_saida` (OLD) | tem `fl_relevante` | só mai/26 (carga única) | versão parada em 13/05, descartada |
| **Fígado** | `diamond_hepatologia.hepatologia.tb_diamond_mod_hepatologia_saida` (OLD) + unidade via `…_entrada_v2` | `id_exame` / `id_unidade` (join) / `fl_relevante` / `dt_execucao` | jan/24 → 18/11/26 | legado, o ano todo |
| | `diamond_fabrica_ia.hepatologia.tb_mod_diamond_hepatologia_saida_v0` (NEW) | padrão plataforma | 21/08 → 14/10/26 | paralelo, não entrega |
| **Pulmão – Nódulo** | `hive_metastore.ia.tb_diamond_mod_pulmao` (entrada, OLD) + `…_pulmao_saida` | `an` / `idunidade` / **`exm_encaminhamento_nlp = 'S'`** na saída / `dataExecucaoModelo` | 27/01 → 18/11/26 | legado, o ano todo. A saída casa 100% com a entrada (26.793 de 26.793) |
| **DII** | `diamond_fabrica_ia_hml.legado.tb_diamond_mod_dii_saida` (imagem) e `…_dii_colonoscopia_saida` (NEW) | `exm_an` / `idunidade` / `fl_relevante` / `dataExecucaoModelo` | 09/01 → 28/09 e 17/03 → 28/09 | legado jan–set |
| | `diamond_fabrica_ia.doenca_inflamatoria_intestinal.…_saida_v0` | padrão plataforma | 29/09 → | plataforma a partir de out |
| **Reumatologia** | `diamond_fabrica_ia_hml.legado.tb_diamond_mod_reumatologia_saida` | idem legado | 29/01 → 16/09 | legado jan–set |
| | `diamond_fabrica_ia.reumatologia.…_saida_v0` | padrão plataforma | 11/09 → | plataforma a partir de out |
| **Ca de Cólon** | `…hml.legado.tb_diamond_mod_colon_saida` (TC/imagem) e `…_colon_colonoscopia_saida` | idem legado | 20/02 → 01/10 e 30/03 → 01/10 | legado fev–set |
| | `diamond_fabrica_ia.cancer_colon.…_saida_v0` | padrão plataforma (na prática só colonoscopia) | 29/09 → | plataforma a partir de out |
| **Doenças biliares** | `mlops_fabrica_ia.operacao.tb_nlp_platform_historico_legado` (NEW) | monitoramento do job: `qtd_saida` / `qtd_relevante` por nome de unidade | 03/04 → 06/10 | legado abr–out (contagem do job) |
| | `…hml.legado.tb_diamond_mod_doencas_biliares_saida(_v2)`, `hive…_saida_v2`, `hive…_saida_v3` | por exame | **1 a 5 dias cada** (abr, jun, set, out) | tabelas sobrescritas a cada versão, servem só de referência |
| | `diamond_fabrica_ia.doencas_biliares.…_saida_v0` | padrão plataforma | 12/10 → | plataforma a partir de nov |
| **Neuroimunologia** | `…hml.legado.tb_diamond_mod_neuroimunologia_saida` | idem legado | 29/04 → 01/10 | legado abr–set |
| | `diamond_fabrica_ia.neuroimunologia.…_saida_v0` | padrão plataforma | 29/09 → | plataforma a partir de out |
| **Próstata (PI-RADS)** | `diamond_pirads.pirads.tb_diamond_mod_pirads_saida` (OLD) | `exm_an` / `idunidade` / `fl_relevante` (= PI-RADS 4–5) | 27/05 → 14/10 | legado mai–out |
| | `hive_metastore.ia.enave_df_historico_pirads` (OLD) | `AN` / `idUnidade` / **`pirads >= 4`** / `data_carga` | 2022 → 12/11/26 | captação PACS (enfermeira navegadora), cobre jan–abr |
| | `diamond_fabrica_ia.pirads.…_saida_v0` | padrão plataforma | 12/10 → | plataforma a partir de nov |
| **Ateromatose** | `…hml.legado.tb_diamond_mod_ateromatose_coronariana_saida` (= `diamond_ia.ateromatose_coronariana…` no OLD) | idem legado | 24/06 → 02/09 | legado jun–set |
| | `diamond_fabrica_ia.ateromatose_coronariana.…_saida_v0` | padrão plataforma | 29/09 → | plataforma a partir de out |
| **Tireoide (TI-RADS)** | `diamond_tirads.tirads.tb_diamond_mod_tirads_saida` (OLD) | `exm_an` / `idunidade` / `fl_relevante` (= TI-RADS 4–5) | 15/06 → 02/09 | legado motor, jun–ago |
| | `hive_metastore.ia.enave_df_historico_tirads` (OLD) | `AN` / `idUnidade` / **`tirads >= 4`** / `data_carga` | 2022 → 12/11/26 | captação PACS, cobre jan–mai |
| | `diamond_fabrica_ia.tirads.…_saida_v0` | padrão plataforma | 21/08 → | plataforma a partir de set |
| **Ca de Estômago** | `diamond_fabrica_ia.cancer_estomago.…_saida_v0` | padrão plataforma | 26/08 → | só plataforma (ago parcial) |
| **Transplante de pulmão** | `diamond_fabrica_ia.transplante_pulmao.…_saida_v0` | padrão plataforma | 26/08 → | só plataforma (ago parcial) |
| **Ca de Rim** | `diamond_fabrica_ia.cancer_rim.…_saida_v0` | padrão plataforma | 10/09 → | só plataforma (set parcial) |
| **Tumor ósseo** | `diamond_fabrica_ia.tumor_osseo.…_saida_v0` | padrão plataforma | 29/09 → | só plataforma (set quase vazio) |

"Padrão plataforma" = `id_exame` / `id_unidade` / `fl_relevante` / `dt_execucao_modelo`, catálogo `diamond_fabrica_ia` (prd), perfil NEW.

### Por que o legado vem de `diamond_fabrica_ia_hml.legado` e não do `hive_metastore.ia`

Para Cólon, DII, Reumatologia, Neuro, Ateromatose e Biliares, as duas cópias são **idênticas mês a mês de
jan a ago** (conferido no perfil mensal). O hive parou em 02/09; a cópia `hml.legado` seguiu recebendo do
job legado até 16/09–01/10. O espelho `diamond_fabrica_ia.legado` parou em set e **não foi usado**.

### Sobreposições (legado e plataforma no mesmo mês)

| linha | mês | o que acontece |
|---|---|---|
| Fígado | ago–out | plataforma em paralelo, sem entrega; vale o legado |
| Tireoide | ago | legado motor até 02/09; plataforma desde 21/08. Vale o legado (mês incompleto na plataforma) |
| Reumatologia | set | legado até 16/09, plataforma desde 11/09 |
| Cólon, DII, Neuro | set (fim) | legado até 28/09–01/10, plataforma desde 29/09 |
| PI-RADS | out | legado até 14/10, plataforma desde 12/10. Vale o legado, e out fica parcial nos dois |
| Biliares | out | legado até 06/10, plataforma desde 12/10. Vale o legado, e out fica parcial nos dois |
| Ateromatose | set | **lacuna**: legado parou em 02/09, plataforma só começou em 29/09 |

## Proxies de relevância, declarados

- **Mama:** BI-RADS ≥ 4 (inclui 6), a régua do estudo de migração v4. O legado envia **todas** as categorias
  às navegadoras: o `qtd_envio` do monitoramento fica em ~20 mil/mês e não é número de relevantes.
- **Nefrologia:** a tabela de saída não tem flag. Para cada exame vão dois proxies:
  `relevantes_proxy_clearance_lt60` (~18 mil/mês, que bate com o `qtd_saida` do monitoramento, 17,6 mil em ago)
  e `_lt30`. **O número declarado pelo próprio job** é o `relevante` do monitoramento (~3 mil/mês desde abr;
  10,7 mil e 13,7 mil em fev e mar, provável reprocessamento). No consolidado, `relevantes` da nefro é `null`.
- **TI-RADS e PI-RADS via PACS (enave):** categoria ≥ 4, a mesma régua do motor legado. Ela foi conferida na
  tabela do motor: `fl_relevante = 1` só em TR/PI-RADS 4 e 5.
  ⚠️ A fonte enave conta **6 como relevante** (250 casos de TR6 e 0 de PI-RADS 6 em 2026), e o motor não.
  A diferença é desprezível, mas existe.
- **Pulmão:** relevante = `exm_encaminhamento_nlp = 'S'` (~1,5–1,8 mil/mês). O job envia menos (~0,9–1,0 mil/mês).

## Enviados (`enviados-mensal.json`)

| fonte | linhas | período | o que mede |
|---|---|---|---|
| `plataforma_onedrive_prd` | 13 da plataforma | 23–26/10 → | `exame_an` distinto em `mlops_fabrica_ia.operacao.tb_mov_onedrive_saida`, unidade via tabela de saída (100% casou) |
| `plataforma_api_prd` | tirads, reumatologia | 30/10 → (reumato só em nov) | `tb_mov_api_saida` |
| `legado_monitoramento_qtd_envio` | 12 projetos legados | jan → out | `qtd_envio` do `hive_metastore.ia.tb_diamond_mod_monitoramento`, por nome de unidade. ⚠️ Em Cólon, DII, Reumato, Neuro, PI-RADS, Ateromatose, Tireoide e Biliares ele é **igual ao número de relevantes do dia**: indica linha posta no arquivo de envio, não confirma entrega. Para a Mama, inclui todas as categorias. Nefro não tem envio |
| `legado_hepatologia_navegacao_v2` | Fígado | jun → | linhas de navegação enviadas (`id_navegacao` distinto; o `id_exame` vem nulo nessa tabela) |

**Não mensurável:** envio da plataforma antes de 23/10. Entre 29/09 e 23/10 não há log de envio em prd. O
`tb_mov_randomizacao_log` tem `fl_enviado` desde 12/10 (reumato e TI-RADS), mas o campo registra **decisão**,
não envio, e por isso ficou de fora. O `tb_log_envio_central_captacao` só tem hml. A navegação da hepatologia
antes de jun/26 está na `…_navegacao` sem `_v2`, que tem `id_exame` nulo; ela não foi extraída.

## Lacunas e ressalvas

- **Sem dado (—):** Cólon em jan; Biliares jan–mar; Neuro jan–mar; Ateromatose jan–mai; Ca Estômago e
  Transplante jan–jul; Ca Rim e Tumor ósseo jan–ago. Nos três últimos casos a linha não existia antes da
  plataforma. Nenhum legado com esses nomes foi encontrado nos dois workspaces (hive `ia`, `diamond_*`,
  `diamond_ia*`).
- **Tireoide, jun:** o motor legado estreou em 15/06 com 55 mil exames (carga retroativa), e o mês fica inflado.
- **DII, jan–fev:** relevância de ~5,6% contra ~0,6% depois. É régua diferente, não volume.
- **Ca Rim, set:** 182 mil exames, entrada inicial maior que o regime (120 mil em out).
- **Ca de Cólon na plataforma:** o recorte `imagem` sai praticamente vazio. A linha da plataforma cobre
  colonoscopia; a TC que o legado tinha (`colon_saida`) não tem equivalente a partir de out.
- **Biliares, legado:** as contagens vêm do job (não deduplicadas). As tabelas por exame são snapshots de 1 a
  5 dias, e a de abr (`saida`, 79% de relevância) é piloto.
