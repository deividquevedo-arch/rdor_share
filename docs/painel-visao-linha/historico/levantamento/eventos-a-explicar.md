# Eventos a explicar — histórico mensal jan–out/2026 (16 linhas)

Cada evento abaixo foi **observado nos dados** (contagens agregadas, mês pela data do exame). O
levantamento busca, para cada um: **o que aconteceu, quando (data exata), por quê, e a fonte**
(arquivo:linha, commit sha + data, card número + título, job/run id, documento).

| id | linha | evento observado nos dados |
|---|---|---|
| E01 | Ateromatose | legado estreia em 24/06; relevantes caem jun 1.285 → jul 567 → ago 132; legado roda até 01/10; plataforma a partir de 29/09 |
| E02 | Ca de Cólon | legado estreia em 20/02 (só TC/imagem, `colon_saida`); colonoscopia (`colon_colonoscopia_saida`) entra em 30/03; fev com só 138 relevantes; legado até 01/10; plataforma (29/09) cobre só colonoscopia — corte de escopo de 21/08 (commit `6c458da` em `fabrica-ia-plataforma`) e job legado pausado em 02/10 (card `360625`) |
| E03 | DII | relevância 5,9% em jan e 3,6% em fev contra ~0,6% a partir de mar (mudança de régua?); colonoscopia entra em 17/03; legado até 28/09; plataforma 29/09 |
| E04 | Reumatologia | volume e hospitais crescem jan → abr (21 mil → 135 mil exames; 51 → 74 hospitais); queda em ago (região craniana removida no PR 6893, 23/07?); legado até 16/09; plataforma desde 11/09 |
| E05 | Tireoide (TI-RADS) | antes de 15/06 só captação manual PACS (`hive_metastore.ia.enave_df_historico_tirads`); motor legado estreia 15/06 com carga retroativa de jan–jun; legado até 02/09; plataforma desde 21/08 com volume ~3,5x (filtro de entrada mais amplo) |
| E06 | Próstata (PI-RADS) | captação manual PACS (enave) até mai; motor legado desde 27/05; plataforma desde 12/10 com volume ~3x |
| E07 | Doenças biliares | legado desde 03/04 (piloto, 79% relevância na 1ª versão); tabelas `_saida`, `_saida_v2`, `_saida_v3` sobrescritas a cada versão; contagem do job até 06/10; plataforma desde 12/10 |
| E08 | Neuroimunologia | legado desde 29/04; até 01/10; plataforma 29/09 |
| E09 | Nefrologia (clearance) | tabela de saída sem flag de relevante; monitoramento do job: de ~6 para ~62 registros/dia entre 09/02 e 02/04 (contagens triplicam); `qtd_relevante` do job ≈ clearance < 30 por exame — **confirmar a regra no código**; duas implementações (IAAzureDatabricksRim × versão UC parada em 13/05) |
| E10 | Mama (BI-RADS) | relevante usado = BI-RADS ≥ 4 (proxy); o envio do legado inclui todas as categorias (~20 mil/mês) — **confirmar no código/documentação** |
| E11 | Pulmão – Nódulo | relevante = `exm_encaminhamento_nlp = 'S'`; job envia menos que isso (~0,9–1,0 mil × 1,5–1,8 mil/mês) — por quê? |
| E12 | Fígado (hepatologia) | legado o ano todo; plataforma em prd só de 21/08 a 14/10, em paralelo sem entrega — por que começou e por que parou |
| E13 | Ca de Estômago, Transplante de pulmão | plataforma desde 26/08 (estreia em prd) |
| E14 | Ca de Rim | plataforma desde 10/09; hospitais crescem 99 (set) → 117 (out) |
| E15 | Tumor ósseo | plataforma desde 29/09 |
| E16 | Envios | legado: `qtd_envio` do monitoramento = relevantes do dia; plataforma: log OneDrive (`tb_mov_onedrive_saida`) só desde 23–26/10, API TI-RADS desde 30/10, reumato API em nov; entre 29/09 e 23/10 sem log de envio — por quê |
| E17 | Unidades | códigos de unidade antigos (faixas 1009xx, 1010xx, 2000xx, 2001xx) até ~25/01/2026, depois códigos novos — quando e por que mudou na gold |
| E18 | Geral | datas de deploy da plataforma em prd: 28/09, 09/10, 13/10 (e quais linhas entraram em cada) |

**Regras:** só leitura. Nada de push, post, voto, alteração de card ou job. Nenhum dado de paciente
(laudo, CPF, nome): só metadado, código, documento, contagem agregada. Registro impessoal.
