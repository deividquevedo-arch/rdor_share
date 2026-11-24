# Levantamento no board — eventos E01–E18

> **Fonte:** Azure DevOps, org `RedeDor-corp`, projeto `IA` (work items: descrição, critério de aceite,
> repro steps, comentários e histórico de estado). Levantamento **só leitura** em 19/11/2026 — nenhum
> card foi comentado, movido ou editado. Nenhum dado de paciente foi copiado (há cards com nome/CPF
> em comentário, ex. `214346`; só o metadado é citado aqui).
>
> **Como ler:** as datas são as do comentário ou da mudança de estado no card. "Confiança" diz se o
> board **afirma** o fato (alta), se o fato é **inferido** de card próximo (média) ou se é **indício**
> que precisa de outra fonte (baixa).
>
> ⚠️ **Armadilha do board:** em 20/08/2026 houve um encerramento em massa — centenas de cards antigos
> têm `Encerrado` com data `2026-08-20`. **Essa data não é a data da entrega.** A data útil está no
> histórico de estado anterior (`Concluído`, `Liberado`) e nos comentários.

---

## E01 — Ateromatose

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Pipeline de envio diário com início planejado para 19/06; erro de cluster em 24/06 resolvido, "já está funcionando" em 25/06 | 19/06 (plano) · 25/06 | — | `172767` — *[Ateromatose Coronariana] Pipeline de envios diários automáticos - Inicio envios 19/6* (comentários 15/06, 25/06) | alta |
| Volume previsto de **8.800 envios/mês**, acima da capacidade de navegação → algoritmo mais específico (só TC de tórax; incipiente/discreta deixa de ser relevante) | história de 15/06; concluída 01/07 | capacidade da navegação | `182295` — *[Ateromatose Coronariana] Melhorias Algoritmo (especificidade)* | alta |
| **Filtro de idade 45–70 anos** nos envios | concluído 07/07; aplicado **15/07/26** | pedido do negócio; "reduz volume de exames lidos para 1/3 do original" | `194937` — *[Ateromatose Coronariana] Filtro de idade - 45 a 70 anos*; data de efeito em `299310` — *[Captação] Melhoria algoritmo ateromatose (aumento volume pacientes)* | alta |
| **Só "difusa" e "importante"** em TC de tórax (commit `73e7a67`) | aplicado **20/07/26** | pacientes "com poucas chances" de procedimento complexo | `197189` — *[Ateromatose Coronariana] Ajuste Findings TC de Torax* (comentário 13/07); data em `299310` e `286925` — *[Ateromatose][Estudo] Reavaliar Recall do algoritmo Ateromatose* | alta |
| Doppler de carótidas acrescentado (350 positivos → 295 coronária + 55 carótida) | 17/07 | casos mais graves | `197217` — *[Ateromatose Coronariana] Adicionar exame doppler de carótidas* | alta |
| Queda de volume no Oeste D'Or atribuída ao filtro de idade | 23/07 | filtro de idade pedido pelo negócio | `214390` — *[Ateromatose] Volume de Entrada no Oeste D'Or baixo* | alta |
| **Envios interrompidos de 03/08 a 13/08**: job no cluster errado (fora do ambiente FIA), conflito `numpy`/`thinc` | 03/08 → 13/08 | troca de runtime do cluster | `279307` — *[Ateromatose Coronariana] Interrupção dos envios desde 3.8*; `279331` — *Erro ateromatose - 13/08* | alta |
| Volume em produção legado: ~130–190 envios/mês; sem o filtro de "difusa/importante" iria a ~1.400/mês | medido 16/10 | — | `303427` — *[Estudo] Avaliar volume de envios em rollback Ateromatose* | alta |
| Job legado pausado e tasks api/onedrive desativadas no workspace `fia-hml` | **02/10** | envio duplicado HML (legado) + PRD | `360625` — *[Captação] Envios de e-mail duplicados em Neuro, Colon e Ateromatose* | alta |
| Plataforma em produção | 29/09 (deploy 28/09) | migração | `303791` — *Plano de Migração algoritmos final (randomização e central captação)* (descrição de 14/10) | alta |
| Rollback de escopo na plataforma (config `0.4.0`): ~7/dia → 33 no 1º dia | 12/11 | baixo volume | `299310` (comentário 12/11); `399835` — *[Ateromatose] Achados de Ateromatose Coronariana Importante não estão sendo enviados* | alta |

**Leitura:** a queda jun 1.285 → jul 567 → ago 132 é explicada por três decisões de negócio
(especificidade 01/07, idade 15/07, só difusa/importante 20/07) e pela interrupção de 03–13/08.

## E02 — Ca de Cólon

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Algoritmo TC/RM homologado pelo negócio em 30/01; operação assistida até 20/02 | 30/01 · 06/02 | — | `142395` — *Desenvolvimento Algoritmo Colon (TC) V2* (comentário 06/02) | alta |
| Envio manual de 1 semana (25–31/01) para operação assistida | 09/02 | — | `143534` — *[Colon TC e RM] Envio manual 1 semana de laudos - operação assistida* | alta |
| **Pipeline diário TC/RM:** dev em 19/02, "navegação já deu OK, subir para HML" em **20/02** | 20/02 | — | `137070` — *[Cólon] Pipeline de envios diários para Sharepoint - TC e RM*; `142851` — *[Colon] Automação dos envios diarios algoritmo Colon (TC)* (Liberado 20/02) | alta |
| Colonoscopia: negócio valida envio **sem pólipo** (26/03); só "ulceração" deixa de ser enviada | 06/03 · 26/03 | precisão | `147767` — *Homologação Colon Colonoscopias*; `152462` — *[DII][Ca de Colon] Atualizar Datacards com algoritmos Colonoscopias* | alta |
| Colonoscopia: rodou em dev sem parâmetros (24–27/03); cópia manual DEV→HML em 27/03; parâmetros de HML em **30/03** (1ª execução 8 h por acúmulo) | 27/03 · 30/03 | parâmetro ausente no job | `147936` — *[Ca de Colon Colono] Pipeline de envios diários automáticos para Sharepoint - Colonoscopias*; `153614` — *[Ca Colon] Bug no envio de arquivos para OneDrive* | alta |
| Negação de "rastreamento" (indicação lida como diagnóstico) | 09/07 | FP | `195170` — *[Ca de Colon] Algoritmo confunde indicação com diagnostico* | alta |
| Refatoração por FP de pólipo ressecado ("lesao") | 06–17/08 | baixa precisão | `260064` — *[Ca de Colon] Revisão do algoritmo - Baixa Precisão - Muitos FP* | alta |
| **Legado congelado na v4.6 (decisão 20/08)**; régua LST v5.0 (PR 7109) mergeada em HML em 24/08 | 20/08 · 24/08 | evolução passa a ser config na plataforma | `283293` — *[Ca de Colon] Migração para NLP Engine + revisão da régua (LST)* | alta |
| Corte de escopo da plataforma para colonoscopia (`6c458da`, 21/08) | — | **não registrado no board** | — | sem fonte |
| Job legado pausado | **02/10** | envio duplicado | `360625` | alta |

## E03 — DII

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| V1 TC/RM em janeiro com vocabulário amplo (inclui `ileite`, `colite inespecífica`, `proctite` etc.); apresentação ao negócio mostra que exames/algoritmo "não trazem valor" | 23/01–09/02 | — | `142542` — *[DII] Ajustes Algoritmo DII - Órgãos incorretos TC*; `143002` — *[DII] Analise Algoritmos DII p/ Stakeholder TC e RM* | alta |
| **Novo escopo "cenário 4":** ~42 envios/mês com 50% de precisão, lista reduzida (DII explícito + complicações) | acordado ~05/02; homologação 23–27/02 | custo × retorno (R$ 150 mil/ano por paciente) | `144769` — *[DII] Desenvolvimento Algoritmo TC e RM - Novo escopo acordado com negócio*; `143856` — *[DII] Ajustes algoritmo TC e RM - Cenário 4* | alta |
| **Pipeline DII pausado e envios antigos movidos para pasta `Old`** até homologação | **10/03** | não usar algoritmo não homologado | `146643` — *[DII] Limpar pasta DII dos envios antigos* | alta |
| Homologação TC/RM validada; "de acordo" do PO por e-mail | 16–17/03 | — | `149802` — *DII TC e RM - Validar Homologação* | alta |
| Colonoscopia + TC/RM no mesmo arquivo: dev desde 13/03, teste manual em HML e **automático a partir de 20/03** (04h) | 13/03 · 20/03 | — | `144568` — *[DII Colono] Pipeline de envios diários automáticos para Sharepoint - Colonoscopias* | alta |
| Legado até 28/09 | — | não há card de desligamento específico de DII (o `360625` cita só neuro, cólon e ateromatose) | — | sem fonte |

**Leitura:** a relevância de 5,9% (jan) e 3,6% (fev) é a V1 ampla; a queda para ~0,6% a partir de
março é a troca para o cenário 4 (pausa em 10/03, nova versão em 20/03).

## E04 — Reumatologia

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Pipeline diário automatizado (SP/RJ/BA/Outras) em dev em 22/01; acesso do negócio em 30/01; feature "Liberado" 04/02 | 22/01 · 30/01 · 04/02 | — | `140946` — *[Reumato]  Pipeline de envios diários automatizado para Sharepoint*; `142269` — *Automação dos envios diarios algoritmo Reumato* | alta |
| Filtro de idade até 80 anos **removido** | 22/01 | — | `140946` (comentário 22/01) | alta |
| **Migração do ID de unidade (MDM) em 26/01**: "volumetria aumentou, surgiram novas unidades" (JSON de reumato atualizado) | 26/01 → 30/01 | ver E17 | `135635` — *26/01: Migração do ID das Unidades MDM na tabela Observation Exames (Engenharia)*; `143013` — *Atualizar JSON de cada projeto: Figado, DII, Colon e Reumato* | alta (crescimento de hospitais); média (crescimento de exames) |
| Troca do campo de nome do exame para `proced_descricao_ajustado` (coalesce de 3 campos); feito para reumato em 18/03, com `NOT LIKE` vascular em 25/03 | 18/03 · 25/03 | 37% de nulos no campo antigo derrubavam volume | `149565` — *[Captação] Substituir campos de nome do exame em todos os algoritmos*; `149021` — *[Captação] Estudo campo de descrição dos exames* | alta |
| **Remoção de face/seios da face/crânio/mastoide** da entrada | PR em **24/07** (bug aberto 21/07) | FP em exames sem parte do corpo relevante | `211227` — *[Reumatologia] Exames não relevantes sendo enviados (FP)* | alta |
| Plataforma: merge em HML 10/09; card de migração fechado 15/09; piloto da Central com reumato | 10/09 · 15/09 | — | `299111` — *[Migração][Reumato] Migrar algoritmo para nova estrutura Fabrica IA + NLP Engine*; `285303` — *[Fábrica IA] Subida em PRD Reumatologia na Central Navegação* | alta (HML); sem data de PRD no board |
| Legado fora do ar desde 03/09 (Hugging Face) | 03/09 → ~08/09 | erro ao baixar modelo | `299525` — *[DII][Reumato][Doencas Biliares][Colon][Ateromatose][Hepato] Erro ao baixar modelo Hugging Face* | alta |
| Legado até 16/09 | — | **não registrado no board** | — | sem fonte |

## E05 — Tireoide (TI-RADS)

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Fluxo antigo de TI-RADS/PI-RADS no **ADF** disparando e-mail; negócio "resolveu abrir" o e-mail em 23/02; pedido de paralisar | 24–25/02 | e-mail de JSON antigo | `146157` — *[Tireoide][Prostata] Entender envios de Ti-Rads e Pi-Rads que foram feitos para negocio*; `146159` — *Paralisar os envios dos e-mails*; `146155` — *[Estudo][Tireoide] Análise crítica dos Algoritmos Legado que podem ser aproveitados Ti-Rads* ("está no ambiente produtivo, porem no ADF") | alta |
| **Motor legado V1: início da navegação 15/06/2026** (dev → HML em 12/06) | **15/06** | — | `172770` — *[Tireoide V1] Pipeline de envios diários automáticos* (comentários 11–12/06) | alta |
| Carga retroativa jan–jun | — | **não registrada no board** | — | sem fonte |
| Hospital Aliança (BA) incluído | 15/07 | — | `206868` — *[Tireoide][Prostata][Doenças Biliares] Envios Hospital Aliança (BA)* | alta |
| Plataforma: TI-RADS em **produção em 21/08** (config `0.8.0-tirads`, engine `0.9.4`, 31.418 laudos, 997 relevantes) depois da quebra de 20/08 | 20/08 · 21/08 | `main` da lib parada na `0.1.0` | `282904` — *[NLP Engine] Problema no arquivo de configuração TIRADS* | alta |
| Job da plataforma em HML com destinatários | 24/08 | — | `195008` — *[Tireoide] Deploy da V2 até 17.08* | alta |
| Envio da plataforma trazendo "quase 2 meses de exame" em vez de D-1 | 17/08 | — | `280589` — *[Tireoide] Campos de envios com erro* | média (indício do volume ~3,5×) |
| Volume ~3,5× por filtro de entrada mais amplo | — | **não afirmado no board** | — | sem fonte |
| Legado até 02/09 | — | **não registrado no board** | — | sem fonte |

## E06 — Próstata (PI-RADS)

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| PI-RADS antigo no ADF (mesmo e-mail do TI-RADS) | 24/02 | — | `146157` | alta |
| Algoritmo novo: filtro de entrada RM de pelve/próstata + US transretal **com "biópsia"** | 04/05 | TC não é relevante; PI-RADS só em RM | `134317` — *[Prostata] Desenvolvimento do algoritmo* | alta |
| **Envios iniciam 27/05** (execução teste de 26/05 descartada) | **27/05** | — | `172763` — *[Prostata] Pipeline de envios diários automáticos. Partida 27.5* | alta |
| Plataforma: correção "PIRADS RM 4" (`0.1.2`) em 30/09; carga seca em PRD em 13/10; 1ª execução em 12/10 | 30/09 · 12/10 · 13/10 | — | `358427` — *[Migração][Prostata] Migrar algoritmo para nova estrutura Fabrica IA + NLP Engine*; `303791` | alta |
| Envio em dobro (legado + plataforma) e legado desativado | ~14/10 | — | `373805` — *Envios Retroativos* ("Enviamos 2 vezes, parar legado; legado já desativado") | alta |
| Volume ~3× na plataforma | — | **não registrado no board** | — | sem fonte |

## E07 — Doenças biliares

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Discovery: ~5 mil exames/mês, ~100 achados/mês esperados | 26/02 | — | `144303` — *Discovery Doenças Biliares* | alta |
| Algoritmo com expansão semântica (`min_sim_seed_vocab 0.65`, `top_k_per_seed 24`) | 16/03 | — | `146409` — *[Doenças Biliares] Desenvolvimento do algoritmo Doenças Biliares* | alta |
| **Pipeline em produção (hml) — arquivos de 03/04 e 06/04**, agendado 04h | **03/04** | — | `150735` — *[Doencas Biliares] Pipeline de envios diários automáticos para Sharepoint* (comentários 06/04) | alta |
| V1 com FP de colelitíase (cálculo renal), precisão 70–80% | 14/04 | — | `164296` — *[Doenças Biliares] Melhoria na detecção de achados de colelitiase* | alta (relevância alta da 1ª versão) |
| POC Tabia: inclusão de pronto-socorro (04/05); bug PS indo para todas as unidades corrigido 26/05 | 04/05 · 26/05 | — | `167875` — *[Doenças Biliares] [POC Tabia] Envio de amostras de resultados com valor ao negócio* | alta |
| **V3 em produção** (homologada, PR concluído, pipeline atualizado) | **24/06** | precisão | `174104` — *[Doenças Biliares] Deploy V3 do algoritmo - inicio 25/6* | alta |
| Rollout de 5 unidades (PR, MG, PB, AL, SE) | 25/06 | — | `185291` — *[Doenças Biliares][Tireoide][Ateromatose] Rollout 5 unidades* | alta |
| Tabelas `_saida`, `_v2`, `_v3` sobrescritas | — | **não registrado no board** | — | sem fonte |
| Legado sem rodar desde 07/10 | 07/10 | — | `373805` | alta |
| Plataforma desde 12/10 (deploy 09/10) | 12/10 | — | `303791` | alta |

## E08 — Neuroimunologia

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Algoritmo: 54 positivos em 2.889 RM de SNC, precisão ~94% | 23/03 | — | `149142` — *[Neuroimunologia] Desenvolvimento do algoritmo Neuroimunologia* | alta |
| **Partida em 29/04** (SP e RJ, sem Hospital da Criança) | **29/04** | data acordada com o negócio; navegadoras em treinamento antes | `155996` — *Disponibilização Algoritmo Neuroimunologia* (comentário 24/04); `150812` — *[Neuroimunologia] Pipeline de envios diários automáticos para Sharepoint (Partida 29/4)*; `164417` — *Partir envios dia 29/04* | alta |
| Rollout de 6 unidades (BA, PR, MG, PB, AL, SE) | 01/07 | pedido do negócio | `184970` — *[Neuroimunologia] Rollout 6 unidades* | alta |
| Sem envios de 24/08 a 04/09 (~50 pacientes a recuperar; média 7/dia) | 24/08 → 07/09 | job não rodou | `298100` — *[Neuroimune] Não há envios desde o dia 24/08* | alta |
| Plataforma: PR em HML aprovado 28/09; produção 29/09 | 28–29/09 | — | `299110` — *[Migração][Neuroimuno] Migrar algoritmo para nova estrutura Fabrica IA + NLP Engine*; `303791` | alta |
| Legado pausado | **02/10** | envio duplicado | `360625` | alta |

## E09 — Nefrologia (clearance)

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Tabela de entrada sem data de execução; `dt_execucao` passa a ser gravada **a partir de 22/01** (histórico usa data do exame) | 19–22/01 | criar a monitoria | `139385` — *[Rim] Dashboard de Acompanhamento diário manual* | alta |
| Queda de entrada a partir de 28/01 (RJ sem envios): migração do ID de unidade não aplicada ao Rim; troca de `codIntegracao` (164 unidades) pelo novo campo (**525 unidades**); envios normalizados em 04–05/02 | 28/01 → 05/02 | migração MDM (E17) | `143672` — *[Pulmão] [Rim] Queda de exames na entrada dos algoritmos*; `143676` — *Investigação volume algoritmo Rim* | alta |
| Novo pipeline `ia-rim` criado; **`ia-clearance` desativado** | 19/02 | repositório próprio do Rim | `140691` — *Pipeline monitoria Rim* | alta |
| Monitoria reportava 8.624 envios relevantes em fev/26 contra 3.584 reais (RJ+SP, clearance 0–35) | aberto 30/03 | — | `154348` — *[Rim] Monitoria trazendo volume maior que o real* | alta |
| Causa 1: o update da saída sobrescrevia `dt_execucao` com a data da modificação — corrigido no PR 6019 (`cec789b0`) | 31/03 | — | `154348` (comentário 31/03) | alta |
| Causa 2: o negócio usa **idade ≤ 80**; filtro aplicado só à contagem de relevantes da monitoria | 07–08/04 | alinhar com o que o negócio usa | `154348` (comentários 07–08/04) | alta |
| Faixa de relevância: **clearance ≤ 35** para navegação; a visão até ≤ 60 já existia | 28/01–05/02 | — | `142968` — *[Monitoria] [Mama e Rim] Envios relevantes para navegação*; `156961` — *[Rim] Envios algoritmo Rim - contagem pacientes*; `403340` — *[Nefro - Clearance] Lista de Pacientes identificados Copa Star* (≤ 35) | alta |
| Fórmula Cockcroft-Gault × CKD-EPI **não confirmada** | aberta 28/07 | documentos divergem | `240943` — *Confirmar formula do clearance do algoritmo de Nefrologia (Cockcroft-Gault vs CKD-EPI)* (Novo) | — |
| V2 (base de entrada do time IA, lake 2.0, `fabrica-ia-plataforma`, branch `release/rim`): mais ~500 laudos/dia sem perda; produção depende de catálogo e de avisar o negócio | 05–14/05 | — | `168523` — *[Rim] Homologação v2*; `166485` — *[Estudo] Algoritmo Rim mudança base de entrada (engenharia x ia)* | alta (estudo); sem data de entrada em produção |

**Leitura:** o salto de ~6 para ~62 registros/dia entre 09/02 e 02/04 coincide com três mudanças do
board: ID de unidade (04–05/02), pipeline `ia-rim` no lugar de `ia-clearance` (19/02) e correção da
monitoria (31/03–08/04). O board **não** atribui o salto a nenhuma delas isoladamente — confiança média.
A regra registrada é **≤ 35**, não < 30.

## E10 — Mama (BI-RADS)

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Monitoria de mama passa a contar **BI-RADS 4 e 5** como relevantes | 28/01–05/02 | medir verdadeiros positivos | `142968` | alta |
| Base do funil de mama para BI com **BI-RADS 0 a 6** (exclui só −1), atualização diária | 25/02–13/03 | — | `146507` — *[Mama] Funil de Mama: Disponibilizar dados para BI* | alta |
| E-mail recebido pela navegação não é backup: "são laudos do algoritmo antigo, que tem volume menor porque são só os dados do PACS" | 05/03 | — | `146885` — *[Mama] Erros no envio dos e-mails para negocio* | alta |
| Contrato da linha Mule: "categoria de todo exame, um registro por paciente/dia" | 04/11 | — | `303791` (comentário 04/11) | média (confirma que o envio leva todas as categorias) |
| Processo do ADF cobre mama, fígado, tireoide e próstata; dados de servidor on-premises por túnel | 15/09 | — | `304135` — *[BI-RADS] Levantamento para Migração* | alta |
| Data de entrada em produção diverge (fev/2023 × jan/2024) | aberta 28/07 | — | `240945` — *Reconciliar data de entrada em producao do algoritmo de Mama (fev/2023 vs jan/2024)* (Novo) | — |

## E11 — Pulmão – nódulo

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Envio ao Salesforce **só SP**; navegação começa 12/01 | 12/01 | navegação só em SP | `139082` — *[Pulmão] Enviar apenas unidades SP para Salesforce* | alta |
| **Só `encaminhamento_nlp = S`**, idade 20–85, novos exames; "já está em PRD" | 22–23/02 | pedido do negócio | `145518` — *[Pulmão] Ajustes algoritmo Pulmão*; `145645` — *[Pulmão] Ajustes algoritmo Pulmão - V3* | alta |
| Pausa de Vivalle, Maternidade Star e Vila Nova Star | 27/02 → 04/03 | sem equipe de navegação | `146915` — *[Pulmão] Pausar envio de 3 unidades* | alta |
| Pausa do Hospital Antônio Afonso | 05–06/03 | sem navegação | `147779` — *[Pulmão] Pausar envio Hospital Antonio Afonso* | alta |
| **Só as 21 unidades SP com navegação de nódulo** | filtro em **19/03** | — | `149972` — *[Pulmão][SP] Enviar apenas 21 unidades com navegação de pacientes* | alta |
| Expansão para RJ (e DF/PR) planejada para 08/04 e **não executada** | 07/04 → 21/05 | falta o AS IS de quem navega e licença Salesforce | `154430` — *[Pulmão] Iniciar envios para unidades RJ* (Novo) | alta |
| Pipeline parado 2 dias (tensorflow); cluster volta do runtime 17 para 14 | 28/04 → 04/05 | — | `167033` — *[Pulmão] Pipeline não rodou ultimos 2 dias* | alta |
| Sem envio desde 09/10; cluster serverless trocado por `jobcluster-ia-pulmao` | 09/10 → 13/10 | — | `371813` — *[Captação] Todas as linhas indisponíveis*; `371836` — *Disponibilidade Nodulo Pulmonar* | alta |

**Leitura:** o job envia menos que os `S` porque o envio é recortado por região/unidade (só SP desde
12/01; só 21 unidades com navegação desde 19/03); o `S` conta todas as unidades.

## E12 — Fígado (hepatologia)

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Homologação no NLP Engine em paralelo ao legado | 03–10/06 | — | `173885` — *[NLP Engine][Figado] Homologar Algoritmo Figado no NLP Engine V0* | alta |
| Feature "levar o hepato para execução diária **em paralelo ao legado**", observar 3 dias 1:1 antes de desligar o legado; estado **Aguardando Produção desde 20/08** | 22/06 · 20/08 | validação antes de desligar o legado | `184596` — *[NLP Engine][Figado] Levar o hepato (fabrica-ia-plataforma) para o fluxo de execução diária em paralelo ao legado* | alta |
| Job da especialidade criado; 1ª carga full de hepatologia (10/08); jobs de hepatologia, TI-RADS e transplante de pulmão construídos | 06/08 → 19/08 | — | `259900` — *[Fabrica-ia] Criação do Job e Migração* | alta |
| Juiz LLM ligado por contorno em produção | 21/08 | — | `283644` — *[NLP Engine] Juiz LLM ligado por contorno nao documentado em hepatologia e transplante_pulmao (producao)* | alta |
| "Em produção, ciclo incompleto": faltam board médico, retorno e navegação; o legado tem três fluxos | 14/10 | entregas de plataforma | `303791` (descrição de 14/10) | alta (explica "sem entrega") |
| Por que a plataforma parou em 14/10 | — | **não registrado no board** | — | sem fonte |

## E13 — Ca de Estômago e Transplante de pulmão

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Estômago: job criado e rodando em HML com destinatários (planejado "iniciar 24.08") | 24/08 | — | `280907` — *[CA estômago] Deploy Algoritmo CA estômago (iniciar 24.08)* | alta |
| Estômago: sem envio em 26/08 — `environment` não setado na config do job, arquivos gerados em dev | 26/08 | — | `284823` — *[Ca de Estomago]Sem envios 26/08/26* | alta |
| Transplante: go-live adiado de 10/08 para 18/08 | 05/08 | licença médica na equipe do negócio | `194642` — *Disponibilização Algoritmo Transplante de Pulmão* | alta |
| Transplante: erro 403 ao rodar em PRD (26/08); envios concluídos (31/08) | 26/08 · 31/08 | — | `246669` — *[Transplante de Pulmão] Deploy Algoritmo Transplante de Pulmão (deve adiar para 18.08)* | alta |
| Primeira execução da plataforma processa a janela de 30 dias (`limit_rows` vazio) | 06/08 | padrão do job | `259900` (comentário 06/08) | média (explica o volume do 1º dia) |

## E14 — Ca de Rim

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Configs de rim não passaram pelo pipeline de deploy; corrigido em 08/09 | 08/09 | — | `285842` — *Pipeline de deploy* | alta |
| Atraso de 1 dia (DevOps fora do ar); indisponibilidade de rede priorizada | 02/09 · 09/09 | — | `194726` — *Disponibilização Algoritmo Ca de Rim* | alta |
| **Primeiro envio à navegação na manhã de 10/09** | **10/09** | — | `194726` (comentário 09/09) | alta |
| Escopo de regiões "unidades Brasil (Targa)" | 28/08 | acordado com o negócio | `285837` — *[CA Rim] Deploy Algoritmo CA Rim (iniciar 09.09)* | alta |
| **Revisão das unidades**: lista "Nacional Targa" como oficial, inclusão de unidades de PE e satélites — **enviado para PRD em 13/10 09:25** | 13/10 | listas herdadas do legado eram amplas e havia risco de perder unidades navegadas | `365142` — *[Captação] Revisão das unidades enviadas para time Giovanni Targa* | média (explica 99 → 117 hospitais) |

## E15 — Tumor ósseo

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Deploy com início planejado em 28/09; feature "Em Execução" em 28/09 e encerrada 05/10 | 28/09 | — | `301214` — *[TU Osseo] Deploy Algoritmo TU Osseo (iniciar 28.09)*; `259743` — *Disponibilização do Algoritmo TU Osseos* | alta |
| Escopo inicial: 4 unidades de SP | 17/08 | avaliar volume antes de ampliar | `280651` — *[TU Ósseo] Desenvolvimento do Algoritmo TU Ósseo* | alta |
| Sem rodar desde 08/10 | 08/10 → 14/10 | — | `373805` | alta |
| Expansão de 4 para 50 unidades (Nacional Targa), estudo projetou 5,9× | 28/10 · 05/11 | aumentar envios | `391631` — *[Estudo][TU Osseo] Impacto envios com expansão de unidades*; `393668` — *[TU Osseo] Expansão de unidades* | alta (fora da janela jan–out) |

## E16 — Envios

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Processo genérico de OneDrive (Logic App, base64, limite ~20 MB) "deve salvar no controle de carga" | 21/07 → 04/08 | — | `211128` — *[MLOps] Disponibilização OneDrive* | alta |
| **Task `api_<linha>` removida dos 12 jobs** (commit `990cdfd`, 01/10); relatório passa a depender só do OneDrive | 01–02/10 | "Removendo envio de API da central" | `321370` — *[NLP Platform] Aplicar randomização - Reumato e Tireoide - Desenvolvimento inicial e padronização* (comentário 02/10); `303791` | alta |
| **12–13/10: todas as linhas da plataforma sem envio** — secret `onedrive-logic-app-url` ausente no scope `fabrica-ia` (PRD e HML) + falha de import de notebook (PR 7591) | 12/10 → 14/10 | — | `371813`; `371855` — *Solução do defeito que causou indisponibilidade* | alta |
| **Logic App consumido pela aplicação trocado em 14/10**; retroativo enviado em 14/10 com nomenclatura sem o sufixo `_atual` | 14/10 | — | `371855` (14/10); `373805` | alta |
| Envio da Central de Captação perde pacientes | 26/10 | — | `391630` — *[Fabrica ia] Envios para central de captação* (Novo) | alta |
| Rastreabilidade Databricks → Logic App → Excel → OneDrive: divergências por coluna "Sexo", edição manual do arquivo pelo negócio e view de exportação com IDs de unidade diferentes da histórica | 27/10 → 11/11 | — | `391512` — *[Fábrica IA] Correção e rastreabilidade do envio Databricks → Logic App → Excel → OneDrive*; `383391` — *[Nlp Plataform] Histórico de filtros* | alta |
| Três reprocessamentos da task → e-mails repetidos em 12 linhas | 22/10 | retry da task | `384790` — *[Captação] 6 E-mails duplicados foram enviados em 12 linhas de cuidado* | alta |
| Sem carga do repositório clínico em 28–29/10 → sem envio em todas as linhas | 28–29/10 | carga da engenharia (INC2186345) | `394389` — *[Fabrica IA] Falha da carga do repositório clinico*; `395441` — *Investigação da causa raiz indisponibilidade dia 29/10* | alta |
| Envio API/OneDrive só para intervenção (randomização) implementado em HML | 23/10 | — | `353629` — *Envio API só para intervenção*; `353630` — *Envio OneDrive só para intervenção* | alta |
| Por que **não há log** em `tb_mov_onedrive_saida` entre 29/09 e 23/10 | — | **não registrado no board**; indício: troca do Logic App em 14/10 e a história de rastreabilidade aberta em 26/10 | `371855`, `391512` | baixa |

## E17 — Unidades

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| **Migração do ID das unidades em 26/01/2026**: o campo `codIntegracoes` da `tb_gold_mov_organization` é desativado; passa-se a usar o ID do MDM (`idbup`/`_ID`), via `replace(obs.identifier.assigner.reference, 'Organization/', '')` na `gold_corporativo_ia.corporativo.tb_gold_mov_exame` | anunciado 19/01; efetivo **26/01**; algoritmos atualizados 28–29/01; concluído 30/01 ("volumetria aumentou, surgiram novas unidades") | adequação ao padrão Rede D'Or e identificar mais unidades | `135635` — *26/01: Migração do ID das Unidades MDM na tabela Observation Exames (Engenharia)*; `141921` — *[26/01] Migração dos IDs nos Algoritmos*; `142850` — *Migração ID das unidades no MDM​* | alta |
| JSONs de unidade atualizados em reumatologia, DII, hepatologia e cólon; código em pulmão, reumatologia, DII e cólon | 29/01 | — | `143013` — *Atualizar JSON de cada projeto: Figado, DII, Colon e Reumato*; `143006` — *id unidade e id exame para: Pulmao, DII, Reumato e Colon* | alta |
| Rim migrado só em 03–05/02 (164 → 525 unidades) | 03–05/02 | — | `143676` | alta |
| Listas "antes da migração" (26/12/2025–26/01/2026) com IDs antigos nas faixas `1009xx`/`1010xx`/`2000xx`/`2001xx` | 26/01 | baseline | `142580` — *[Figado]Lista de envios pro negocio por dia, em Janeiro, por unidade - Antes da migração*; `142579`, `142581`, `142573` | alta |
| Regional PR: queda acentuada desde 17/01/2026 também na tabela da engenharia | 21/05 | origem | `172427` — *[Captação] Sem exames para Parana desde 17/01* | alta |

## E18 — Deploys da plataforma em produção

| fato | data | motivo declarado | fonte | confiança |
|---|---|---|---|---|
| Primeiros jobs (hepatologia, TI-RADS, transplante de pulmão) e esteira; TI-RADS em produção em 21/08 | 19–21/08 | — | `259900`; `282904` | alta |
| Estômago e transplante em PRD | 26/08 | — | `284823`; `246669` | alta |
| Ca de rim em PRD | 08–10/09 | — | `285842`; `194726` | alta |
| **Deploy de 28/09: cinco linhas** — DII, tumor ósseo, ateromatose, neuroimunologia, cólon (produção desde 29/09) | 28/09 | migração | `303791` (descrição de 14/10) | alta |
| **Deploy de 09/10: duas linhas** — doenças biliares (`0.3.2`) e próstata (`0.1.2`), 1ª execução 12/10; o mesmo deploy leva a remoção da `api_` (`990cdfd`) | 09/10 | — | `303791` | alta |
| **13/10**: revisão de unidades enviada para PRD às 09:25; pausa de Copa Star, Vila Nova Star e Maternidade Star com promoção solicitada | 13/10 | escopo Nacional Targa; unidades sem navegação | `365142`; `363228` — *[Captação] Pausar envios Copa Star, Vila Nova Star e Maternidade Star* | alta |
| Rotina semanal de migração HML → PRD (toda quarta), 1ª em 11/11 (PR 7940) | 11/11 | — | `405849` — *[Rotinas] Migração HML para PRD*; `406599` — *1º migração (1ª semana sprint)* | alta (fora da janela) |

---

## O que ficou sem fonte no board

- **E02** — o corte de escopo de 21/08 (`6c458da`) que deixou a TC de fora da plataforma: nenhum card registra a decisão.
- **E03** — data de desligamento do legado DII (28/09).
- **E04** — data de entrada da reumatologia em PRD na plataforma (11/09) e fim do legado (16/09).
- **E05** — carga retroativa jan–jun do TI-RADS legado; fim do legado (02/09); causa do volume ~3,5×.
- **E06** — causa do volume ~3× da próstata na plataforma.
- **E07** — sobrescrita das tabelas `_saida`, `_v2`, `_v3`.
- **E09** — fórmula do clearance (card `240943` aberto, sem resposta) e atribuição do salto de registros a uma causa única.
- **E12** — motivo de a plataforma de hepatologia parar em 14/10.
- **E16** — motivo da ausência de log de envio entre 29/09 e 23/10.

## Cards consultados

**Varredura:** `SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject]='IA' AND [System.Title] CONTAINS '<termo>'`
com os termos `ateromat, colon, cólon, DII, inflamat, reumato, tireoid, TI-RADS, tirads, próstata, prostata,
PI-RADS, pirads, biliar, neuro, nefro, clearance, Rim, mama, BI-RADS, birads, pulm, nódulo, nodulo, hepato,
fígado, figado, estômago, estomago, transplante, ósseo, osseo, OneDrive, exchange, Central, unidade, hospital,
migra, legado, envio, deploy, Randomiza, captação, indispon, produção, Tabia, código` → **598 work items**
(criados entre nov/2025 e nov/2026). Lidos em detalhe (descrição, critério, comentários, histórico de estado):

| evento | cards |
|---|---|
| E01 | 150839, 158639, 160356, 160364, 168549, 172767, 174106, 179028, 179647, 182295, 185291, 194937, 197189, 197217, 214390, 279307, 279331, 286925, 299109, 299310, 303427, 399835 |
| E02 | 133799, 134311, 137070, 137756, 139306, 142395, 142851, 142855, 142858, 143131, 143534, 145853, 146143, 147767, 147936, 153614, 163003, 165016, 180398, 195170, 260064, 283272, 283277, 283293, 303643, 360841 |
| E03 | 135271, 137496, 137497, 140795, 140800, 142256, 142542, 142672, 142854, 143002, 143856, 144568, 144586, 144769, 145899, 145950, 146643, 149802, 152462, 260952 |
| E04 | 134304, 137499, 139002, 140801, 140946, 142269, 146323, 149021, 149565, 211227, 219140, 285303, 299419, 299525, 409071, 415406 |
| E05 | 146155, 146157, 146159, 172035, 172770, 174111, 174683, 194556, 195008, 206868, 253321, 280589, 281186, 282904, 285294, 285305, 302198 |
| E06 | 134317, 146615, 158567, 160081, 160288, 172763, 173771, 173778, 358427 |
| E07 | 142859, 144303, 145119, 146409, 150735, 153471, 153939, 156009, 162958, 163539, 164296, 167875, 170022, 173747, 174104, 299113 |
| E08 | 142862, 144494, 149142, 150812, 153472, 155996, 160704, 162865, 164417, 167013, 167034, 182662, 184970, 298100 |
| E09 | 133997, 139385, 140691, 140917, 143672, 143676, 154348, 156961, 166485, 168523, 182074, 214346, 240943, 298275, 371833, 403340 |
| E10 | 139386, 140404, 140690, 142573, 142968, 146507, 146885, 147265, 240945, 260893, 261181, 304135, 371832, 372039, 385075, 395488 |
| E11 | 133044, 133800, 134305, 135029, 139082, 139307, 139639, 142998, 143000, 145518, 145645, 146915, 147779, 148942, 149972, 154430, 167033, 167119, 170533, 173940, 298141, 371836 |
| E12 | 135138, 150721, 150844, 169211, 169213, 169238, 169242, 172441, 173885, 179866, 183352, 184596, 184605, 197274, 371835 |
| E13 | 194332, 194371, 194642, 194676, 194681, 195004, 280907, 283773, 284823 |
| E14 | 194396, 194726, 284848, 285837, 285842, 398642 |
| E15 | 253362, 259739, 259743, 280651, 301214, 391631, 393668 |
| E16 | 146522, 170041, 211128, 280711, 287325, 353629, 353630, 360482, 363228, 371855, 373187, 373805, 384790, 391512, 391599, 391630, 395441 |
| E17 | 135635, 141921, 142576, 142579, 142580, 142581, 142850, 143006, 143013, 169082, 169091, 172427, 365142 |
| E18 | 177599, 179298, 184655, 197268, 256372, 259900, 286981, 286987, 299109, 299113, 358427, 405849, 406599 |
| transversais | 246669, 298553, 299110, 299111, 303629, 303791, 321370, 360225, 360625, 371813, 383391, 393825, 394389 |
