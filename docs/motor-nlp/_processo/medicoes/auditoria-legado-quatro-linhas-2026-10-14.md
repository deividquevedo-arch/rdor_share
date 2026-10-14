---
tags: [auditoria, migracao, legado]
---

# Auditoria do legado — Mama, Nódulo pulmonar, Nefro e Hepato

> **Etapa de Research**, 14/10/2026. Ponta a ponta: o que cada algoritmo lê, como decide, o que
> grava, para onde envia, o que volta — e, para cada peça, se a plataforma já suporta, se cabe em
> config ou se precisa ser construído.
>
> **Escopo:** as quatro linhas de captação que seguem no legado — **mama (BI-RADS)**, **nódulo
> pulmonar**, **nefro** e o **ciclo da hepato**.
>
> ✅ **Verificado no workspace antigo (14/10):** as quatro **rodam todo dia**, a partir de Repos na
> branch `hml`, disparadas por um orquestrador sem acesso para o usuário que auditou. Ver §0.1.
> ⚠️ **Segue não verificado:** o conteúdo das configurações em `/mnt/` e quem lê a view da nefro.

Plano relacionado: card `303791` — *Plano de Migração algoritmos final*.

---

## 0. Fontes, e por que importam

| linha | fonte lida | ponta |
|---|---|---|
| **Mama (BI-RADS)** | `IAAzureDatabricksBiRads`, `main` | `3391569`, 28/05/2026 — *Deploy PRD* |
| **Nódulo pulmonar** | `IAAzureDatabricksPulmao`, `hml` | `c0ee4d3`, 03/09/2026 |
| **Nefro** | `fabrica-ia-plataforma`, `apps/databricks/rim/model/ntb_ia_rim_motor.ipynb`, em `hml` e `main` | PR 6629 (18/06), conteúdo idêntico ao PR 6856 (20/07) |
| **Nefro** (versão anterior) | `IAAzureDatabricksRim`, `main` | `0381b16`, 28/05/2026 |
| **Hepato** | `IAAzureDatabricksHepatologia`, `hml` | `da4c31b`, 22/06/2026 |

🔴 **Não existe uma fonte única por linha.** Na nefro há uma segunda implementação no
`fabrica-ia-plataforma` (`ntb_ia_rim_motor`), com outra entrada, outro catálogo e outra regra de
relevância — **e não é ela que roda** (§0.1). No BI-RADS, a cópia do `fabrica-ia-plataforma` não
descriptografa PII; o repositório canônico, que é o que roda, **descriptografa**.

### 0.1 O que roda em produção — verificado no workspace antigo em 14/10

| linha | job | Repo no workspace | branch e commit | execuções desde 01/09 | última |
|---|---|---|---|---|---|
| **Mama** | `ia-birads-v2` | `IAAzureDatabricksBiRads` | `hml` · `75a5898` (19/02) | 44 de 44 dias | 14/10 |
| **Nódulo** | `ia-pulmao` | `IAAzureDatabricksPulmao` | `hml` · `c0ee4d3` (03/09) | 44 de 44 dias | 14/10 |
| **Nefro** | `ia-rim` | `IAAzureDatabricksRim` | `hml` · `df21725` (24/04) | 44 de 44 dias | 14/10 |
| **Hepato** — motor e board | `ia-hepatologia-envio-clone` | `IAAzureDatabricksHepatologia` | `hml` · `a6a33ec` (08/06) | 44 de 44 dias | 14/10 |
| **Hepato** — retorno e navegação | `ia-hepatologia-retorno-navegacao-clone` | idem | idem | dias úteis, 07:00 | 14/10 |

- **Os quatro primeiros são disparados por outro job** (gatilho `RUN_JOB_TASK`, mesmo horário nos
  quatro — 08:13 em 14/10, 08:37 em 13/10). `ia-rim` e `ia-pulmao` aparecem **pausados** na própria
  agenda; quem roda é o orquestrador. 🔴 **O orquestrador não é visível** para o usuário que auditou
  (a execução-pai devolve erro de permissão). É a orquestração que os `workflow/*.json` vazios
  deveriam versionar.
- ✅ **Código em produção = código auditado** na mama e na nefro: `git diff` vazio entre o commit do
  Repo e a fonte lida.
- 🔴 **Hepato roda atrás da ponta da `hml`:** falta `da4c31b` (22/06, PR 6646, *"tirando a
  duplicidade"*), que deduplica por `id_exame` na extração para o board médico. **Sem a correção, o
  board pode receber o mesmo exame mais de uma vez** — não medido.
- 🔴 **Nódulo falhou em 12/10 e 13/10** no disparo agendado; voltou com uma execução manual em 13/10
  10:27 e com o agendado de 14/10.
- 🔴 **Nefro: a versão em Unity Catalog está morta.** `diamond_nefrologia.nefrologia.tb_diamond_mod_rim_saida`
  tem última execução em **13/05/2026**; a que roda grava em
  `hive_metastore.ia.tb_diamond_nefrologia_clearance_saida`, atualizada em 14/10.

### 0.2 Volume, na fonte

`hive_metastore.ia.tb_diamond_mod_monitoramento`, totais de 01/09 a 14/10/2026:

| `nm_projeto` | entrada | relevantes | última |
|---|---|---|---|
| `Rim` (nefro) | **1.270.366** | 4.512 | 14/10 |
| `Figado` (hepato) | 193.230 | 1.006 | 14/10 |
| `Pulmao` (nódulo) | 52.870 | não registrado | 14/10 |
| `Mama` | 43.222 | 1.647 | 14/10 |
| `Reumatologia` *(controle)* | 4.735 | — | **02/09** |
| `Ateromatose` *(controle)* | 1.096 | — | **02/09** |

🟢 **Controle positivo:** as duas linhas desligadas no legado param em 02/09 **na fonte** — a tabela
mostra quando uma linha para, e as quatro não pararam.
🔴 **O espelho** `diamond_fabrica_ia.legado.tb_diamond_mod_monitoramento` **para em 05/09 nas
quatro**, enquanto a fonte segue diária: é o espelho que deixou de ser alimentado para elas.
⚠️ "Relevante" é a definição **do monitoramento** — na nefro, diferente da do motor (§3). E a
entrada de mama (43.222) bate com as linhas gravadas na saída no mesmo período (43.222).

---

## 1. Quadro comparativo

| | Mama | Nódulo pulmonar | Nefro | Hepato (ciclo) |
|---|---|---|---|---|
| **é NLP?** | sim — categoria ordinal | sim — extração estruturada | 🔴 **não** — cálculo sobre valor de laboratório | motor já na plataforma |
| **cabe em config do motor hoje?** | ✅ sim | 🔴 não | 🔴 **não se aplica** | ✅ régua sim |
| **entrada** | gold de exames + PACS | gold de exames | gold de exames, **exame de creatinina** | — |
| **saída** | categoria de **todo** exame (−1 a 6) | lista de nódulos com atributos | valor de clearance | relevância |
| **destino** | API Mule — Portal de Acolhimento | Salesforce | **não está no código** | e-mail, três etapas |
| **retorno** | `fl_integracao_concluida` | — | — | planilha do board médico |
| **config fora do repo** | 1 JSON em `/mnt` | **14 CSV** em `/mnt` | nenhuma | JSONs em `/mnt` |
| **plataforma suporta o envio?** | 🔴 não | 🔴 não | 🔴 indefinido | 🟡 só a 1ª etapa |

🔴 **Nenhuma das quatro envia para a Central de Captação.** A plataforma tem o envio à Central
(`data_exchange/runs/ntb_ia_api.py`), mas nenhuma das quatro a usa hoje. Migrar para o padrão da
plataforma **muda o destino** — decisão de negócio por linha, não porte técnico.

---

## 2. Mama (BI-RADS)

### Entrada
- `gold_corporativo_ia.corporativo.tb_gold_mov_exame` e `tb_gold_mov_paciente`.
- Filtro `mam[o|a|á]` em **quatro** colunas (`dsc_procedimento_integracao`, `nme_codigo`,
  `dsc_codigo`, `nme_exame`), sem biópsia, `tp_procedimento IN ('IMG','IMA')`, unidades `RICHET%`
  excluídas.
- 🔴 **Janela móvel de 30 dias até D-1, reprocessada todo dia.** A plataforma processa por dia e
  deduplica por `id_exame` — essa semântica não existe lá.
- **Segunda fonte: PACS**, via `ia.vw_enave_df_historico_birads` (view do `hive_metastore` do
  workspace antigo), unida à entrada da gold.

### Régua
Mesma família do PI-RADS legado: normaliza o texto, apaga a legenda do ACR por `replace` de frases
fixas, extrai a categoria e agrega pelo máximo. Usa `nltk.download('all')`.

✅ **Na plataforma é config pura** de `ordinal_extraction` com `systems.bi_rads`, como foi a
próstata — a lib não tem código por sistema RADS, e a `aggregation_legend_filter` substitui o
`replace` de legenda. Já existe homologação de bancada em
`rads/notas/Relatorio-homologacao-birads-bancada-v1.md`.

### Saída e contrato
Catálogo próprio `diamond_birads.birads`: `tb_diamond_mod_birads_entrada`, `_saida`,
`_complemento` e a view `vw_diamond_mod_birads_complemento`.

🔴 **O contrato é outro:** a view leva `vl_proced_birads` de **todo** exame, de −1 a 6, e não só os
relevantes. A validação embutida é o contrato da API do outro lado: CNPJ com 14 dígitos, regional
com 2 letras, nome de unidade até 250 caracteres, categoria em `{-1..6}`.

### Envio
`serving/ntb_ia_envio_api.py`:

1. **GET** em `onco-navegadoras-exp-api(-prod).br-s1.cloudhub.io/api/navegadoras/inicial`, com
   `dataInicial`, `dataFinal` e `flagIntegracao=false`, autenticação básica. O GET **dispara** a
   integração Mule, que **vai buscar** os dados na view — não é envio de payload.
2. Espera até **4 horas**, tentando a cada minuto, até a Mule registrar o log em `logs.api_birads`.
3. Notificação de sucesso ou erro em card do Teams, por Logic App.

### Retorno
`fl_integracao_concluida` na saída — a confirmação de que a integração consumiu o registro.

### O que a plataforma não suporta
- envio por **disparo + espera de confirmação** (a plataforma empurra arquivo ou faz POST);
- **contrato de todas as categorias** (a plataforma entrega `fl_relevante = 1`);
- **segunda fonte de entrada** (PACS);
- **janela móvel com reprocessamento**.

---

## 3. Nefro

🔴 **Não é algoritmo de NLP.** Não lê laudo de imagem e não decide por texto: lê o **exame de
laboratório de creatinina** e calcula a **taxa de filtração glomerular** com idade e sexo do
cadastro. A plataforma de NLP não tem onde encaixá-lo — **não é migração de config, é decisão de
arquitetura**.

⚠️ **Não confundir com o `cancer_rim`**, que está em produção na plataforma e trata lesão renal.

⚠️ **A versão que roda é a do `IAAzureDatabricksRim`** (descrita em *Versão em produção*, abaixo).
A do `fabrica-ia-plataforma` está detalhada primeiro porque é a mais recente e a candidata natural
a base de migração — mas **parou em 13/05**.

### Entrada — versão do `fabrica-ia-plataforma` (`ntb_ia_rim_motor`, parada)
- `gold_corporativo_ia.corporativo.tb_gold_mov_exame`, `nme_codigo = '40301630'` (TUSS de
  creatinina), itens de `proced_lista_exames` cujo nome contém `creatinina`.
- **O valor sai do texto do resultado** (`laudo_original` do item): tira `Inferior a `, troca vírgula
  por ponto, **apaga tudo que não é dígito nem ponto**, converte para número; falha vira **0**.
- Marca `fl_urina` quando o nome do item contém `24`, `urina` ou `urinaria`.
- Um registro por exame e dia: fica o de **menor** creatinina.
- Junta `tb_gold_mov_paciente` para **sexo, nascimento e telefone**; idade na data do exame.

### Cálculo
`calc_cleareance_without_weigth` — documentado no código como **CKD-EPI**:
`142 × (Scr/κ)^−1,2 × 0,9938^idade` (× 1,012 para sexo feminino), κ = 0,7 (F) ou 0,9 (M).

| ponto | efeito |
|---|---|
| a fórmula CKD-EPI 2021 tem o termo `min(Scr/κ, 1)^α`; o código aplica `−1,2` em toda a faixa | com creatinina **abaixo** de κ o valor sai superestimado — mas nessa faixa o resultado fica acima de 60 de qualquer forma até ~138 anos (cálculo analítico: 142 × 0,9938^idade ≥ 60), então **o corte de 60 não é afetado** |
| sexo ausente ou não reconhecido | calcula como **masculino** |
| **idade abaixo de 18** | aplica a fórmula **adulta**; a versão anterior tinha Schwartz pediátrica, que deixou de ser chamada |
| falha de conversão do valor | creatinina **0** → divisão por zero → `−2` → descartado |
| apagar tudo que não é dígito | resultado com faixa de referência no mesmo texto vira número inválido → **0** → descartado; e o critério "menor valor" escolhe justamente o **0** quando o exame tem itens válidos e inválidos |

⚠️ As duas últimas linhas são **perda silenciosa possível, não medida**: depende de quantos
resultados trazem mais de um número no texto.

### Saída e relevância
`diamond_nefrologia.nefrologia.tb_diamond_mod_rim_entrada` e `_saida`, view `vw_gold_creatinina`.

- `fl_relevante = 1` quando o clearance está **entre 0,01 e 60**.
- A view exclui exame de urina, convênio **AMIL** (regex) e restringe a **D-31 a D-1**.
- 🔴 **`cast(vl_clearance as numeric)` na view** é `DECIMAL(10,0)`: o valor entregue é **inteiro
  arredondado**.
- 🔴 **Três definições de "relevante":** a do motor (≤ 60), a do **monitoramento** (≤ 35 **e** idade
  ≤ 80, com 44 unidades excluídas por ID literal) e a de quem envia — que **não está no código**.
  O número da tabela do §0 é a do monitoramento.
- A view devolve **telefone, nome e CRM do solicitante descriptografados**.

### Envio
🔴 **Nenhum código de envio** em nenhuma das duas fontes — `serving/` e `workflow/` só têm README.
A view é **lida de fora**. O único consumidor encontrado no código é
`lakehouse/preparacao_dados/diamond/ntb_ia_diamond_modelo_captacao.py`, que cruza a saída de todas
as linhas com o consumo dos pacientes (**medição de valor**, não envio). **Quem envia a nefro, para
onde e com que filtro é a primeira pergunta.**

### Versão em produção (`IAAzureDatabricksRim`, job `ia-rim`)
Lê a observação FHIR (`gold_corporativo.observation.tb_gold_mov_observation_exames`), TUSS
40301630, janela de **5 dias**, grava em `ia.tb_diamond_nefrologia_clearance_saida` e
`ia.vw_gold_creatinina` no `hive_metastore` — 2.828.544 linhas, atualizada em 14/10.
Mesma fórmula, os mesmos pontos da tabela acima, e a view aplica clearance 0,1–60 e a exclusão de
AMIL. O `MERGE` **não atualiza** `valorClearance` em exame já gravado (trecho comentado).
O job tem só `modelo` e `monitoramento` — **nenhuma tarefa de envio**.

### O que a plataforma não suporta
- **linha sem NLP** — o runner é construído em volta do `nlp_engine`;
- **cálculo derivado com dado de cadastro** (idade, sexo) — a lib tem `threshold_by`, mas lê idade
  **do texto do laudo**, não do cadastro;
- **entrada de laboratório** — o `gold_filter` seleciona por descrição de exame de imagem.

---

## 4. Nódulo pulmonar

### Entrada
- Gold `tb_gold_mov_exame` e `tb_gold_mov_paciente`, filtro TC/RM de tórax em três colunas.
- **Janela de 30 dias pela data de liberação do laudo**, ancorada em `current_date()` — não na data
  de execução, o que impede reprocessar um dia passado.
- Calcula **idade** do paciente a partir do cadastro.

### Régua
**Não é classificação binária — é extração estruturada.** spaCy `pt_core_news_lg` com
`EntityRuler`, padrões montados a partir de **14 CSV em
`/mnt/trusted/datalake/ia/projetos/pulmao/config/<amb>/`**:

| dicionário | o que carrega |
|---|---|
| tipo e risco | classificação e localização |
| descritores estruturais (primários e secundários), fora do pulmão, procedimento | o que é e o que não é nódulo |
| qualitativos: indeterminado, maligno-ou-suspeito, benigno, de contraste | caracterização |
| **comparativos** | **surgimento, progressão, estabilidade, redução, resolução** — contra o exame anterior |
| frase padrão | boilerplate |
| de-para de unidades Salesforce | destino |

Para cada nódulo: **tamanho em mm, localização, caracterizações, risco**; por laudo, **conduta** do
radiologista (seguimento, controle, correlação clínica) e uma probabilidade.

### Saída e destino
`tb_diamond_mod_pulmao`, **`tb_diamond_mod_pulmao_saida_salesforce`** e
`tb_diamond_mod_pulmao_saida_descripto`. **Destino: Salesforce**, com de-para próprio de unidades.

### O que a plataforma não suporta
- **extração multiatributo por lesão** — a lib extrai categoria (`ordinal_extraction`), medida
  (`quantitative`) e já vincula medida à lesão (`lesion_linking`, série 0.15), mas não carrega
  localização, caracterização e risco por nódulo;
- **evolução comparativa** — zero ocorrência na lib;
- **conduta do radiologista** como saída;
- **idade estruturada na entrada do motor**;
- **saída em lista de lesões**, não em `fl_relevante` + `findings`;
- **Salesforce** como destino.

⚠️ Uma função grava CSV num **caminho de sandbox pessoal** em `/mnt/temp/sandbox/...` — depuração
deixada no código.

---

## 5. Hepato — o ciclo do legado

O motor já está na plataforma. O que o legado faz **depois** dele, e a plataforma não:

1. **Extração para o board médico** — Excel por unidade com lista suspensa de **complexidade**: Alta,
   Baixa, Transplante, Oncologia, Não é doença de fígado, Não navegável. Leva nome e CRM do
   solicitante descriptografados (`security.prd.rdsl_decrypt`).
2. **Retorno** — o Logic App busca no OneDrive as planilhas preenchidas pelos médicos, que viram
   `tb_diamond_mod_hepatologia_retorno_v2` e um histórico, com pastas de sucesso e erro.
3. **Navegação por fluxo** — a partir do retorno, **duas listas**: `transplante` e
   `onco_e_baixaComplexidade`, cada uma com seu JSON de destinatários.

### O que a plataforma não suporta
- **pipeline com humano no meio** — a plataforma é motor → entrega, numa passada;
- **captura de retorno** (na plataforma, "retorno" só aparece em comentário);
- **roteamento por fluxo** a partir de uma decisão humana.

✅ A primeira etapa, isolada, cabe no `data_exchange` (Excel + lista suspensa + Logic App).

🔴 **A URL do Logic App de retorno está no código com a assinatura SAS**
(`serving/ntb_ia_hepatologia_retorno.py`). A assinatura dá direito de disparar o fluxo. Não
reproduzida neste documento.

---

## 6. Lacunas da plataforma, consolidadas

| capacidade | Mama | Nódulo | Nefro | Hepato | natureza |
|---|---|---|---|---|---|
| envio fora do OneDrive/Central (Mule, Salesforce) | ● | ● | ? | | exchange |
| captura de retorno de planilha | ○ ack | | | ● | exchange + tabela |
| pipeline multi-etapa com decisão humana | | | | ● | orquestração |
| linha **sem NLP** (regra/cálculo sobre dado estruturado) | | | ● | | runner |
| janela móvel com reprocessamento | ● | ● | ● | | runner |
| contrato com todas as categorias, não só relevantes | ● | | | | contrato de saída |
| segunda fonte de entrada (PACS) | ● | | | | runner |
| dado de cadastro (idade, sexo) na decisão | | ● | ● | | contrato de entrada |
| extração multiatributo por lesão + evolução | | ● | | | **lib** |
| configuração em `/mnt` → Volume/UC | ● | ● | | ● | migração de artefato |

**Leitura:** só uma lacuna é da **biblioteca** — a do nódulo. As outras são de **plataforma**
(runner, exchange, orquestração), alçada de outro time pelo POP-IA-08.

---

## 7. Achados de segurança e governança

| achado | onde |
|---|---|
| 🔴 URL de Logic App **com assinatura SAS** no código | Hepato, `serving/ntb_ia_hepatologia_retorno.py` |
| 🔴 **duas implementações** da mesma linha em repositórios diferentes, uma parada desde 13/05 com catálogo próprio | Nefro |
| 🔴 **orquestrador sem acesso** e definições de job fora do versionamento | as quatro |
| 🔴 produção roda **branch `hml`**, e a hepato atrás da própria `hml` | as quatro |
| 🟡 identificador de cliente da API Mule no código | Mama, `serving/ntb_ia_envio_api.py` |
| 🟡 **definições de job vazias** — `workflow/*.json` com 0 bytes | Mama e Hepato |
| 🟡 caminho de sandbox pessoal em código | Nódulo, `model/ntb_ia_predicao.ipynb` |
| 🟡 `nltk.download('all')` | Mama — proibido pelas regras do motor |
| 🟡 regra de negócio por **ID literal de unidade** (44 IDs) | Nefro, monitoramento |
| ⚠️ CPF, nome e telefone descriptografados na view lida pela integração | Mama |
| ⚠️ armazenamento `sardslusdevelopmenthml` usado também em prd | Mama, Hepato, Nefro |

🔴 **A orquestração do legado não está versionada.** Agenda, ordem das tarefas e branch só existem no
workspace antigo.

---

## 8. Ordem sugerida, pelo que cada uma exige

| | régua | entrega | onde está o esforço |
|---|---|---|---|
| **Mama** | config pura, com bancada | adaptador Mule + contrato de todas as categorias | **plataforma** |
| **Hepato (ciclo)** | — | três etapas com humano no meio | **plataforma** |
| **Nefro** | cálculo, não NLP | destino desconhecido | **decisão de arquitetura** antes de esforço |
| **Nódulo pulmonar** | extração estruturada | Salesforce | **lib + plataforma**, o maior |

**Mama é a de menor risco na régua** e a mais concreta na entrega. **Nefro** precisa primeiro de
uma resposta: a plataforma de NLP é o lugar dela? **Nódulo pulmonar** não é migração — é projeto.

---

## 9. O que falta verificar

Fechados em 14/10, no workspace antigo:

1. ✅ **As quatro rodam todo dia** — §0.1.
2. ✅ **Versão em execução** — branch `hml` nas quatro; mama e nefro iguais ao auditado; hepato
   **sem** a correção de duplicidade.

Abertos:

3. **O orquestrador** que dispara as quatro — sem acesso. Dono, agenda e definição.
4. **O conteúdo das configs em `/mnt/`** — os 14 CSV do nódulo, os JSON de navegação da hepato,
   `configCorrecaoDuplicidade.json` do BI-RADS.
5. **Quem lê** `vw_gold_creatinina` para enviar, e a tabela Salesforce do nódulo — o histórico de
   consultas (`system.query`) também é sem acesso.
6. **Por que o nódulo falhou** em 12 e 13/10.

Fora do workspace:

5. **Decisão de destino** por linha: manter Mule e Salesforce, ou convergir para a Central.
6. **Decisão de arquitetura** para a nefro: linha sem NLP dentro da plataforma, ou fora dela.
7. **Validação clínica** da fórmula da nefro — termo `α` ausente e pacientes pediátricos.

---

## Anexo — Endometriose (levantada, fora do escopo)

Levantada antes da definição do escopo; registrada para não se perder.
`IAAzureDatabricksEndometriose`, `hml`, `d482d7e` (30/07). **Não consta** do monitoramento
espelhado.

- **Entrada:** tabelas do workspace antigo (`ia.tbl_gold_exame`, `tbl_gold_paciente`,
  `tbl_silver_encounter`, `tbl_silver_patient_telecom`); descrição com abdome, pelve, bexiga, útero,
  vias urinárias; **só paciente com convênio e telefone**; incremental sobre todo o histórico.
- **Régua:** achados, sintomas, ~60 frases de negação (janela de 70 caracteres para os dois lados).
  🔴 **Sintoma sozinho entrega** — colide com o princípio de que relevância vem de achado.
- **Envio:** FTP para o **Salesforce Marketing Cloud**, Excel por unidade por Logic App/OneDrive, e
  **captura de retorno** das planilhas. ⚠️ Telefone enviado a ferramenta de marketing.
