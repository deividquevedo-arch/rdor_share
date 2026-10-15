---
tags: [randomizacao, relatorio, impacto-financeiro]
---

# Relatório dinâmico e medição de impacto — randomização da Captação

> v0 — 15/10/2026. Consultas em `relatorio-sql/`, todas executadas contra produção em 15/10.
> Ligação: [[proposta-conciliacao-303629-v0]].

## 1. Verificação em produção (15/10)

| pergunta | resposta | evidência |
|---|---|---|
| Os pacientes estão separados em grupos? | **sim** | base com 1 bi de CPFs (5,0001% controle); log com 227 registros em prd, 11 controle |
| Isso está nas tabelas de PRD? | **sim** | `mlops_fabrica_ia.operacao.tb_mov_randomizacao_{controle,log,intervencao,relatorio}`; views `vw_mod_diamond_{reumatologia,tirads}_export_v0_intervencao` em `diamond_fabrica_ia` |
| O envio considera a randomização? | **sim, no OneDrive** | o envio lê só a view `_intervencao`; em 15/10 a view original tinha 15 e 41 linhas (1 e 3 de controle) e a `_intervencao` 14 e 38, **zero controle**; nenhuma etapa com falha de 12 a 15/10 |
| E pela API da Central? | **produção nunca enviou** | log da Central só tem hml (142, o último em 14/10), todos a partir da `_intervencao`. Contexto: task `api_` fora dos jobs desde 09/10 |
| A atribuição é estável? | **sim** | zero CPF com dois grupos; 618 de 618 iguais à base |

## 2. Fase 1 — primeira linha de atenção (randomização)

Foco: **a randomização está funcionando todo dia, em toda linha ativa, sem vazar controle.**
Sem cruzamento com consumo nesta fase.

| # | checagem | regra | tipo | consulta |
|---|---|---|---|---|
| 1 | **execução** | toda linha ativa tem registro de relatório em cada dia útil (agenda seg–sex, 04:00) | alerta | `06` |
| 2 | **falha** | `st_execucao = SUCESSO` e nenhuma etapa em `ds_etapas_falha` | alerta | `06` |
| 3 | **conservação** | elegíveis ao envio = controle + navegação + sem CPF | alerta | `06` |
| 4 | **log × relatório** | registros no log do dia = controle + navegação do relatório | alerta | `06` |
| 5 | **controle enviado** | nenhum registro de controle com `fl_enviado` | alerta | `06`, `03` |
| 6 | **grupo único** | nenhum CPF com dois grupos, em qualquer linha ou data | alerta | `03` |
| 7 | **aderência à base** | grupo do log igual ao da base pré-gerada | alerta | `03` |
| 8 | **proporção acumulada** | fração de controle por paciente dentro de 3 erros-padrão da fração da base | alerta | `07` |
| 9 | **sem CPF** | contagem de pacientes sem chave (vão à navegação, fora da análise) | informativo | `06` |
| 10 | **contaminação** | randomizados relevantes em outra linha | informativo | `04` |
| 11 | **volume** | funil por linha: processados → relevantes → elegíveis → grupos | informativo | `01`, `02` |

**Por que acumulada na proporção:** o dia isolado tem de 2 a 41 elegíveis por linha; 0 ou 3 controles
num dia é variação normal. O acumulado em 15/10: total 9 de 200 pacientes (4,5%, z = −0,32),
reumatologia 3 de 47 (z = 0,44), TI-RADS 6 de 153 (z = −0,61) — todos dentro.

**Resultado em produção, 12–15/10:** 8 de 8 dias-linha **OK**; integridade 0 / 0 / 0 sobre 227
registros.
**Controle positivo dos alertas:** a mesma consulta em hml acusa 5 dias-linha em ALERTA — falha
em 08/10 nas duas linhas, dias sem registro de relatório e um dia com 26 registros de log sem
relatório correspondente (02/10). A checagem consegue acusar.

**Rotina:** consultar após o fim da janela dos jobs (~08:00), em dia útil. `PENDENTE` = hoje, job
ainda sem registro.

**Painel (rascunho, não publicado, 15/10):** *Randomização — primeira linha de atenção*, workspace
`adb-7405607882166874`, pasta `/Users/<usuário>/randomizacao/` (pasta pessoal do autor), warehouse
`Serverless Starter Warehouse`. Definição versionada em `painel/painel-randomizacao.lvdash.json`,
gerada por `painel/gerar_painel.py`. Complementa a página *Randomização* do painel
*Fábrica de IA — Execuções & Qualidade [HML]*, que cobre volume e fração diária, mas não as
checagens de integridade.
No painel, a contaminação conta relevância em outra linha **a partir da data de randomização de
cada paciente** (23 pacientes de navegação em 15/10), mais estrita que a consulta `04` com data
única (26).

### Cruzamento com o painel *Fábrica de IA — Execuções & Qualidade [HML]*, página *Randomização* (15/10)

Mesmas tabelas de origem (`tb_mov_randomizacao_relatorio` e `_log`), mesmas contagens de base em
produção. As diferenças são de **definição e de filtro**:

| número no painel | fórmula | prd | o que observar |
|---|---|---|---|
| Total Controle / Total Intervenção | `SUM(qt_controle)` / `SUM(qt_intervencao)` | 11 / 216 | ✅ batem — são **exames**; em pacientes, 9 / 191 |
| Total Exportados | `SUM(qt_exportados)` | **2.505** | soma as **13 linhas**; controle + intervenção vêm só das 2 randomizadas (227). Os três números não fecham entre si |
| Fração Controle Média | `AVG(vl_fracao_controle)` | **3,91%** | média de frações diárias — dia com 2 exames pesa igual a dia com 95. A fração agregada é 4,85% (exames) ou 4,5% (pacientes); o próprio dataset tem a medida `pct_controle` (4,85%), não usada no widget |
| Total Pacientes (gráficos do log) | `COUNT(1)` | 227 | conta **registros** (exames), não pessoas; `cpfs_distintos` = 200 |
| filtro Ambiente | sem valor padrão | — | ao abrir, **soma hml + prd**: 141 dos 200 pacientes de prd também estão em hml (mesmos exames nos dois ambientes) — contagem em dobro |
| execuções repetidas | `SUM` por linha | — | hml tem 4 dias-linha com mais de uma execução; somadas, inflam os totais. O painel novo usa a última execução do dia (`max_by(..., dh_registro)`) |

Em hml, sem filtro: 24 controle / 568 intervenção / 10.255 exportados — números que não
correspondem a produção.

## 3. O relatório — painéis

Proposta: **painel AI/BI (Lakeview) no Databricks**, sobre as tabelas de produção, com filtros de
período e de linha. Uma consulta por painel.

| painel | o que mostra | consulta |
|---|---|---|
| **Funil por linha** | exames que entraram → processados → relevantes → elegíveis ao envio → controle / navegação, por dia e linha; as 13 linhas, randomizadas ou não | `01_funil_por_linha_e_dia.sql` |
| **Grupos** | exames e pacientes por grupo e linha; enviados; primeiro e último dia | `02_grupos_por_linha.sql` |
| **Integridade** | CPF com dois grupos, controle enviado, divergência da base — devem ser zero | `03_integridade.sql` |
| **Contaminação** | randomizados que também foram relevantes em outra linha | `04_contaminacao.sql` |
| **Atenção diária** | status por linha e dia útil, com as checagens 1–5 | `06_atencao_diaria.sql` |
| **Proporção acumulada** | fração de controle por linha e total contra a base, com z | `07_proporcao_acumulada.sql` |
| *Desfecho — fase 2* | conversão e valor médio por grupo em 30/90/180 dias, com o período anterior | `05_desfecho_consumo.sql` |

**Primeiro retrato (12–15/10):**

| linha | grupo | exames | pacientes | enviados | relevantes em outra linha |
|---|---|---|---|---|---|
| Reumatologia | controle | 4 | 3 | 0 | 0 |
| Reumatologia | navegação | 58 | 44 | 58 | 2 |
| TI-RADS | controle | 7 | 6 | 0 | 0 |
| TI-RADS | navegação | 158 | 147 | 158 | 24 |

⚠️ **"Relevante" e "randomizado" não são o mesmo número.** A randomização se aplica aos exames
**elegíveis ao envio** (depois da view: deduplicação, validação, unidade). Em 12/10 o TI-RADS teve
207 relevantes e 95 elegíveis. O relatório mostra os dois.

## 4. Fase 2 — o que observar para medir impacto financeiro

> Fora do escopo da primeira linha de atenção. Registrado para quando a medição começar.

| ponto | por quê | como |
|---|---|---|
| **Intenção de tratar** | compara-se o grupo **atribuído**, não quem foi efetivamente contatado | unidade de análise = paciente randomizado; contato da navegadora é variável secundária |
| **Fonte e chave do desfecho** | `gold_corporativo_ia.corporativo.vw_mov_fato_consumo`, chave **`num_cpf_paciente_d`** | 171 de 200 CPFs randomizados casam (85,5%) nos 12 meses anteriores; pela coluna cifrada, zero |
| **Atraso da fonte** | consumo vai até **29/09** em 15/10 (~2 semanas) | só entra na taxa quem tem a janela inteira observada (`fl_janela_completa`) |
| **Janela** | conversão leva tempo e varia por linha | pré-registrar 30, 90 e 180 dias; reportar as três |
| **Linha de base** | grupos pequenos ficam desbalanceados por acaso | ajustar pelo consumo no período anterior (diferença em diferenças ou CUPED); em 12–15/10 a média prévia da reumatologia foi R$ 193 no controle contra R$ 1.211 na navegação, com 3 pacientes no controle |
| **Contaminação** | paciente de controle pode ser navegado por outra linha, não randomizada | 26 de 191 pacientes de navegação (14%) foram relevantes em outra linha; o mesmo deve ocorrer no controle em escala. Reportar e analisar com e sem eles |
| **Unidades com navegadora** | navegação só acontece em algumas unidades; nas outras o efeito é zero por construção | estratificar por unidade com e sem navegação ativa |
| **Poder** | a decisão de 23/09 mostrou que nenhuma linha isolada conclui | análise **agregada** entre linhas; com 10% nas cinco linhas da onda 1, ~1.908 controles/ano detectam efeito de ~14% |
| **Sem CPF** | não tem grupo; vai à navegação | excluir da análise; acompanhar a taxa (zero no período) |
| **Métricas pré-registradas** | evita escolher a métrica depois de ver o dado | conversão (qualquer consumo), valor faturado, valor no produto da linha |
| **Pacientes repetidos** | o mesmo CPF volta em outro exame | primeira data de randomização por paciente e linha |
| **Desfecho clínico** | valor financeiro não é o único efeito | registrar exame/consulta de seguimento da linha como métrica secundária |

## 5. Próximos passos

**Fase 1**
1. Publicar o painel AI/BI com atenção diária, proporção acumulada, integridade, grupos, funil e
   contaminação (`01`–`04`, `06`, `07`), com filtros de período e linha.
2. Agendar a consulta `06` em dia útil após 08:00, com alerta quando houver `ALERTA`.

**Fase 2**
3. Lista de unidades com navegadora ativa, para o corte por unidade.
4. Primeiro desfecho com janela de 30 dias completa, quando o consumo chegar a ~12/11.
