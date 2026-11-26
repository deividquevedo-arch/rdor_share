# SPEC — Ca de Cólon por tomografia: linha própria na plataforma (correção do `420309`)

> **Card:** `420309` — *[Ca de Colon] Sem envios de TC e RM* (Defect). **Versão:** v0, 26/11/2026.
> **Escopo:** recolocar em produção a captação de Ca de Cólon a partir de **laudo de tomografia de abdome/pelve**,
> parada desde 02/10/2026, migrando para a plataforma a régua de TC do legado.
> **Não é escopo:** colonoscopia (já em produção na `cancer_colon`), biópsia (motor de regras estruturadas),
> ressonância (ver D3), juiz LLM e camada semântica (fase 2).

---

## 1. O problema, medido

| fato | número | fonte |
|---|---|---|
| Última execução do legado de TC | **01/10/2026** (job pausado em 02/10 pelo card `360625` — *[Captação] Envios de e-mail duplicados em Neuro, Colon e Ateromatose*) | `legado.tb_diamond_mod_colon_saida` |
| Universo do legado | ~46 mil TC/mês; set/2026: 46.485 exames, **167 relevantes** (0,36%) | idem |
| Relevantes/mês jun–set | 221 · 189 · 212 · 178 | idem |
| `cancer_colon` na plataforma | só colonoscopia (`gold_filter: ['colonoscop']`); TC entra por acaso, 1–4/mês | `tb_mod_diamond_cancer_colon_saida_v0` (prd) |
| Perda desde 02/10 | ~350–400 relevantes de TC não enviados | estimativa pelo ritmo mensal |

**Por que não basta ampliar o filtro da `cancer_colon`** — medido em dev (run `928025031296291`, 25/11; config
`0.4.0`, lib `0.15.4`, exames de 01–28/09, 43.304 TC pareadas com o legado):

| | legado relevante | legado não relevante |
|---|---|---|
| plataforma relevante | 127 | **1.439** |
| plataforma não relevante | **34** | 41.704 |

A régua de colonoscopia marca ~10× mais TC (1.129 só pelo achado "Sinal endoscópico de gravidade", 99% pelo termo
`infiltra*`) e perde 21% do legado. A régua de TC é outra e precisa de linha própria.

## 2. A régua do legado — procedência e conteúdo

| | |
|---|---|
| repositório | `IAAzureDatabricksColon` (Azure DevOps `RDOr-Corp`, projeto `IA`) |
| branch / commit lido | `hml` / `999bdf39` (04/09/2026, PR 7208) — ⚠️ **a confirmar no job** (passo 0) |
| entrada | `colon/data/ntb_ia_colon_entrada_v2.ipynb`, célula 11 (view balizadora) |
| régua | `colon/model/ntb_ia_colon_algoritmo.ipynb`, **célula 16** ("v4 do Fabio, 13/02/2026"); células 15 e 17 inteiramente comentadas |
| decisão | `fl_relevante = 1` se `process_report(laudo, target_organ='colon_reto')` devolve algum achado não negado |
| saída | `tb_diamond_mod_colon_saida` (`exm_laudo_achados` = famílias; `exm_laudo_resultado` = trechos do laudo) |

**Entrada do legado** (o que a linha nova tem de selecionar):
`tp_procedimento IN ('IMG','IMA')` **e** (`tomografia|tomo|tc`) **e** (`abd|pelve`) **e não** `angio` **e não** `superior`,
sobre `proced_descricao_ajustado`, `dsc_codigo` e `cod_procedimento`; `modalidade != 'COL'`; janela por
`COALESCE(dt_liberacao_laudo, dt_previsao_laudo)` dos últimos 30 dias, deduplicada por exame.
**Ressonância não entra.**

**Régua (célula 16, `CONFIG` linhas 327–675; `TARGET_ORGAN = 'colon_reto'`, `FORCE_FULL_DOC_FOR = set()`):**

| bloco | conteúdo |
|---|---|
| achados `colon_reto` | 4 famílias, **53 termos**: `lesao` 10 · `polipo` 18 · `ulceracao` 10 · `tumor_ou_massa` 15 |
| órgão `colon_reto` | 44 seeds, 14 regex (inclui `diverticul*` como âncora de órgão) |
| negação | 23 negadores, `window_tokens: 7`, lido nos dois lados da janela |
| semântica | `min_sim_seed_vocab 0.65`, `top_k_per_seed 24`, `min_token_len 2`; MiniLM multilingual |
| léxico do órgão | só lexical: seeds + fuzzy no vocabulário do laudo (`min_ratio 0.86`, 2 por seed) |
| segmentação | cabeçalhos (`HEADER_ALIASES`); sem cabeçalho, âncora de órgão |
| gate de órgão | (A) achado a ≤ 200 caracteres de menção ao órgão; (B) **"órgão mais próximo vence"** (`_snippet_mentions_other_organs`): na sentença, o achado é descartado se **qualquer** dos 19 órgãos do `CONFIG` estiver ≥ 12 caracteres mais perto que `colon_reto` |
| órgãos do `CONFIG` | 19 entradas em `organs` (fígado, vias biliares, vesícula, pâncreas, baço, rins, ureteres, bexiga, adrenais, útero, ovários, próstata, vasos, peritônio, retroperitônio, pulmão, conclusão, `colon_reto`, `reumatologia`) e o bloco `findings.reumatologia` — **os outros órgãos não são vocabulário morto aqui: alimentam o desempate (B)** |

**A expansão semântica do legado** (`semantic_expand`) exige sobreposição lexical com o termo: produz variantes
morfológicas dos 53 termos, não termos novos (mesmo mecanismo documentado na config `doencas_biliares`, cabeçalho
"Expansão semântica do legado").

## 3. A linha nova — o que construir

| item | valor proposto | observação |
|---|---|---|
| `specialty_id` / schema | **`cancer_colon_imagem`** | D1; cobre TC e, se decidido, RM sem renomear |
| arquivos (6) | `jobs/clusters/cancer-colon-imagem-batch.json`, `jobs/definicoes/cancer-colon-imagem-batch.json`, `plataform/config/exchange/{dev,hml,prd}/ntb_ia_cancer_colon_imagem_navegacao.py`, `plataform/config/speciality/ntb_ia_cancer_colon_imagem_config.py` | padrão de linha nova |
| `nlp_engine_version` | **`"0.12.3"` literal** | versão das 12 linhas; nunca `${nlp_engine_version}` |
| `pause_status` | `"${job_status_cancer_colon_imagem_batch}"` (sem `?`) | variável nasce `PAUSED` em `jobs/ambientes` |
| perfil | `rule_only`: `embeddings.use_embeddings: False`, `llm_router.enabled: False` declarados | D4 |
| navegação | clone da `ntb_ia_cancer_colon_navegacao.py` (22 colunas, mesma lista de 103 ids de unidade, mesmos destinatários por ambiente), `id_linha_navegacao: cap:lin:cancer-colon`, `projeto` próprio | D2 |

**Config — tradução da régua, adaptações de motor anotadas no cabeçalho (molde: `doencas_biliares` `0.3.2`):**

1. **Gerar programaticamente** do `CONFIG` da célula 16, portando **só** `organs.colon_reto` e `findings.colon_reto`.
   Termos, seeds, regex e negadores entram **como estão, erros inclusive**. Nenhum termo clínico novo.
2. `data.filters.gold_filter.keywords`: `['^(?=.*(tomo|tc))(?=.*(abd|pelve))(?!.*(angio|superior))']`
   (sobre `proced_descricao`, única coluna da plataforma). Sem `\b` e sem acento no literal.
   Medido na gold (exames 01–28/09): **44.080** selecionados; cobre **43.314 de 44.635** do legado (97,0%) e
   **161 de 163** relevantes.
3. `segmentation.mode: 'full_doc'` — `auto` não tem os `HEADER_ALIASES` do legado e lê 2–22% do laudo
   (medido nas biliares). O recorte fica no gate de órgão.
4. `findings_policy: {'organ': {'scope': 'block', 'max_chars': 200}}` — gate (A) de 200 caracteres do legado.
   🔴 **O desempate (B) não tem equivalente na lib** (registrado no porte das biliares: "esta lib não tem essa regra").
   Nas biliares o efeito foi nulo; **em TC de abdome não deve ser**, porque o laudo cita vários órgãos na mesma
   frase ("lesão hepática… alças colônicas…"). É a fonte mais provável de falso positivo da linha. Tratamento:
   medir no passo 4 quantas divergências "só plataforma" têm outro órgão mais perto; mitigar com `exclude`/`unless`
   por família (adaptação de motor, uma por rodada). Se não bastar, a regra vira demanda de lib (`nlp-engine-lib`,
   SPEC própria, global e configurável) — **não** código na config.
5. `negation`: os 23 negadores, `window: 7`; direção `left` (fiel ao comportamento do legado, decisão já tomada nas
   biliares); medir `both` na bancada e registrar o delta no changelog.
6. Expansão semântica → **variantes observadas viram `regex`** do termo de origem, só depois de medidas (passo 4).
7. `column_map` igual ao da `cancer_colon` (`exm_laudo_texto` = `proced_laudo_exame_original`); o legado lia
   `laudo_transformado` — divergência de fonte de texto a medir na paridade (RTF cru, memória `laudo-original-e-rtf-cru`).
8. Sem blocos mortos (`catalog`, `monitoring`, `distribution`); `llm_router.enabled` e `use_embeddings` declarados.

## 4. Passos, em ordem

| # | passo | saída verificável |
|---|---|---|
| 0 | **Confirmar a fonte que o job rodava**: job do legado de TC no workspace antigo (`adb-2013197995950192`) → repositório, branch e commit do clone em `/Repos/...`; comparar a célula 16 chave a chave com a `hml@999bdf39` | procedência registrada no cabeçalho da config (nas biliares o job rodava `refatoracao`, não `hml`) |
| 1 | Extrair o `CONFIG` (script versionado, sem PHI) e gerar a config `0.1.0-cancer_colon_imagem` | config carrega no `ConfigLoader`; `config_version` no padrão |
| 2 | Medir o filtro nos dois sentidos, mesmo eixo de data | perdidos e a mais, com quantos relevantes do legado em cada lado |
| 3 | Rodar em dev (receita: memória `dev-instala-do-feed-de-prd`, seção "rodar o motor em dev fora dos jobs") sobre os exames de **01–28/09/2026**, `model_version` de bancada | `fl_relevante` por `id_exame` gravado; pareamento com o legado por `id_exame` = `exm_an` |
| 4 | Abrir as divergências **pela trilha**: lado plataforma pelo `decision_trail`; lado legado pelo `colon/analysis/ntb_ia_colon_algoritmo_log.py` (rastro por achado) | cada divergência com causa: expansão, negação, gate, segmentação, texto-fonte |
| 5 | Ajustar só **adaptação de motor** (regex de variante, direção de negação, gate) e re-medir; uma variável por rodada | changelog com o número de cada versão |
| 6 | Medir o efeito dos demais órgãos: rodar com e sem eles **e** contar, nas divergências, os casos que o desempate (B) do legado descartaria | decisão de portar ou não, e se (B) vira demanda de lib, com o delta |
| 7 | e2e em dev: view + arquivo de navegação (skill `validar-arquivo-navegacao`) | arquivo com as 22 colunas e unidades navegadas |
| 8 | PR para a `hml` com a medição na descrição (≤ 4.000 caracteres; detalhe nesta SPEC) | revisão do time de plataforma |
| 9 | Promoção: `disabled` nas tasks de exchange **no mesmo commit** da ida à `main`; schema em prd; cadastro de envio; data de início (D5) | primeira execução em prd sem carga seca |

## 5. Critérios de aceite

| # | critério | como verificar |
|---|---|---|
| CA1 | Filtro cobre ≥ 95% dos exames que o legado selecionava, sem perder relevante do legado além de 2% | passo 2, janela 01–28/09 |
| CA2 | Paridade com o legado de TC na janela 01–28/09: relevantes do legado encontrados ≥ 95% (≤ 8 de 163 perdidos) e taxa de relevância entre 0,3% e 0,6% | passo 3; divergências abertas no passo 4 |
| CA3 | Divergências restantes (só legado / só plataforma) avaliadas pela navegação antes de produção | lista sem texto de laudo no card; laudos só no ambiente |
| CA4 | Nenhum arquivo da `cancer_colon` alterado; colonoscopia segue idêntica | `git diff` do PR; controle: decisões da `cancer_colon` inalteradas |
| CA5 | Definição do job: versão literal, `pause_status` por variável sem `?`, `disabled` nas tasks de exchange na promoção | revisão do PR |
| CA6 | Em prd, envio diário pelo fluxo da colonoscopia (view, randomização conforme D6, arquivo/API), sem duplicidade com o legado (que segue pausado) | primeira semana em prd: volume e taxa na ordem do legado |
| CA7 | Escopo da ressonância decidido e registrado no card | D3 |

## 6. Decisões abertas

| # | decisão | recomendação | quem decide |
|---|---|---|---|
| D1 | nome da linha | `cancer_colon_imagem` | DS + plataforma |
| D2 | `projeto` no arquivo e na Central | próprio ("Câncer de Cólon — Imagem"), mesma `id_linha_navegacao`, para o caso ser rastreável por origem | PO |
| D3 | ressonância | fora desta correção (o legado não processava); se entrar, é ampliação com medição própria | PO |
| D4 | perfil `rule_only` para entrega | aceitar como fase 1 por paridade com o legado (que não tinha LLM), como `cancer_colon` e `doencas_biliares`; fase 2 = híbrido + juiz | PO + DS |
| D5 | retroação do período parado (02/10 → estreia) | não enviar retroativo por padrão (risco de carga seca e de caso já resolvido); se o PO quiser, janela e volume medidos antes | PO |
| D6 | randomização | mesma regra da `cancer_colon` (na config global desde 19/10, sem a marca `prd`) | PO |
| D7 | Tábia | entrada em `envio_tabia/projetos.py` e `config_tabia.LINHAS` (repo da Central) só se a linha for à Tábia | dono da Central |

## 7. Dependências externas

- **Schema `cancer_colon_imagem`** em dev, hml e prd — provisionado pelo time da Fábrica (POP-IA-08).
- **Variável `job_status_cancer_colon_imagem_batch`** em `jobs/ambientes` e **cadastro de envio** da linha
  (a view de exportação só mostra datas cadastradas).
- **Acesso ao workspace antigo** para o passo 0.

## 8. O que NÃO fazer

- **Não ampliar o filtro da `cancer_colon`** — medido: 1.439 falsos positivos em 28 dias.
- **Não reativar o job legado** — duplicaria envio (motivo da pausa no `360625`).
- **Não acrescentar termo clínico**, nem remover termo "no olho": régua muda com dono clínico e medição.
- **Não ligar embeddings nem juiz** nesta correção sem a decisão D4.
- **Não descartar nem portar os outros órgãos** sem a medição do passo 6 — aqui eles têm função (desempate B).
- **Não ler nem copiar trechos de laudo** para fora do ambiente (`exm_laudo_resultado` do legado guarda trechos).

## 9. Dimensionamento

~46 mil TC/mês (~1,5 mil/dia), `rule_only`: a medição de 52 mil laudos levou ~3h45 num cluster D8ds_v5 de bancada;
a execução diária fica em minutos. Esforço estimado (DS): passos 0–6, 2 a 3 dias; 7–8, 1 dia; promoção depende das
dependências da seção 7.
