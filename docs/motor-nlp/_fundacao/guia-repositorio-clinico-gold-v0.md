# Guia do repositório clínico — `tb_gold_mov_exame` (v0, 2026-09-18)

> O que a medição da gold inteira em 12 meses mostrou: onde está o laudo, o que cada família de
> conteúdo significa, quais colunas enganam, como filtrar exames sem perder nem inflar, e as
> consultas que reproduzem os números entregues ao Head em 18/09. Vale para qualquer especialidade
> que leia laudo da gold. Diário em `_processo/diario/2026-09-16.md`.

## 1. A tabela

`gold_corporativo_ia.corporativo.tb_gold_mov_exame` · 92 colunas · uma linha por exame (21 linhas
duplicadas em 340 mil de colonoscopia e endoscopia; deduplicar por `id_exame` mesmo assim).

| coluna | o que é | cuidado |
|---|---|---|
| `id_exame` | chave do exame | contar **exames distintos**, nunca linhas |
| `dt_dia_exame` | data do exame | janela por ela |
| `etl_data_atualizacao_carga_gold` | carga | a carga das 10:55 de 18/09 acrescentou 378 colonoscopias entre duas extrações; **não serve para congelar**: exames recarregados depois ganham carga nova e somem do filtro `<= T` (40.517 de agosto entre 18 e 22/09). Congelar com `TIMESTAMP AS OF '<T>'` |
| `proced_lista_exames` | **array de struct** com `laudo_original`, `laudo_transformado`, `nme_exame`, faixas de referência | exame pode ter **array nulo ou vazio**; o `explode` o elimina. Contar antes |
| `laudo_transformado` / `laudo_original` (no array) | texto limpo / texto bruto (RTF, HTML) | usar `coalesce(nullif(trim(laudo_transformado), ''), laudo_original)` |
| `proced_descricao_ajustado`, `dsc_codigo`, `cod_procedimento` | descrição do procedimento (três grafias) | normalizar NBSP `[\u00A0\u2007\u202F]`; o TUSS vem **dentro** da descrição, `cod_procedimento` não é TUSS puro |
| `etl_tipo_integracao` (= `tp_procedimento` = `nme_categoria`) | LABORATORIO / IMAGEM / ANATOMIA | **não é modalidade**: 45% do IMAGEM é laboratório pela descrição; ANATOMIA tem 400 mil exames de imagem. Mudou de significado em dez/25 |
| `etl_nome_sistema_origem` | TASY, WPD, SMART, mv_soul, mv, MV SOUL, MV | grafias duplicadas de MV |
| `num_pedido_integracao` | pedido da integração de resultado | preenchido em 97–99% dos "sistema externo", 43–45% dos legíveis, **0% dos PDF, vazios e sem item**; `''` agrupa 8,6 mil exames, tratar como nulo |
| `id_encounter`, `id_patient` | atendimento, paciente | 100% preenchidos; chave alternativa |
| `emp_descricao_unidade`, `nme_region` | unidade, regional | unidade é o corte que separa PDF, vazio e sem item |
| `nme_origem_exame` (= `tp_proced_classe_encontro`) | Internação, Externo, Pronto Socorro, Ambulatorial | |

Outras colunas de nome (`nme_codigo`, `proced_descricao`, `dsc_procedimento_integracao`) não
escondem exame digestivo; só otorrino. Não precisam entrar no filtro.

## 2. As famílias de conteúdo do laudo

Classificar **pelo marcador, nunca pelo tamanho**, sobre o texto normalizado (minúsculas, sem
acento, espaços colapsados). Um exame com vários itens fica com o **mais informativo**.

| ordem | família | regra | o que é de verdade |
|---|---|---|---|
| 0 | **sem item** | array nulo ou `size = 0`, contado **antes** do `explode` | registro de atendimento ou faturamento sem integração de resultado; 100% sem pedido. 92% dele em 12 meses é TASY (11,26 M); WPD 0,97 M; MV ~0. Nas endoscopias, unidades específicas e linhas de procedimento faturado |
| 1 | **sistema externo** | contém `sistema especialista` | aviso "acesse o viewer / a imagem"; tem mais de 120 caracteres e engana quem classifica por tamanho. Sumiu em 20/07/26 nas endoscopias |
| 2 | **laudo em PDF** | começa com `laudo em pdf` ou `resultado pdf`, menos de 40 caracteres | o arquivo existe no Viewershell; alvo do CDI |
| 3 | **anexo** | `laudo em anexo`, `vide laudo`, `pasta anexos`, `proxima pagina`, `outro registro`, `laudo anexado`, menos de 400 | |
| 4 | **outro sistema** | `interbase`, `sistema his`, menos de 200 | |
| 8 | **não-laudo** | começa com `detectado`, `reagente`, `material:`, `metodo`, menos de 120 | item de laboratório sob procedimento de endoscopia; zero no recorte digestivo |
| 5 | **vazio (em branco)** | menos de 120 caracteres sem casar nada acima | 99,9% têm até 20 caracteres; em laboratório é a natureza do exame, fora dele é defeito de integração (Santa Helena) |
| 6 | **legível** | o resto | 97,9% contêm termo clínico |

O sufixo *"este laudo pode não estar completo aqui em função do padrão de arquivo RTF..."* é colado
em laudos inteiros e **não é marcador** (46 mil legíveis o carregam). "Em HTML" é atributo do
legível, não família.

**Colunas gerenciais:** recuperáveis = PDF + sistema externo + anexo + outro sistema; perdidos =
sem item + vazio; legíveis + recuperáveis + perdidos = bruto, em toda linha.

## 3. Universo: gold inteira, classificada pela descrição

Nunca filtrar o universo pelo rótulo de integração. Classificar cada exame pela descrição em grupos
globais (regex em `cdi_grupos.py`, ordem importa): colonoscopia, endoscopia digestiva, outras
endoscopias, ecocardiograma, ECG/Holter/MAPA, ergométrico, função pulmonar, mamografia, medicina
nuclear, TC, RM, US e doppler, radiografia e densitometria, hemodinâmica, neurofisiologia, anatomia
patológica, outros procedimentos, laboratório (resíduo). O laboratório é 86% do volume e devolve
número, não laudo: "em branco" ali não é problema.

Números de referência (12 meses set/25–ago/26, snapshot de 18/09 15:42): gold 66.878.996; sem
laboratório 9.146.113 (75,6% legíveis, 4,2% recuperáveis, 20,1% perdidos); agosto sem laboratório
720.556 (94,4 / 4,1 / 1,6). Uma remedição com `TIMESTAMP AS OF '2026-09-18T15:42:01'` devolve esses números (agosto conferido
ao exame em 22/09: 5.014.642). Sem time travel, a diferença é recarga, não método. A tabela não declara
retenção própria; no padrão do Delta o snapshot deixa de ser alcançável poucos dias depois.

## 4. Filtro de exame para colonoscopia e endoscopia (final v3)

Sobre `exame_nr` = `regexp_replace(lower(coalesce(proced_descricao_ajustado, cast(cod_procedimento as string), '')), '[\u00A0\u2007\u202F]', ' ')`, e o mesmo para `dsc_codigo_txt` e `cod_procedimento_txt`.

- **Exclusão comum:** `nasoss|naso-?sinus|naso sinus|naso - sinus|nasofibro|nasal|endonasal|sinus|laring|etmoid|adenoid|velo-?palat|epistaxe|septo|rinoss|oximetria|anestesia|microdata|otol|ouvid|urolog|ureter|vesical|coluna|vertebral|artrosc|bronc|hemodinamica`. Não usar `naso` sozinho: "passagem de sonda nasoenteral por endoscopia" é digestiva.
- **Colonoscopia:** `%colono%` nas três colunas **ou** `colonosc|colonossc|videocolon|ileocolon|retossigmoidosc`; menos a exclusão.
- **Endoscopia digestiva:** `%endoscopia%` nas três colunas **ou** `\beda\b|esofagogastro|gastroduodenosc|gastroscop|ecoendosc|enterosc|\bcpre\b|colang[ie]o?pancreatografia retrograda|capsula endosc`; menos a exclusão; menos o que já é colonoscopia.
- **Camada 2, procedimento terapêutico** (polipectomia, hemostasia, mucosectomia, ligadura, gastrostomia, papilotomia, dilatação, corpo estranho): reportar à parte, nunca somar. Quase tudo sem item: é a linha faturada ao lado do exame que tem o laudo.
- **Anuscopia:** fora, até o negócio decidir (~17 mil linhas/ano, 4,5 mil com laudo).

Por que: o `%endoscopia%` do time trazia ~20 mil linhas/ano de otorrino, anestesia, oximetria e
faturamento (16,5 mil sem item, inflando a família em 40%) e deixava fora retossigmoidoscopia
(2,3 mil laudos/ano) e CPRE. Inventário nos dois sentidos antes de fechar qualquer filtro: as 20
descrições que entram e as 20 que a exclusão tira.

## 5. Consultas que reproduzem os números

**Um mês, toda a gold, por tipo de exame, cinco critérios** (base de tudo; ~5 min por mês):
`.alt.doc/CDI/volumetria/data/rel2_genie/sql_agosto_2026_por_tipo_de_exame.sql`. Trocar as datas
para outro mês; rodar três meses em paralelo; agregar em Python. Versão restrita ao universo IMAGEM:
acrescentar `AND m.etl_tipo_integracao = 'IMAGEM'`.

**Esqueleto** (o que toda consulta de laudo precisa ter):

```sql
WITH d AS (  -- 1. janela + congelamento + dedup
  SELECT m.*, row_number() OVER (PARTITION BY m.id_exame ORDER BY m.etl_data_atualizacao_carga_gold DESC) AS rn
  FROM gold_corporativo_ia.corporativo.tb_gold_mov_exame TIMESTAMP AS OF '<T>' m
  WHERE m.dt_dia_exame >= DATE'2026-08-01' AND m.dt_dia_exame < DATE'2026-09-01'
), b AS (  -- 2. sem item ANTES do explode
  SELECT m.id_exame, <descricao normalizada> AS tipo_exame,
         CASE WHEN m.proced_lista_exames IS NULL OR size(m.proced_lista_exames) = 0 THEN 1 ELSE 0 END AS sem_item,
         m.proced_lista_exames AS lista
  FROM d m WHERE m.rn = 1
), i AS (  -- 3. melhor item por exame, classificado por marcador
  SELECT id_exame, min(CASE WHEN l LIKE '%sistema especialista%' THEN 1
                            WHEN l RLIKE '^ *(laudo|resultado) em pdf' AND length(l) < 40 THEN 2
                            WHEN l RLIKE 'laudo em anexo|vide laudo|pasta anexos|proxima pagina|outro registro|laudo anexado' AND length(l) < 400 THEN 3
                            WHEN l RLIKE 'interbase|sistema his' AND length(l) < 200 THEN 4
                            WHEN l RLIKE '^(detectado|nao detectado|n/a|reagente|nao reagente|material:|metodo)' AND length(l) < 120 THEN 8
                            WHEN length(l) < 120 THEN 5 ELSE 0 END) AS rk
  FROM (SELECT b.id_exame,
               regexp_replace(lower(translate(trim(coalesce(nullif(trim(x.laudo_transformado), ''), x.laudo_original, '')),
                              'áàâãéêíóôõúçÁÀÂÃÉÊÍÓÔÕÚÇ', 'aaaaeeioooucaaaaeeioooucc')), '\\s+', ' ') AS l
        FROM b LATERAL VIEW explode(b.lista) t AS x WHERE b.sem_item = 0) GROUP BY 1
), e AS (SELECT b.*, CASE WHEN b.sem_item = 1 THEN 7 ELSE coalesce(i.rk, 5) END AS fam FROM b LEFT JOIN i USING (id_exame))
SELECT ... FROM e GROUP BY ...
```

**Verificações que acompanham qualquer número** (imprimir, não supor):
`<T>` do `TIMESTAMP AS OF` e `max(etl_data_atualizacao_carga_gold)`; linhas removidas pelo dedup; soma das famílias =
bruto em toda linha; soma dos meses = total; as 20 descrições que entram no filtro e as 20 que a
exclusão tira; cobertura de `num_pedido_integracao` por família; distribuição de comprimento dos
"vazios"; proporção de legíveis com termo clínico.

**Sanidades conhecidas:** jun/26 EDA em sistema externo ≈ 2.950 de 4.669; ago/26 colono + endo em
sistema externo = 62 (33 + 29); colonoscopia 12 m sem item ≈ 26,7 mil; legíveis com sufixo RTF
≈ 46 mil; PDF na gold inteira = PDF no universo IMAGEM = 83.454 (12 m).

## 6. O que a gold mostra sobre o negócio

- **Origem (12 meses):** TASY 48,07 M (71,9%), WPD 10,62 M, SMART 3,99 M, MV 4,20 M em quatro grafias.
  O "sem item" é 92% TASY; MV praticamente zero. **SMART é só laboratório e parou de alimentar a
  gold em jul/26** (284 mil exames em junho, 37 em agosto).
- **Duas viradas:** dez/25–jan/26, gold inteira, "sem item" de 34% para 7% (mudança de integração
  não documentada; mesma data em que o rótulo IMAGEM mudou de significado); 20/07/26, endoscopias,
  marcador do viewer some nas mesmas unidades com o mesmo volume (São Luiz e Brasil Santo André).
- **Corrente (agosto/26), fora do laboratório:** imagem 97–100% legível; endoscopias 91%; PDF em
  cinco unidades TASY (Tatuapé, Central Sul, Samer, Central Leste 100%; Barra D'Or II misto);
  Santa Helena 100% vazio; sem item em Quinta D'Or e Sorocaba (WPD) e Arthur Ramos e Alphamed (TASY).
- **O maior volume de PDF e "outro sistema" hoje é cardiologia gráfica** (ergométrico 39% PDF,
  MAPA 40%, Holter 30%, ECG 12%: 7 mil dos 11,9 mil PDFs do mês), e obstetrícia aponta para outro
  sistema em 82–99%. Teste ergométrico é o único tipo piorando desde julho.
- **Passivo de 12 meses nas endoscopias:** 237 mil sem texto, 140 mil no viewer, 67 mil sem item.
  Recuperar o viewer depende de ler `especialistas.pacs_streaming.laudopdf_*` e
  `diamond_axis_hml.axis_viewer_bkp_*`, **sem permissão** para o time. `gold_fabrica_ia_dev.fhir.
  laudo_diagnostico` existe com 0 linhas; o legado `tb_diamond_mod_colon_entrada` é TC de abdome e
  não é fonte.

## 7. Como auditar um relatório gerado (Genie ou outro)

1. Somar a tabela mensal antes de aceitar a capa: a de 18/09 tinha "sem item" pela metade só no
   sumário.
2. Exigir o filtro do universo impresso: 15,35 M contra 66,9 M era um `WHERE` não declarado.
3. Somar as colunas de uma linha: se excede o bruto, há coluna-atributo contando duas vezes (HTML).
4. Distinguir "consultado e negativo", "existe e não consultado", "sem permissão": a Genie escreveu
   "não existe" para tabela existente.
5. Reproduzir a categoria dela sobre a referência antes de comparar: Ecocardiograma dela tinha 23 mil
   porque "ecodopplercardiograma" casou "doppler".
6. Percentual de confiança sem amostra e intervalo não é estatística. Censo prova consistência;
   amostra usa Wilson.
7. Prompt v3 fechado em `.alt.doc/CDI/prompt-genie-refinamento-laudos.md`.

## 8. Onde está tudo

- Entregas para o time: `.alt.doc/CDI/` (síntese orientadora, comparativo, consolidado, Excel
  consolidado com 20 abas, Excel no formato da Genie, gráficos em `volumetria/graficos/`).
- Dados: `.alt.doc/CDI/volumetria/data/rel2_genie/` (CSVs finais) e
  `.alt.doc/CDI/volumetria/data/ref_2026-09-18/` (mensais `tipo_*.csv` gold inteira e
  `imagem_*.csv` universo IMAGEM; validações; chave).
- Regex declaradas: `cdi_grupos.py` (grupos globais), `cdi_categorias_genie.py` (31 categorias da
  Genie), `cdi_familias.py` (50 famílias da planilha de 26/08, 16 fora de tolerância). Copiar para
  `_ferramentas/` antes de o diretório do job ser apagado.
- Mapa visual: `.alt.doc/CDI/mapa-repositorio-clinico-gold-2026-09-22.drawio`.
- Operação: `databricks experimental aitools tools query "<sql>" --profile nlp-platform`; um mês por
  consulta, três em paralelo; sete concorrentes arrastam todas.
