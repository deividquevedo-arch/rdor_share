---
titulo: Tratamento de texto, transformações e regras técnicas da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - src/.../text_pipeline/ · 0.13.0
  - docs/spec-0.11.2 · espaço colado antes de acento
  - docs/spec-0.11.1 · negação no semântico
  - RELEASE.md · 0.13.0
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém desenha a cadeia de transformação da prancha 2 na ordem correta, e sabe
  nomear cada regra técnica pela consequência que ela produz em dado errado se for violada.
relacionado:
  - 02-arquitetura-e-camadas.md
  - 03-inventario-de-objetos.md
  - 09-codigos-decisoes-e-intencao.md
---

# Tratamento de texto, transformações e regras

> **O que este arquivo é:** o que acontece com o texto, na ordem em que acontece, e as regras cuja violação produz dado errado em silêncio.
> **O que ele não é:** as etapas de decisão — isso é o `02`; nem o que a lib devolve — isso é o `06`.

## 🔴 Desvio de unidade, declarado

**Não há *jobs*.** A lib é síncrona, sem estado entre chamadas, e não agenda nada — quem agenda é
o job da plataforma. A tabela de "cargas" abaixo descreve a **unidade de trabalho da lib**: uma
chamada a `process()` sobre um lote.

## A unidade de trabalho

| job | gatilho | ordem | janela | estrategia | idempotente | o_que_quebra_se_rodar_duas_vezes |
|---|---|---|---|---|---|---|
| `ClinicalNlpEngine.process(rows, cfg)` | chamada do runner | única | o lote que o chamador entrega | devolve `list[dict]`; **não escreve** | **sim**, com uma ressalva | nada na lib. ⚠️ A ressalva é o juiz: o LLM não é determinístico, então dois runs sobre o mesmo laudo **dentro da banda** podem divergir. Fora da banda, byte a byte |

ℹ️ **A idempotência foi provada onde é determinística:** 2.920 linhas, `sha256` idêntico em duas
árvores de trabalho (ver `05`). O golden estuba o LLM justamente para isolar essa propriedade.

## O tratamento de texto, na ordem em que roda

**A ordem é conteúdo, não formatação.** Trocar dois passos muda o que a régua lê.

| # | passo | módulo | o que faz | armadilha registrada |
|---|---|---|---|---|
| 1 | conversão para texto plano | `to_plain` | RTF e HTML viram texto | ⚠️ uma fatia dos laudos chega como **documento RTF inteiro numa linha só**; o maior medido tem 814.685 caracteres |
| 2 | fallback de RTF | `rtf_fallback` | extrai texto quando a conversão falha | passa adiante o que não é RTF, sem tocar |
| 3 | HTML para texto | `html_plain` | remove marcação e descarta `<script>` | — |
| 4 | remoção de boilerplate | `boilerplate` | tira cabeçalho de sistema e ruído de formatação | ⚠️ regra por **linha**: laudo inteiro numa linha já foi apagado por completo (corrigido na `0.9.1`) |
| 5 | remoção de rodapé | `footer` | tira aviso de sistema, instrução de visualização e a seção de referências | ⚠️ o corte de referências depende de dois *lookbehind* negativos; sem eles, `.*$` com `DOTALL` **trunca o laudo e apaga a IMPRESSÃO** |
| 6 | normalização | `norm` | minúsculas, acentuação, espaços | 🔴 **`to_plain` já colou espaço antes de palavra acentuada** — `sem úlceras` virava `semúlceras`, o negador sumia, e 6 laudos foram entregues dizendo o oposto. Corrigido na `0.11.2` |
| 7 | padrão de acento | `accent_pattern` | casa termo com e sem acento | — |
| 8 | âncoras | `anchors` | localiza o trecho no texto original | sustenta `findings_spans` |
| 9 | segmentação | `by_headers` | recorta por cabeçalho quando `mode: auto` | 🔴 na hepatologia descarta **86%** — `segmentation_coverage < 1,0` em 3.867 de 4.507 |
| 10 | negação | `negation` | marca o achado negado na janela | ⚠️ a direção default é `left`; declarar `None` **não** cai no default |

## Mapa de tipos, entrada → saída

| origem_tipo | destino_tipo | conversao | armadilha |
|---|---|---|---|
| `str` (RTF cru) | `str` (texto plano) | `to_plain` | payload hexadecimal de imagem embutida; leitura em lote estoura o teto de 25 MB por resposta |
| `str` (HTML de editor) | `str` | `html_plain` | ⚠️ 21 de 10.000 laudos num run de dev tiveram o texto **zerado** pelo tratamento — entrada de 158 a 64.512 caracteres |
| `str` numérico (`"1,6"`, `"16mm"`) | `float \| None` | `_single_float` | **mais de um número devolve `None`** — ambiguidade é recusa deliberada, não falha |
| valor de config qualquer | `float` | `_finite_float` | `inf` e `NaN` caem no default; **nunca saturam** |
| qualquer | `float` em `[0,1]` | `_clip01` | 🔴 **`NaN` é inválido, nunca zero** — se sobrevivesse, contaminaria toda comparação a jusante, porque comparação com `NaN` é sempre falsa |
| `list[dict]` | `str` (JSON) | serialização do blob | o blob é string na saída, não estrutura |

## Regras técnicas obrigatórias

Cada uma escrita pela **consequência em dado errado**, não em código. Todas têm incidente real
atrás.

| regra | o_que_acontece_se_violada | codigo |
|---|---|---|
| A ordem do tratamento de texto não se altera sem medir | achado some ou aparece em silêncio; o texto tratado é o que a régua lê, e ninguém audita o que não vê | `R11` |
| Chave que decide comportamento é **sempre declarada**, mesmo igual ao default | `llm_router` com modelo, banda e prompt completos mas **sem `enabled`** não roda o juiz — e quem lê conclui o contrário | `R4` |
| `runtime` e `nlp` mantidos coerentes, e `nlp` declarado **antes** de remover o `runtime` | inverter desliga o juiz e muda a política de falha **sem erro e sem log** | `R1` |
| Falha de infraestrutura nunca vira decisão clínica | erro de LLM rebaixando por "medida ausente" transforma indisponibilidade em negativa clínica | `D5` |
| A camada semântica não promove trecho negado | a régua nega e a semântica promove **o mesmo texto** — aconteceu até a `0.11.1` | `D6` |
| Âncora ausente não significa "não se aplica" | o critério some do gate e a promoção sai **sem nunca ter sido conferida** — 36 de 1.032 entregas | `D7` |
| Config com bloco morto não passa em revisão | os valores são plausíveis — catálogo que existe, prompt inteiro — e ninguém desconfia | `R12` |
| Lista ao negócio só a partir do **fluxo completo calibrado** | `rule_only` e híbrido puxam em direções **opostas**; homologação não transfere entre perfis | `D9` |
| Medição só vale com a **população** presente na coorte | zero divergência sobre coorte sem os casos afetados não mede nada | `R13` |
| Todo número com denominador e data | `76,7%` é ruído; `3.412 de 4.448 em 2026-07` é dado | `R14` |

🔴 **As quatro primeiras já produziram entrega errada em produção.** Não são boas práticas — são
cicatrizes.

## Filtros da extração — fronteira declarada

⚠️ **O `gold_filter` não é da lib.** Ele vive na configuração da especialidade e é aplicado pelo
**runner**, antes de a lib ver qualquer coisa, como `rlike` sobre `proced_descricao` — a descrição
do procedimento, **não o texto do laudo**.

Está aqui porque quem desenha a prancha 2 precisa saber que existe um filtro **antes** do primeiro
bloco da lib, e que erro nele é invisível do lado de dentro: o exame nunca chega, então não existe
linha de saída para denunciar. Medido no `cancer_estomago`: o filtro deixava de fora **mais laudo
legível do que trazia** — 124/dia contra 106/dia.
