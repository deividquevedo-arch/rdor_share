---
titulo: Índice e fontes do pacote de briefing da nlp-engine
tipo: briefing-de-diagramacao
solucao: nlp-engine — biblioteca de NLP clínico
projeto: Documentação visual de arquitetura de dados e APIs
autor: Ciência de Dados e IA — dono da biblioteca
criado_em: 2026-09-18
atualizado_em: 2026-09-18
status: vigente
versao_contrato: 0.13.0
fontes:
  - RELEASE.md · 0.13.0 · 2026-09-18
  - contracts.py · 0.13.0
  - tests/api_surface.json · gerado, travado por gate
  - docs/REFERENCIA-API.md · gerado, travado por gate
  - docs/spec-0.10.0 a spec-0.13.0 · 7 SPECs
  - docs/adr/0001, 0002 · status proposto
publico: Engenharia de Dados · Arquitetura · Product Owner
objetivo: >
  Depois de ler, alguém sabe quais arquivos do pacote respondem o quê, de onde veio cada
  afirmação, e o que deliberadamente não foi apurado — sem abrir o repositório.
relacionado:
  - 09-codigos-decisoes-e-intencao.md
---

# Índice e fontes

> **O que este arquivo é:** o mapa do pacote, a procedência de cada afirmação e a lista honesta do que não foi possível apurar.
> **O que ele não é:** conteúdo técnico. Toda decisão e todo código `D/P/L/R/BL/RNFD` mora no `09`; os demais citam.

## Alcance

**Este pacote descreve a biblioteca `nlp-engine` na versão `0.13.0`, da entrada de um laudo em
texto até a linha de decisão que ela devolve ao chamador — e a observabilidade que ela expõe.
Não descreve a plataforma que a chama.**

## Os arquivos

| arquivo | o que responde | prancha que alimenta |
|---|---|---|
| `00-indice-e-fontes.md` | de onde vem cada afirmação, e o que falta | rodapé de todas |
| `01-visao-escopo-e-atores.md` | que pergunta a lib responde, o que ela **não** faz, quem a chama | 1 · PDF §1 e §2 |
| `02-arquitetura-e-camadas.md` | as etapas de decisão, em ordem, e como cada uma degrada | 1 e 2 |
| `03-inventario-de-objetos.md` | as unidades de configuração que a régua declara | 2 e 4 |
| `04-cargas-transformacoes-e-regras.md` | o tratamento de texto na ordem em que roda, e as regras técnicas | 2 e 3 |
| `05-chaves-cobertura-e-lacunas.md` | a cascata de decisão com denominador, e a cobertura do contrato | 4 · PDF §3 |
| `06-contrato-de-api.md` | a API Python: função, tipos e estados de resposta | 5 |
| `07-sequencia-e-degradacao.md` | a sequência de uma chamada e o que acontece quando algo falha | 3 |
| `08-lgpd-seguranca-e-operacao.md` | o que a lib faz e não faz com o dado que a atravessa; esteira e gate | 1 e 2 · colunas laterais |
| `09-codigos-decisoes-e-intencao.md` | o registro canônico de decisões, pendências, lacunas e riscos | todas · PDF §4, §5, §6 |

## 🔴 Quatro desvios de unidade, declarados

O template foi desenhado para uma **solução de dados** — camadas bronze a gold, catálogos,
tabelas, ponte de chaves entre sistemas. A `nlp-engine` é uma **biblioteca**: não cria tabela, não
lê catálogo, não persiste. Quatro arquivos mantêm **as colunas idênticas** — o que preserva o
parsing determinístico — e mudam a **unidade**:

| arquivo | unidade no template | unidade aqui |
|---|---|---|
| `02` | camada de dado (bronze → gold) | **etapa de decisão** |
| `03` | objeto de dado (`catalogo.schema.tabela`) | **unidade de configuração** (`nlp.findings.<x>`) |
| `05` | ponte de chaves entre sistemas | **cascata de decisão**, com denominador |
| `08` | classificação por coluna e grants | **o que a lib não faz** com o dado que a atravessa |

## Tabela de fontes

| documento | tipo | versao | data | responsavel | o_que_sustenta |
|---|---|---|---|---|---|
| `RELEASE.md` | registro de decisão | 0.13.0 | 2026-09-18 | dono da lib | `09` inteiro; o histórico de cada comportamento |
| `src/.../contracts.py` | código | 0.13.0 | 2026-09-18 | dono da lib | `06` — os três `TypedDict` e seus campos |
| `tests/api_surface.json` | gerado, travado por gate | 0.13.0 | 2026-09-18 | automático | `06` — os 35 módulos da superfície pública |
| `docs/REFERENCIA-API.md` | gerado dos docstrings, travado | 0.13.0 | 2026-09-18 | automático | `06` — assinaturas e contratos de função |
| `docs/REFERENCIA-PARAMETROS.md` | referência | >= 0.13.0 | 2026-09-18 | dono da lib | `03` — as chaves de configuração lidas |
| `docs/spec-0.10.0` … `spec-0.13.0` | SPEC | por versão | 2026-07 a 09 | dono da lib | `04` e `07` — casos de borda e o que não faz |
| `docs/adr/0001`, `0002` | ADR · **proposto** | — | 2026-09-18 | aguarda aval | `09` — como **pendência**, não como decisão |
| `azure-pipelines.yml` · `scripts/check_release.py` | esteira | — | — | dono da lib | `08` — publicação por ambiente e gate |
| `Makefile` · `.pre-commit-config.yaml` · `CONTRIBUTING.md` | gate | — | — | dono da lib | `08` — os sete alvos e os dois estágios de hook |
| `tests/chaves_observadas_em_run_real.json` | evidência de produção | — | 2026-09 | — | `05` — 21 chaves observadas em 1.500 blobs |
| `scripts/golden_300.py` | harness | 0.13.0 | 2026-09-18 | dono da lib | `05` — não-regressão, 2.920 linhas byte a byte |
| `scripts/medir_custo_por_camada.py` | harness | 0.13.0 | 2026-09-18 | dono da lib | `07` — `RNFD-01` a `RNFD-06` |

ℹ️ **Sem versão própria:** `COMO-USAR.md`, `COMO-CALIBRAR.md`, `GUIA-ORDINAL.md` e
`CONTRIBUTING.md` acompanham a versão da lib por convenção, não por declaração.

## O que não foi possível apurar

| o_que_falta | por_que | codigo | quem_destrava |
|---|---|---|---|
| Latência com o modelo real de embeddings | o modelo vive num Volume do Databricks; localmente roda `token_overlap` | `L1` | run no ambiente |
| Latência do juiz com rede | o harness estuba o cliente HTTP, de propósito | `L2` | run no ambiente |
| Prova do caminho `livre` da esteira | só o caminho `publicada` foi exercitado | `L3` | próximo bump |
| Documentação própria do `monitoring/` | nunca foi escrita; o subpacote é consumido por 8 arquivos da plataforma | `L4` | dono da lib |
| O contrato de dados de saída | fora do escopo por decisão: este pacote descreve a **API Python** | `BL-D-01` | PR 7228 da plataforma |

## Cobertura — o pacote está pronto quando

| prancha / seção | precisa de | arquivo dono |
|---|---|---|
| 1 · Blueprint | etapas, escopo negativo, o que a lib não faz com o dado, esteira, D/L/P | 01 · 02 · 08 · 09 |
| 2 · Fluxo de dados | unidades de configuração com semáforo, tratamento de texto, tipos | 02 · 03 · 04 |
| 3 · Sequência | atores, mensagens numeradas, degradação, RNFD | 07 |
| 4 · Dados do domínio | régua clínica, cascata com denominador, cobertura, lacunas, regras | 03 · 04 · 05 |
| 5 · Endpoints | função pública, tipos, estados de resposta | 06 |
| PDF · o que não funciona | `BL` e `L` com causa e destravamento | 05 · 09 |
| PDF · decisões do PO | `P` com opções e efeito | 09 |
| PDF · riscos | `R` em linguagem de consequência | 04 · 09 |
| PDF · intenção | decidido / por quê / descartado / o que quebra | 09 |

## ⚠️ Duas condições de uso deste pacote

1. **O pacote é vista; o repositório é a fonte.** `REFERENCIA-API.md` e `api_surface.json` são
   **gerados e travados por gate**. Onde este pacote os resume, ele **linka** — não os substitui.
   Divergência entre pacote e repositório se resolve sempre a favor do repositório.
2. **O contrato está em revisão.** `versao_contrato: 0.13.0` descreve o que existe hoje, e há
   mudança mapeada e não implementada para a `0.14.0` — ver `P3`. A prancha nasce sabendo qual
   parte vai mudar.
