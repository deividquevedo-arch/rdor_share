# Índice de cards — número → título

> **Para que serve.** A regra é citar card sempre com **número e título** (`.claude/rules/00-global-project-rules.md`). Este é o índice que a torna aplicável.
>
> **Fonte:** board `RedeDor-corp`, lido em **14/09/2026**. O prefixo `Fabrica IA/NLP Engine - ` foi removido dos títulos por ser ruído.
>
> ⚠️ **Título é o do board, verbatim.** *Estado* e *dono* envelhecem — o retrato vive em `ESTADO.md`. Para atualizar: `.claude/queries/cards.json`.

## nlp-engine — a lib

| card | tipo | título | estado | dono |
|---|---|---|---|---|
| `283644` | Defect | [NLP Engine] Juiz LLM ligado por contorno nao documentado em hepatologia e transplante_pulmao (producao) | Em Execução | Deivid Lejes Quevedo |
| `283647` | Task | [NLP Engine] Declarar contrato de entrada e saida entre a lib e a plataforma | Em Execução | Deivid Lejes Quevedo |
| `283648` | Task | [P0-29] Impedir que o juiz LLM promova sem evidencia de regra | Em Execução | Deivid Lejes Quevedo |
| `285305` | Defect | [NLP Engine] TI-RADS entrega TR falso: legenda ACR nao filtrada e medida associada ao nodulo errado | Em Execução | Deivid Lejes Quevedo |
| `298598` | Feature | [NLP Engine] Plano de bumps do backlog tecnico - 0.11.1 a 0.15.0 | Em Execução | Deivid Lejes Quevedo |
| `298600` | Defect | [NLP Engine] embedding_model aponta para volume de HML do workspace antigo nos tres ambientes | Em Execução | Deivid Lejes Quevedo |
| `300200` | Defect | [NLP Engine] Ancora ausente sai do gate: TR4 entregue sem conferir o tamanho | Desenvolvido | Deivid Lejes Quevedo |
| `300202` | Defect | [NLP Engine] Hepatologia descarta 86% dos laudos na segmentacao (mode: auto) | Novo | — |
| `301938` | Story | [NLP Engine] Fixar versao da nlp_engine por linha em producao - pin inicial 0.12.3 | Em Refinamento | João Marcelo da Silva Ferreira |

## nlp-engine — higiene técnica (`0.12.x` e `0.13.0`)

| card | tipo | título | estado | dono |
|---|---|---|---|---|
| `253573` | Story | [P2-07] Resolver o extra databricks vazio e validar o wheel no CI | Desenvolvido | Deivid Lejes Quevedo |
| `253574` | Story | [P2-08] Uniformizar a convenção de tipos de config e eliminar type: ignore | Em Execução | Deivid Lejes Quevedo |
| `253575` | Story | [P2-09] Conectar os TypedDict de contracts.py ao caminho principal do engine | Desenvolvido | Deivid Lejes Quevedo |
| `253576` | Story | [P2-10] Criar hierarquia de exceções própria da biblioteca | Desenvolvido | Deivid Lejes Quevedo |
| `253577` | Story | [P2-11] Eliminar except Exception silenciosos e adotar StructuredLogger | Desenvolvido | Deivid Lejes Quevedo |
| `253578` | Story | [P2-12] Ampliar regras do ruff (B, BLE, complexidade) e endurecer mypy | Desenvolvido | Deivid Lejes Quevedo |
| `253579` | Story | [P2-13] Consolidar o singleton do spaCy e torná-lo thread-safe | Em Execução | Deivid Lejes Quevedo |
| `253580` | Story | [P2-14] Quebrar ClinicalNlpEngine.process() em métodos coesos | Planejado | Deivid Lejes Quevedo |
| `253581` | Story | [P2-15] Dividir rads_extraction.py por responsabilidade | Em Execução | Deivid Lejes Quevedo |
| `253582` | Story | [P2-16] Eliminar duplicação de helpers utilitários entre módulos | Planejado | Deivid Lejes Quevedo |
| `253583` | Story | [P2-17] Blindar SQL montado por f-string em monitoring/ | Desenvolvido | Deivid Lejes Quevedo |
| `253584` | Story | [P2-18] Remover (ou documentar) a dependência de id(mention) como chave de exclusão | Encerrado | Deivid Lejes Quevedo |
| `253585` | Story | [P2-19] Declarar __all__ explícito nos módulos-folha | Desenvolvido | Deivid Lejes Quevedo |
| `253586` | Story | [P2-20] Popular tests/conftest.py com fixtures compartilhadas | Em Execução | Deivid Lejes Quevedo |
| `253587` | Story | [P2-21] Adotar @pytest.mark.parametrize de forma ampla na suíte | Em Execução | Deivid Lejes Quevedo |
| `253588` | Story | [P2-22] Instituir medição de cobertura de testes com gate de PR | Desenvolvido | Deivid Lejes Quevedo |
| `253589` | Story | [P2-23] Gerar referência de API a partir dos docstrings e padronizar o estilo | Em Execução | Deivid Lejes Quevedo |
| `253590` | Story | [P2-24] Criar CONTRIBUTING.md e declarar política de versionamento explícita | Em Execução | Deivid Lejes Quevedo |
| `253591` | Story | [P3-25] Avaliar e (se aprovado) achatar a estrutura de pacote duplamente aninhada | Novo | Deivid Lejes Quevedo |
| `253592` | Story | [P3-26] Separar documentação para humanos da meta-documentação para agentes | Em Execução | Deivid Lejes Quevedo |
| `253593` | Story | [P3-27] Avaliar Protocol/dataclass nas fronteiras de API pública | Novo | Deivid Lejes Quevedo |
| `253594` | Story | [P3-28] Adotar hooks de pre-commit espelhando o make check | Em Execução | Deivid Lejes Quevedo |

## Plataforma / MLOps

| card | tipo | título | estado | dono |
|---|---|---|---|---|
| `283645` | Task | [MLOps] Definir e publicar o fluxo de branch e publicacao da lib entre DS e plataforma | Novo | Diego Giuseppe Marcello |
| `298596` | Defect | [Plataforma NLP] limit_rows nao limita a fila: o corte vem depois da uniao e os reprocessados ficam por ultimo | Em Execução | Diego Giuseppe Marcello |
| `299238` | Defect | [Plataforma NLP] SPEC 27 contradiz o codigo: ajustes acumulados para alinhamento | Novo | — |
| `300201` | Defect | [Plataforma NLP] Texto de entrada duplicado 2n+1 vezes antes do motor | Novo | — |

## Especialidades e processo

| card | tipo | título | estado | dono |
|---|---|---|---|---|
| `246669` | Story | [Transplante de Pulmão] Deploy Algoritmo Transplante de Pulmão (deve adiar para 18.08) | Encerrado | João Marcelo da Silva Ferreira |
| `252955` | Story | [Tireoide] Desenvolvimento da V3 com exames LAB | Cancelado | Leandro Neri da Silva |
| `280008` | Story | [Motor NLP] Estudo e planejamento - contexto do paciente na decisao | Novo | Deivid Lejes Quevedo |
| `283567` | Task | Criar arquivo de configuração Ca de Estomago | Encerrado | Deivid Lejes Quevedo |
| `283646` | Task | [Fabrica IA] Validacao de compliance/DPO do envio de laudo clinico ao LLM | Em Execução | Deivid Lejes Quevedo |
| `299111` | Story | [Migração][Reumato] Migrar algoritmo para nova estrutura Fabrica IA + NLP Engine | Em Execução | Deivid Lejes Quevedo |

## Encerrados

| card | tipo | título | estado | dono |
|---|---|---|---|---|
| `282904` | Defect | [NLP Engine] Problema no arquivo de configuração TIRADS | Encerrado | Deivid Lejes Quevedo |
| `298597` | Defect | [NLP Engine] 0.11.1 - camada semantica deixa de promover trecho negado | Encerrado | — |
| `299423` | Defect | [NLP Engine] Texto tratado cola palavra antes de acento e apaga a negacao: 6 falsos positivos em producao | Encerrado | — |

---

## Divergências entre este índice e o `ESTADO.md`

Encontradas ao puxar os títulos reais em 14/09. **O board é a fonte.**

| card | o `ESTADO.md` dizia | o board diz |
|---|---|---|
| `253582` | *"clamp — pode alterar valor"* | **[P2-16] Eliminar duplicação de helpers utilitários entre módulos** |
| `253591` e `253593` | *"ADRs"* | **[P3-25] achatar a estrutura de pacote duplamente aninhada** e **[P3-27] Protocol/dataclass nas fronteiras de API pública** |
| `301938` | atribuída ao Diego | atribuída ao **João Marcelo**, em *Em Refinamento* |
| os 15 da `0.12.x` | *"em Pronto para QA"* | **9 em `Desenvolvido`, 6 em `Em Execução`, 1 `Encerrado`** |

ℹ️ **`253591` toca o POP-IA-04.** O card pede avaliar o achatamento da estrutura de pacote; o POP declara layout flat, sem `src/`. É o mesmo assunto por dois caminhos — o card existe desde antes e já é nosso.

