# Linha de Evolucao — xxRADS no NLP Engine

## Status

Proposta de evolucao do `nlp_engine` apos a linha V0 publicada em Databricks (`0.2.0`).

Esta nota nao altera o escopo da V0: a V0 continua sendo o motor publicado com TextPipeline, rule engine, scoring, contrato de saida e LLM router conforme configurado no ambiente. A capacidade xxRADS deve ser tratada como extensao dirigida por YAML, sem hardcode clinico no Python.

## Objetivo

Adicionar ao `nlp_engine` uma capacidade generica para extrair classificacoes estruturadas do tipo xxRADS em laudos clinicos, por exemplo BI-RADS, LI-RADS, PI-RADS ou outras familias equivalentes.

O motor deve extrair, normalizar e auditar a classificacao encontrada. A decisao de negocio ou clinica associada a uma categoria xxRADS deve continuar sendo politica de config.

## Principios

- A taxonomia xxRADS vive no YAML, nao no codigo.
- A lib recebe `dict`; nao le arquivo diretamente.
- O extractor nao importa `data_manage`, `monitoring` ou notebook.
- O contrato atual de saida deve ser preservado.
- Qualquer promocao de `fl_relevante` por categoria xxRADS deve ser explicita na config.
- Testes usam frases sinteticas, sem PHI.

## Notas por versao

| Versao | Nota |
|--------|------|
| **V0 (0.2.0)** | Sem escopo xxRADS no contrato minimo publicado |
| **V0 (0.2.0)** | Motor atual permanece fonte de `fl_relevante` + `confidence_score` |
| **V1** | Feature flag `rads_extraction.enabled` no YAML |
| **V1** | Extractor generico de classificacoes RADS |
| **V1** | YAML define sistemas suportados: BI-RADS, LI-RADS, PI-RADS etc. |
| **V1** | YAML define aliases, categorias validas, padroes e normalizacao |
| **V1** | Saida auditavel em `exm_laudo_resultado.rads_mentions` |
| **V1** | Politica YAML para categoria xxRADS promover ou nao `fl_relevante` |
| **V1** | Testes sinteticos por sistema RADS e casos ambiguos |
| **V1** | LLM como fallback somente quando regex/config nao resolver com confianca |
| **V2** | Metricas de cobertura, conflito e ambiguidade por sistema RADS |
| **V2** | Validacao clinica e workflow de aprovacao por taxonomia |
| **V2** | Evolucao multi-especialidade com contrato estavel e gates de paridade |

## Bloco YAML proposto

Exemplo conceitual; os nomes finais devem ser fechados em SPEC antes de implementar.

```yaml
rads_extraction:
  enabled: true
  systems:
    li_rads:
      aliases: ["LI-RADS", "LIRADS", "LR"]
      categories: ["LR-1", "LR-2", "LR-3", "LR-4", "LR-5", "LR-M", "LR-TIV"]
      patterns:
        - "(?:LI[- ]?RADS|LIRADS|LR)[\\s:-]*(LR[- ]?[1-5]|LR-M|LR-TIV)"
      relevance_policy:
        promote_categories: ["LR-4", "LR-5", "LR-M", "LR-TIV"]
    bi_rads:
      aliases: ["BI-RADS", "BIRADS"]
      categories: ["0", "1", "2", "3", "4", "4A", "4B", "4C", "5", "6"]
      patterns:
        - "(?:BI[- ]?RADS|BIRADS)[\\s:-]*([0-6]|4A|4B|4C)"
```

## Payload proposto

A primeira entrega deve preferir payload auditavel dentro de `exm_laudo_resultado`, sem quebrar consumidores atuais.

```json
{
  "rads_mentions": [
    {
      "system": "li_rads",
      "category": "LR-5",
      "confidence": 0.95,
      "source": "regex",
      "matched_text": "LI-RADS LR-5"
    }
  ]
}
```

Campos materializados como `rads_system`, `rads_category`, `rads_confidence` e `rads_source` so devem ser adicionados se o serving ou analytics precisar consumir diretamente em tabela.

## SPEC minima antes de codigo

- Entrada: texto tratado pelo TextPipeline e `nlp_config` com `rads_extraction`.
- Saida: lista de mencoes normalizadas em `exm_laudo_resultado.rads_mentions`.
- Edge cases: multiplas mencoes no mesmo laudo, categoria invalida, alias sem categoria, conflito entre sistemas, negacao/contexto normal.
- Nao faz: leitura de YAML, decisao clinica hardcoded, dependencia de notebook, chamada LLM obrigatoria.
- Testes: regex positivo, categoria invalida, alias variante, multiplas mencoes, fallback LLM mockado, politica de promocao por config.

## Decisao aberta

Antes de virar backlog implementavel, o time precisa definir se xxRADS entra como:

- capacidade global do `nlp_engine` para qualquer especialidade;
- pacote especifico de uma especialidade inicial;
- ou apenas informacao auditavel sem efeito em `fl_relevante`.
