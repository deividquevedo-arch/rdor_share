---
tags: [biopsias, motor, regras-estruturadas, proposta, arquitetura]
---

# Motor de regras estruturadas — proposta

> v0 · 10/11/2026 · direção para industrializar linhas de captação cuja relevância vem de **dado estruturado** (AP,
> citologia) e não de leitura de laudo. Contexto: [[plano-industrializacao-biopsias-v2]]. Linhas medidas:
> [[biopsias-outras-linhas-volumetria-v1]]. Não é SPEC — é a proposta que a SPEC v0 vai detalhar.

## 1. Por que um motor

Cinco linhas já têm o mesmo formato: **cólon, estômago, próstata e rim** pelo AP com categoria do laboratório, e
**tireoide** pela citologia com Bethesda no texto. Uma consulta SQL escrita à mão por linha repete o problema que o
motor NLP resolveu: régua dentro do código, sem versão, sem trilha e sem reuso.

## 2. As operações (e nada além delas)

| operação | o que faz | exemplo |
|---|---|---|
| **selecionar** | fonte e tipo de exame | AP do Motion · citopatológico |
| **qualificar** | filtro por campo estruturado | categoria = `neoplasia maligna primária` |
| **extrair** | categoria por regex num texto | Bethesda V/VI da conclusão da citologia |
| **órgão** | termos com fronteira explícita, exclusões, "excluir só se for o único" | cólon/reto; apêndice só sozinho |
| **classificar amostra** | regras ordenadas (primeira que casa) + campo de reserva | cirurgia × endoscopia × biópsia; macroscopia quando o material não decide |
| **decidir envio** | expressão sobre o classificado | `amostra <> 'cirurgica'` |

Garantias de toda linha: **dedup por pedido** (laudo reemitido) e **janela por emissão do laudo** com retroação.

## 3. Config: linha nova é um dicionário

```python
CONFIG = {
  'linha': 'biopsia_cancer_colon', 'config_version': '0.1.0',
  'fonte': {'tipo': 'motion_ap'},
  'qualificar': {'campo': 'categoria', 'em': ['neoplasia maligna primária']},
  'orgao': {'incluir': ['colon', 'reto'], 'excluir_se_unico': ['apendice']},
  'amostra': {'regras': 'padrao_cirurgia_endoscopia', 'reserva': 'macroscopia'},
  'envio': "amostra <> 'cirurgica'",
  'saida': {'achado': 'categoria', 'laudo': ['material', 'macroscopia', 'microscopia', 'conclusao']},
}
```

Tireoide muda só o que é diferente:

```python
  'fonte': {'tipo': 'motion_citologia'},
  'extrair': {'campo': 'conclusao', 'sistema': 'bethesda'},
  'qualificar': {'campo': 'bethesda', 'em': ['V', 'VI']},
```

**Vocabulário compartilhado** — o equivalente do `ORGANS_SHARED`: termos de órgão e de procedimento (`-ectomia`,
polipectomia, mucosectomia, LST, margem de grampeamento…) escritos e testados uma vez. Os ajustes validados no cólon
passam a valer para próstata e estômago sem custo.

## 4. Por dentro

- **Gera SQL a partir da config** — uma CTE por operação, executada no Spark sobre o conjunto. O SQL gerado pode ser
  impresso e auditado.
- **Funil por etapa** — quantos pedidos cada operação removeu (o equivalente do `decision_trail`): responde *"por que
  este paciente não veio?"* e alimenta o monitoramento.
- **Explicação por linha** — qual termo identificou o órgão e qual regra classificou a amostra.
- **Saída no contrato da plataforma**, com `engine_version` e `config_version`; view, randomização e envio seguem como
  estão.

## 5. Onde mora

| peça | lugar | alçada |
|---|---|---|
| runner do motor | **2º runner da plataforma NLP**, gravando em `diamond_fabrica_ia.<linha>` | plataforma (código) |
| config por linha | `plataform/config/speciality/`, como as linhas atuais | Ciência de Dados (POP-IA-08) |
| vocabulário compartilhado | ao lado do `ORGANS_SHARED` | Ciência de Dados |
| fonte do Motion | Datahub, cadeia de camadas (POP-IA-07) | Datahub |

A escolha depende de alinhamento — é a demanda à plataforma prevista no POP-IA-08 §14.

## 6. Construção sem excesso de engenharia

1. **SPEC v0** desenhada para **duas linhas de formatos diferentes** — cólon (categoria) e tireoide (extração).
2. **Implementação mínima com paridade 100%** com a consulta v3 na janela homologada; os 118 vereditos são o teste de
   não-regressão.
3. **Prova de genericidade**: estômago e próstata entram **só com config**. Se exigir código, o desenho volta à SPEC.
4. Industrialização pelas fases do [[plano-industrializacao-biopsias-v2|plano]].

**Fora, de propósito:** critérios numéricos de exame laboratorial (ex.: creatinina da nefro) — mesma família, entram
quando uma linha precisar.

## 7. Decisões

1. Motor de regras estruturadas como 2º runner da plataforma, em vez de consulta própria por linha?
2. Donos: código do motor · vocabulário · fonte.
3. Primeiro corte com cólon e tireoide; estômago e próstata como prova de genericidade?
