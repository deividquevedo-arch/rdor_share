# Proposta — padrão de versionamento e rastreabilidade de config

**Data:** 2026-08-07 · **Para:** time da Fábrica de IA (MLOps + Ciência de Dados)
**Complementa:** *Guia de Convenções e Fluxo de Trabalho v1.0*

> O guia padronizou catálogo, schema, tabela e branch. Falta o elo que amarra **o que rodou** ao
> **código que o gerou**. Esta proposta cobre essa lacuna, com base em problemas que já nos
> custaram retrabalho.

---

## 1. O problema, em casos reais

**`engine_version` não é confiável.** O runner instala a wheel e importa em seguida, sem
`restartPython`. Em cluster reaproveitado o módulo antigo continua em memória, mas
`engine_version` lê o metadata em disco — **a coluna diz que atualizou quando não atualizou**.
Aconteceu conosco: `0.7.2` reportada, código `0.7.1` executando.

**No runner legado a coluna é de outra biblioteca.** `engine_version` traz a versão da
`fabrica_ia` (`0.5.8`), não a do `nlp_engine`. Quem não sabe disso lê o número errado.

**Sobrou só o `config_version`.** Foi ele que permitiu descobrir que um lote de câncer de estômago
tinha rodado a régua `0.1.11` e não a `0.1.13` que supúnhamos — 6 horas de processamento que
teriam sido interpretadas erradas.

**Hoje os formatos divergem:**

```
0.1.0-tirads-rads-v22.11-v2     (tirads)
0.1.2-pulmao-failover           (transplante_pulmao — usa "pulmao", que não é o schema)
0.1.14-hep-v3                   (hepatologia — "hep" é abreviação, que o guia proíbe)
```

E **o repositório da plataforma não tem nenhuma tag**, então não há como amarrar um
`config_version` que rodou em produção ao commit que o produziu.

---

## 2. Proposta

### 2.1 `config_version` — o contrato

Formato: **`<semver>-<schema>[-<rótulo>]`**

- **`<schema>`** é o nome oficial do projeto no Unity Catalog (guia §6.2) — **sem abreviação**.
  Corrige `pulmao` → `transplante_pulmao` e `hep` → `hepatologia`.
- **`<rótulo>`** é opcional, curto, em minúsculas com hífen. Descreve *o que* mudou, não *quando*.

Semântica do semver, ancorada no **impacto sobre a métrica**:

| incremento | quando | consequência |
|---|---|---|
| **MAJOR** | a régua clínica muda (novo critério de relevância, mudança de escopo) | métricas **não são comparáveis** com versões anteriores; a base ouro precisa ser revalidada |
| **MINOR** | novo achado, novo critério ou limiar, sem mudar a definição de relevância | métricas comparáveis, mas espera-se deslocamento |
| **PATCH** | correção que **não** altera decisão (typo, comentário, ajuste de seleção sem efeito medido) | métricas devem ser idênticas |

Essa é a parte que mais importa: o número precisa dizer se **pode comparar**. Já erramos ao
comparar régua nova contra gabarito antigo.

**Regra:** toda alteração em `plataform/config/speciality/` incrementa o `config_version`. Sem
exceção — é o único identificador confiável na saída.

### 2.2 Tag no Azure DevOps

Formato: **`<schema>/v<semver>`** — mesmo prefixo da branch (guia §4.1).

```
tirads/v0.2.0
transplante_pulmao/v0.1.2
hepatologia/v0.1.14
```

Criada **após o merge do PR em `hml`**, sobre o commit de merge. Amarra o `config_version` que
aparece na tabela ao código exato que o gerou.

### 2.3 Convenção de commit

Título: **`config(<schema>): <semver> — <o que muda>`**

E, no corpo, três linhas obrigatórias quando a régua é afetada:

```
Impacto na métrica: comparável | NÃO comparável (base ouro precisa revalidar)
Requer nlp_engine: >= X.Y.Z
Validado em: <ambiente> · <janela> · <n> laudos
```

O campo de impacto é o que evita a armadilha de comparar números de réguas diferentes.

### 2.4 Filtro na saída

A tabela de saída é **append**: todas as execuções coexistem. Hoje a view de export
(`vw_mod_diamond_<schema>_export_v0`) lê a tabela **sem `WHERE`** — no TI-RADS isso significa
10.843 linhas devolvidas para uma coorte de ~2.700 laudos, com o mesmo exame repetido uma vez por
execução.

Proposta: a view expõe **apenas a execução vigente**, por uma destas vias (a definir com MLOps):

- filtro explícito por `config_version` + `dt_execucao`; ou
- janela por `dt_execucao` mais recente por `id_exame`; ou
- uma tabela/coluna de controle que marque a execução publicada.

Independente da escolha, **quem consome não deveria precisar saber que existem várias execuções**.

---

## 3. O que isso resolve

| problema | como |
|---|---|
| não saber qual régua gerou um resultado | `config_version` único e obrigatório |
| não achar o código de uma versão que rodou | tag `<schema>/v<semver>` |
| comparar métricas de réguas diferentes | campo *Impacto na métrica* no commit |
| consumidor recebendo execuções empilhadas | filtro na view |
| abreviações fora do padrão do guia | schema oficial no `config_version` |

---

## 4. Migração sugerida

Aplicar no próximo incremento de cada especialidade, sem renomear retroativamente (o histórico já
gravado continua válido e legível):

```
0.1.0-tirads-rads-v22.11-v2  ->  0.2.0-tirads
0.1.2-pulmao-failover        ->  0.2.0-transplante_pulmao
0.1.14-hep-v3                ->  0.2.0-hepatologia
```

E criar as tags correspondentes no merge seguinte.

---

## 5. Duas perguntas ao time

1. **A view deve filtrar, ou o consumidor filtra?** Se for o consumidor, precisa estar documentado
   no guia — hoje não está, e a leitura ingênua duplica dados.
2. **Onde documentar a versão vigente de cada especialidade?** Uma tabela de controle no schema do
   projeto resolveria, e conversa com o `_gt` já previsto no guia §7.2.
