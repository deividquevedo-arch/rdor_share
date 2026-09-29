# Pedido de acesso — `USE CATALOG` em `mlops_fabrica_ia`

> 19/09/2026 · Solicitante: Ciência de Dados e IA · Bloqueia homologação a partir de 21/09

## O que se pede

`USE CATALOG` em **`mlops_fabrica_ia`**, mais `SELECT` (ou `EXECUTE`, conforme o tipo) no Model
`mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm`.

**Para duas identidades, e as duas são necessárias:**

| identidade | por quê |
|---|---|
| o grupo de Ciência de Dados e IA | valida em dev e homologa antes de promover |
| **a identidade de serviço que executa os jobs** `*-batch` | é ela que roda às 04:00; não herda acesso pessoal |

**Ambientes:** `dev`, `hml` e `prd`.

## Por que agora

O modelo de embeddings passou a ser referenciado como **Model do Unity Catalog**, em seis
configurações de especialidade. O caminho **funciona** — foi validado em 16/09 com a identidade de
quem registrou o Model, sobre 10.000 laudos, com a camada semântica ativa em 10.000 de 10.000.

Sem o grant, a execução falha na **carga da configuração**, antes de o motor iniciar:

```
[FALHA] config apos 32.9s: PERMISSION_DENIED:
User does not have USE CATALOG on Catalog 'mlops_fabrica_ia'
```

## É acesso, não endereço — verificado

O Databricks distingue as duas situações, e a mensagem confirma qual é:

| consulta | resposta |
|---|---|
| `mlops_fabrica_ia` | `User does not have USE CATALOG on Catalog 'mlops_fabrica_ia'` |
| catálogo inexistente (controle) | `Catalog '<nome>' does not exist` |

O catálogo existe no mesmo metastore que serve `diamond_fabrica_ia` (produção) e
`diamond_fabrica_ia_hml`.

## Impacto se não sair até 21/09

| ambiente | efeito |
|---|---|
| **`hml`** | 🔴 **quatro jobs falham às 04:00 de segunda** — `hepatologia`, `cancer_rim`, `tirads`, `cancer_estomago`. Falha **alta e visível**, sem entrega incorreta |
| `hml` — demais | 🟢 `ateromatose`, `cancer_colon`, `reumatologia`, `transplante_pulmao` seguem normais (embeddings desligados) |
| **produção** | 🟢 **não afetada** — as configurações de produção ainda não referenciam o Model |
| homologação da `0.13.0` | 🔴 **bloqueada** — a versão está publicada e aguardando validação em dev |

## Relacionado

Há um segundo pedido de acesso aberto, da mesma natureza e provavelmente do mesmo interlocutor:
**`USE CATALOG security` + `EXECUTE` em `security.prd.rdsl_decrypt`**, que bloqueia a view de
exportação em três frentes. Vale tratar os dois na mesma conversa.
