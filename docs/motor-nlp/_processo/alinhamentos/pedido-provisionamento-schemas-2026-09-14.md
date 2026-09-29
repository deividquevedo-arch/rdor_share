# Pedido de provisionamento de schemas — 14/09/2026

> Texto pronto para envio ao time da Fábrica. Criar schema não é da nossa alçada
> (POP-IA-08, quadro *Administrador Databricks*), por isso vai por pedido e não por PR.

---

**Assunto:** Provisionamento de schemas — 2 linhas novas + 1 pendência que bloqueia produção

---

Precisamos de três provisionamentos. Os dois primeiros são para migrações que entram esta semana; o
terceiro está pendente desde 08/09 e mantém um defeito ativo em produção.

## 1. `doencas_biliares` — linhas novas

Schema `doencas_biliares` nos três catálogos:

- `diamond_fabrica_ia_dev`
- `diamond_fabrica_ia_hml`
- `diamond_fabrica_ia`

## 2. `neuroimunologia` — linha nova

Schema `neuroimunologia` nos mesmos três catálogos.

**Contexto de 1 e 2.** As duas linhas saem do algoritmo legado para a plataforma nova, com entrega
prevista para 18/09. Seguem a convenção já usada: schema = nome do projeto = prefixo da branch.

⚠️ **Pedimos os três ambientes de uma vez, de propósito.** Na migração da reumatologia o schema
existia só em `dev`, e a ausência em produção virou bloqueio na hora de promover — depois de todo o
trabalho de validação já feito. Provisionar agora custa o mesmo e evita repetir isso.

## 3. `nlp_engine` em `gold_fabrica_ia` — 🔴 bloqueia correção de defeito ativo

Schema `nlp_engine` no catálogo **`gold_fabrica_ia`** (produção). Hoje esse catálogo tem apenas
`fhir` e `information_schema`.

**Por que é urgente.** Sem esse destino, o modelo de embeddings não tem caminho válido em produção,
e as três linhas que declaram embeddings rodam em modo degradado. Medido em 10/09, com o erro
registrado laudo a laudo:

| linha | laudos | com `FileNotFoundError` |
|---|---|---|
| hepatologia | 5.172 | **5.108 (98,8%)** |
| cancer_estomago | 201 | **201 (100%)** |
| tirads | 1.568 | **1.348 (86,0%)** |

A configuração declara `decision_mode: hybrid`; o que executa é régua mais sobreposição de tokens.
**Produção roda um perfil que nunca foi homologado**, e a monitoria não sinaliza porque a régua
sustenta a taxa de entrega.

O modelo já está copiado no equivalente de homologação
(`gold_fabrica_ia_hml/nlp_engine/nlp_engine_lib/st_models/`); falta o destino de produção.

Registrado no card `298600` — *[NLP Engine] embedding_model aponta para volume de HML do workspace
antigo nos três ambientes*.

---

**Prazo que nos serve:** os itens 1 e 2 até **16/09**, para caberem na janela de validação em dev
antes da entrega de sexta. O item 3 é o mais antigo dos três e não tem data acordada.

Qualquer informação adicional que o processo exija — nomes de tabela, grants, retenção —, é só
dizer o formato.
