# PR 7275 — o que ajustar antes de pedir nova revisão

> Ateromatose coronariana · 15/09/2026

O PR está bom. A régua está traduzida, a evidência é sólida e as divergências estão abertas uma a
uma — é o material mais completo que passou por aqui.

**O que já está certo e não precisa mexer:** o bloco `data` completo · os blocos `catalog`,
`monitoring`, `distribution` e `data.legacy` ausentes · `segmentation: full_doc` · `findings` no formato v3 por-entidade ·
navegação com 22 colunas nos três ambientes · o job nascendo `PAUSED`.

Abaixo, o que ajustar.

---

# 1. Primeiro: sincronize com a `hml`

**É o ponto principal, e vem antes de qualquer edição.**

A branch parte de **09/09**. Desde então a `hml` avançou em mais de 38 arquivos — a última entrada é
de **15/09 às 21:28**.

```bash
git fetch origin
git switch ateromatose_coronariana/feature/migracao-plataforma
git merge origin/hml        # ou rebase, como preferir
```

**Por que antes de tudo:**

- **evita conflito** — várias frentes tocaram os mesmos diretórios nesta semana;
- **atualiza o seu local** — o teste de envio rodou sobre uma base em que a view **ainda não
  decifrava** PII, e isso explica o que você reportou;
- **traz o padrão para a árvore** — reumatologia e TI-RADS já estão no formato final, e as seis
  definições de job já estão pinadas. Depois do sync é copiar, não inventar.

⚠️ **O sync não corrige os cinco itens abaixo.** Eles estão em arquivos que a sua branch criou, e o
merge não os toca.

---

# 2. Cinco edições

⚠️ **As 2.2 e 2.3 são dependentes** — ver a nota no fim da 2.2.

## 2.1 Pinar a versão do motor

`jobs/definicoes/ateromatose-coronariana-batch.json`

```diff
- "nlp_engine_version": "${nlp_engine_version}"
+ "nlp_engine_version": "0.12.3"
```

As outras **seis definições já usam o literal**, em `main` e em `hml`. Sua paridade foi medida com a
`0.12.3`; sem pin, a próxima versão publicada entra sem que a medição se aplique.

## 2.2 Remover o bloco `runtime`

`plataform/config/speciality/ntb_ia_ateromatose_coronariana_config.py`

```diff
- 'runtime': {'profile': 'rule_only',
-             'llm_router': {'enabled': False, ...}},
```

O bloco existia para sobreposição via widget durante testes. **Nenhum dos 15 widgets do runner o
alimenta hoje** — perdeu a função, e sai de todas as configs.

🔴 **E aqui remover deixou de ser opcional.** O `runtime` **sobrepõe** o `nlp` no carregamento
(`ntb_ia_loader.py:104-113`), e é o resultado que o motor lê. Se você ligar
`nlp.llm_router.enabled: True` (item 2.3) e **deixar o `runtime` com `False`**, o runtime vence e
**o juiz continua desligado** — sem erro e sem log, com o run passando normalmente.

**Os itens 2.2 e 2.3 precisam ir no mesmo commit.**

## 2.3 Ligar embeddings e o juiz — e conferir que ligaram de fato

Decisão: a linha passa ao perfil completo. São **três** mudanças, e as três precisam andar juntas.

### Na config

```diff
  'embeddings': {
-     'use_embeddings': False,
+     'use_embeddings': True,
      'decision_mode': 'hybrid',
```

```diff
  'llm_router': {
-     'enabled': False,
+     'enabled': True,
```

### No JSON do job

```diff
- "embedding_enable": "false"
+ "embedding_enable": "true"
```

🔴 **Este é o passo que passa despercebido.** O widget decide se a wheel instala o extra
`[embeddings]`. Sem ele, `use_embeddings: True` na config **não instala a biblioteca** — e o motor
cai em `token_overlap` **sem erro e sem log**. O perfil medido não seria híbrido.

**Isso não é hipótese.** Medido em dev, TI-RADS, últimos 30 dias: **4.543 de 17.896 laudos (25,4%)
com `FALLBACK:ModuleNotFoundError`** — a camada semântica rodou em 100% dos laudos e degradou em um
quarto deles, por falta do pacote.

### Como validar que ligou de verdade

Depois do run em dev, sobre a tabela de saída:

```sql
SELECT count(*) laudos,
       sum(CASE WHEN get_json_object(exm_laudo_resultado,'$.semantic_score') IS NOT NULL
                THEN 1 ELSE 0 END) camada_rodou,
       sum(CASE WHEN exm_laudo_resultado LIKE '%FALLBACK%' THEN 1 ELSE 0 END) degradou,
       sum(CASE WHEN get_json_object(exm_laudo_resultado,'$.llm_called')='true'
                THEN 1 ELSE 0 END) juiz_chamado
FROM diamond_fabrica_ia_dev.ateromatose_coronariana.tb_mod_diamond_ateromatose_coronariana_saida_v0
WHERE dt_execucao_modelo >= current_date()-1
```

**O aceite:** `camada_rodou` = `laudos`, `degradou` = 0, e `juiz_chamado` > 0.

⚠️ **Se `juiz_chamado` vier zero**, o juiz está habilitado e nunca é acionado — o score não alcança
a banda de incerteza. Acontece: no transplante de pulmão o juiz está ligado e teve **zero chamadas**
em 312 laudos hoje; no câncer de estômago, 145 laudos ficaram abaixo do piso da banda. Se der zero,
é a `uncertainty_band` que precisa ser calibrada, não o `enabled`.

### Um ponto sobre o caminho do modelo

```python
'embedding_model': '/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/st_models/...'
```

É o volume do **workspace antigo**. **Em dev ele resolve** — zero `FileNotFoundError` em 109.563
laudos das três linhas. **Em produção não existe**, e as quatro linhas que declaram embeddings
falham lá em 86% a 100%.

✅ **Isso está com o time de plataforma** (card `305810`) e não bloqueia este PR: valide em dev,
suba, e acompanhe a correção.
⚠️ Mas **saiba que a validação em dev não diz nada sobre produção** neste ponto específico — são
dois defeitos diferentes, `ModuleNotFoundError` em dev e `FileNotFoundError` em prd.

### E uma consequência para a paridade

A medição de `match_rate` foi feita em `rule_only`. Ligando o semântico e o juiz, **o perfil deixa
de ser o que foi medido** — o semântico alarga o recall e o juiz estreita. A paridade precisa ser
remedida no perfil novo antes de sustentar o merge.

## 2.4 Remover `descriptografia` dos três arquivos de navegação

O padrão mudou: a **view** decifra, via `security.prd.rdsl_decrypt` na projeção da `CREATE VIEW`. O
builder do Excel não descriptografa mais nada, e não há o que declarar na config.

Documentado em `boas-praticas/10` §2.5 e spec 22 §4.3. Depois do sync, **reumatologia e TI-RADS
estão na árvore como exemplo**.

ℹ️ Você chegou a removê-lo (`4f6edf4`) e reintroduziu (`b5d1705`) — correto para a base de 09/09.
Agora não é mais.

## 2.5 Corrigir o e-mail de produção

`plataform/config/exchange/prd/ntb_ia_ateromatose_coronariana_navegacao.py`

Hoje `prd` tem `send_email: True` e **exatamente os mesmos destinatários de `hml`** — sem o negócio.

🔴 **Se o job for despausado antes de a lista existir**, o arquivo é gerado, enviado com sucesso, e
o negócio **não recebe** — sem erro, com `sent=True` e `HTTP 202`. Ninguém descobre até alguém
reclamar.

**Escolha uma:**

- incluir os destinatários do negócio no `emailTo`, e o acompanhamento técnico no `emailCc` — é o
  padrão de reumatologia e TI-RADS; **ou**
- `send_email: False` até que a lista exista.

✅ `dev` e `hml` estão corretos: dev só você, hml com acompanhamento.

---

# 3. Refaça o teste de envio

Sobre a base já sincronizada. Você reportou que as cinco colunas de identificação saíram
**cifradas** e atribuiu à falta de `USE CATALOG` no catálogo `security`.

Na base de 09/09 quem decifrava era o builder; na `hml` atual é a **view**. O diagnóstico pode ser
outro — vale medir de novo antes de manter a atribuição no PR.

---

# 4. Depois disso

Peça nova revisão. Com o sync feito e os cinco itens ajustados, a segunda passada é curta.

**Fica para depois do merge, sem bloquear:**

- as **11 críticas** classificadas como defeito desta versão, na `0.1.1`;
- o alinhamento da janela entre `dt_exame` e `dataExecucaoModelo`, que hoje limita o `A2` a 26,5%;
- unidades e destinatários de produção;
- o schema `ateromatose_coronariana` em `hml`.

⚠️ E um número para a conversa de **despausar o job**, não para esta: a precisão contra o gabarito
dos 44 é **0,556**. O critério deste merge é paridade, e isso está correto — mas o job só deve ser
ligado com esse número na mesa.
