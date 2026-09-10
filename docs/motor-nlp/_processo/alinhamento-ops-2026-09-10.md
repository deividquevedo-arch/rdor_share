# Alinhamento com o time de Ops — pauta consolidada

> **Isto é uma PAUTA, não uma ata.** Reúne o que está pendente entre DS e Ops, com a evidência já
> medida, para que a agenda comece com a informação em mãos e termine em decisão. Depois da
> reunião vira ata em `_processo/atas/`, com o que **passou a valer**.
>
> Consolidado em **2026-09-10**. Fonte: `ESTADO.md` e as medições referenciadas em cada item.

---

## Como ler

Cada item traz **o que é**, **a evidência** (medida, não impressão), **o que se pede** e **quem
decide**. Itens marcados 🔴 **bloqueiam trabalho hoje**; 🟡 custam retrabalho ou risco; ℹ️ são
informativos e não precisam de decisão.

**Resumo:** 20 itens em 8 temas. **5 bloqueiam**, 4 pedem decisão de contrato, 7 são defeitos de
plataforma sem correção, 2 são provisionamento, 4 são PRs parados, 1 é de governança e 1 é uma
proposta de processo a aprovar.

🔴 **O item mais grave é o 3.0:** os embeddings não funcionam em produção, e as três linhas que os
declaram rodam um perfil que nunca foi homologado.

---

## Tema 1 — Publicação e versionamento da lib

### 1.1 🔴 Nenhuma versão publicada pela `hml` é instalável em dev

**O que é.** O cluster de dev resolve o `pip` contra o feed **`fabrica-ai`**, que é o de produção.
O feed `fabrica-ai-hml` existe, recebe as publicações da `hml`, e **não é lido por ambiente
nenhum**.

**Evidência.** Ao tentar rodar a `0.12.2` em dev:

```
Looking in indexes: .../_packaging/fabrica-ai/pypi/simple/
ERROR: Could not find a version that satisfies nlp-engine==0.12.2
       (from versions: 0.9.4, 0.10.0, 0.10.1, 0.11.2)
```

A lista é exatamente o conteúdo do feed de produção. Inventário dos dois feeds no mesmo dia:

| feed | alimentado por | versões |
|---|---|---|
| `fabrica-ai-hml` | `hml` | … 0.12.0 · 0.12.1 · 0.12.2 · 0.12.3 |
| `fabrica-ai` | `main` | 0.9.4 · 0.10.0 · 0.10.1 · 0.11.2 · 0.12.2 · 0.12.3 |

⚠️ **Não há rota alternativa.** O widget `nlp_engine_volume` foi removido no PR 7233, e o
`ntb_ia_dependencies` só monta `nlp-engine` ou `nlp-engine==<versão>`, sempre contra o índice
configurado.

**Consequência.** Para validar qualquer versão é preciso **mergear na `main` antes** — subir para
produção para só então poder testar. É a inversão do fluxo acordado em 21/08, e foi a situação da
`0.12.2`. O fluxo ficou sem implementação quando a instalação passou de wheel para `pip` em 03/09;
ninguém notou porque até então não se tentou instalar uma versão que existisse só na `hml`.

**O que se pede.** Declarar `fabrica-ai-hml` como **índice extra** do `pip` no ambiente de **dev**,
e só em dev. Não está no repositório da plataforma nem nos clusters — é configuração de workspace,
e exige admin.

**Teste de aceite.** Rodar o `ntb_ia_motor_e2e` em dev com `nlp_engine_version = 0.12.3` e conferir
`engine_version` na tabela de saída.

**Quem decide:** MLOps.

---

### 1.2 🔴 Produção usa `latest` — a versão muda sozinha

**O que é.** `jobs/ambientes/prod.json` e `hml.json` declaram `"nlp_engine_version": "latest"`. Sem
pin, o `pip` resolve a maior versão do feed.

**Evidência comportamental.** A versão do motor em produção mudou **cinco vezes** sem ninguém tocar
no job, nas quatro linhas, sempre no mesmo dia:

| até | versão em prd |
|---|---|
| 31/08 | `0.9.4` |
| 02/09 | `0.10.0` |
| 04/09 | `0.10.1` |
| 07–09/09 | `0.11.2` |
| **10/09** | **`0.12.3`** |

Job pinado não troca de versão sozinho.

⚠️ A decisão de 21/08 — *"produção deixa de usar `latest`; cada especialidade declara a versão"* —
**nunca foi aplicada**. O mecanismo existe desde o PR 7194; a configuração ficou como estava.

**O que se pede.** Pinar por especialidade. Proposta: **`0.12.3`** nas quatro linhas — é a versão
que já está rodando desde hoje, então o pin **não muda comportamento**, e é exatamente para isso
que ele serve.

⚠️ **Não pinar `0.9.4`**, que foi o combinado anterior: regride quatro versões e reintroduz um P0
do ca-estômago que entrega o oposto do laudo (contagem do defeito por versão: `0.9.4` = 18,
`0.10.1` = 4, `0.11.2` = 0).
⚠️ **Não pinar `0.12.1`**: ela **não existe** no feed de produção — o build dela foi pulado pelo
defeito descrito em 1.4.

**Quem decide:** Ops / TechLead.

---

### 1.3 🟡 A versão exigida por cada config vive em prosa

**O que é.** Cada config declara a versão de que precisa no **cabeçalho, em texto**
(*"exige `nlp_engine >= 0.8.5`"*), que nenhum código lê.

**O que se pede.** Chave no topo do `CONFIG`, ao lado de `config_version` e `model_version`:

```python
'engine_version_pin': '0.12.3',
```

O runner passa a usar esse valor quando o widget vier vazio, e a **falhar** se a wheel instalada
divergir. Sem o `assert`, um pin que não pega segue em silêncio até alguém conferir
`engine_version` na saída — aconteceu em 09/09.

ℹ️ Há precedente no time: o backtest do ca-cólon já faz esse `assert` manualmente.

**Quem decide:** Ops + DS (a chave é da config, a leitura é do runner).

---

### 1.4 ℹ️ Defeito da esteira que pulava a publicação — já corrigido

**Registro, não pedido.** O gate de release decidia pela **tag**, única no repositório, enquanto os
feeds são **dois**. As promoções da `0.12.1` e da `0.12.2` para a `main` foram **puladas em
silêncio, com deploy verde**, e produção ficou na `0.11.2` com um defeito P1 ativo.

Corrigido nos PRs 7245 e 7246: o feed de **destino** passa a decidir. Medido antes e depois:

| feed | antes | depois |
|---|---|---|
| `fabrica-ai-hml` | `publicada` | `publicada` |
| **`fabrica-ai`** | **`publicada`** | **`livre`** |

As duas versões seguintes publicaram nos dois feeds na primeira tentativa.

---

## Tema 2 — Contrato lib ↔ plataforma (card `283647`)

### 2.1 🔴 Chave `waive` e dois campos novos no blob — PR de config segurado

**O que é.** A `0.12.3` introduz `waive: {text, finding}` no critério quantitativo e emite
`gate_waived_by` / `gate_waived_error` dentro de `quantitative.<criterio>`.

**Estado.** A lib está publicada e **inerte** — nenhuma config declara a chave, e isso foi provado
no ambiente: run com a config anterior deu **zero divergência**. O PR de config
(`tirads/feature/waive-paaf`, `0.9.0-tirads`) está **pushado e segurado**, aguardando este
alinhamento.

**Evidência de impacto.** A/B local e run pelo runner, coorte de 1.052 laudos, **mesmo número pelos
dois caminhos**: 3 promovidos `0 → 1`, **zero** rebaixados, 3 de 3 com `gate_waived_by`. A dispensa
foi aplicada em **28** laudos e mudou a decisão de **3**.

**O que se pede.** Confirmação de que a chave e os dois campos não conflitam com o processo da
plataforma.

⚠️ **Registro honesto:** os campos entraram na lib **antes** deste alinhamento, contrariando a
regra do time. A exposição é zero, porque nada emite sem config, mas o procedimento é alinhar
antes.

**Quem decide:** Ops.

---

### 2.2 🔴 Contabilidade de tokens cobre só uma das duas origens de chamada

**O que é.** `llm_prompt_tokens`, `llm_completion_tokens`, `llm_input_chars` e
`llm_api_key_origin` são escritos apenas por `llm_router_step` — o caminho do **juiz**. A
**extração quantitativa de medida** chama o LLM e não registra nenhum deles.

**Evidência**, produção 10/09:

| linha | juiz | chamadas | tokens por chamada | no dia |
|---|---|---|---|---|
| **hepatologia** | ativo | 49 | **1.004,1** (990,1 + 14,0) | **49.202** |
| tirads | desligado | 143 | — | — |
| transplante_pulmao | — | 71 | — | — |
| cancer_estomago | — | 7 | — | — |

**221 das 270 chamadas do dia não têm contabilidade nenhuma.** Não é estimável por regra de três: o
input do juiz da hepatologia mede 2.000 caracteres e os laudos de TI-RADS vão de ~1.500 a 815.000.

**O que se pede.** Emitir os três campos também no bloco do critério quantitativo. **Incluído no
escopo da `0.14.0`** para reaproveitar esta rodada de alinhamento.

**Quem decide:** Ops.

---

### 2.3 🟡 O contrato de ENTRADA continua implícito

**O que é.** A plataforma supõe o que o motor devolve e a lib supõe o que recebe. O card `283647`
existe desde 21/08 para declarar os dois lados de forma testável; o de **saída** avançou, o de
**entrada** não.

**Pontos concretos que já custaram run:**

- **`column_map`** — os 7 campos que o motor exige e de qual coluna da Gold vem cada um.
  ⚠️ Copiar o mapa de outra especialidade falha em silêncio: `proced_nome_exame` não existe em
  todos os `gold_domains`.
- **`gold_filter.keywords`** casa o **NOME DO EXAME** nesta plataforma e o **TEXTO DO LAUDO** no
  runner legado. Mesma chave, semânticas opostas.
- A config **tem** de devolver o JSON por `dbutils.notebook.exit`; sem isso o runner recebe `None`
  e a mensagem não aponta a causa.

**Quem decide:** DS declara, plataforma valida na esteira.

---

### 2.4 ℹ️ Piso de contrato já versionado

**Registro.** Sete chaves eram emitidas e não declaradas; quatro saíram de auditoria e **três só
apareceram no blob de um run real**. Fechado com piso versionado de 21 chaves extraídas de 1.500
blobs.

⚠️ **Verificação de contrato por fixture é estruturalmente insuficiente** — nenhum perfil escrito à
mão esgota o que produção emite.

---

## Tema 3 — Defeitos de plataforma abertos

### 3.0 🔴 Os embeddings NÃO funcionam em produção — as três linhas rodam em `token_overlap`

**O que é.** As configs declaram `use_embeddings: True` e apontam `embedding_model` para
`/Volumes/**diamond_ia_hml**/nlp_engine/nlp_engine_lib/st_models/paraphrase-multilingual-MiniLM-L12-v2`
— o Volume do **workspace ANTIGO**, com **caminho literal idêntico nos três ambientes**. Em
produção esse caminho não existe, e a camada semântica cai para `token_overlap`.

**Evidência**, produção **10/09** — não é número de agosto:

| linha | laudos | com `FileNotFoundError` | % que cai para `token_overlap` |
|---|---|---|---|
| hepatologia | 5.172 | **5.108** | **98,8%** |
| cancer_estomago | 201 | **201** | **100%** |
| tirads | 1.568 | **1.348** | **86,0%** |

A trilha registra, laudo a laudo:

```
"semantic": "neutral — best 0.14 (below threshold) [token_overlap] FALLBACK:FileNotFoundError"
```

ℹ️ A instrumentação está **funcionando** — foi a `0.11.0` que fez essa queda deixar de ser
silenciosa. O que não devia estar acontecendo é a queda.

🔴 **A consequência é de qualidade, não de custo: produção roda um perfil que nunca foi
homologado.** A config declara `decision_mode: hybrid` com embeddings, e o que executa é a régua
mais sobreposição de tokens. Homologação não transfere entre comportamentos diferentes, e as duas
camadas puxam em direções opostas.

⚠️ **A monitoria não pega** (ver 3.6): não há coluna de LLM nem de backend semântico, e a taxa de
relevância se sustenta pela régua. Sem a trilha da `0.11.0`, isto seguiria invisível.

ℹ️ O ca-rim **já foi corrigido** — aponta para `gold_fabrica_ia_hml` desde o PR 7159. Mas ele não
está em produção; as três que estão são exatamente as três que falham.

**O que se pede.** Resolver o caminho **por ambiente**, e não por literal — é a mesma classe do
`base_url` do LLM, já resolvida no PR 7135. Depende de 4.2 (o schema de destino em prd).

**Quem decide:** Ops + Fábrica (o schema) · DS ajusta as configs depois.

---

### 3.1 🟡 `limit_rows` não isola coorte — card `298596`

O teto é aplicado **depois** da união da fila, cuja ordem é inéditos → pendentes → **reprocessados
por último**. A coorte a remedir fica fora do teto e o run **fecha com sucesso sem tocá-la**.

Medido em 02/09, hepatologia: fila de 110.777 para medir 6.398; os 1.000 primeiros gravados tiveram
**zero id em comum** com a coorte.

ℹ️ Contorno em uso: janela de um dia integralmente processado, mais o widget `reprocess_enable`.

---

### 3.2 🟡 Texto de entrada duplicado `2n+1` vezes — card `300201`

6 laudos em 4.507. É a montagem da entrada, antes da lib. Custa LLM proporcional e pode truncar por
`max_input_chars`.

---

### 3.3 🟡 A entrada recebe o laudo em RTF cru — **sem card**

**O que é.** `exm_laudo_texto` vem de `proced_laudo_exame_original`, e para parte dos exames esse
campo é o **documento RTF inteiro**: começa em `{\rtf1\ansi\ansicpg1252`, numa única linha, e o
maior tem **814.685 caracteres** — 482 mil dígitos contra 389 espaços, payload hexadecimal de
imagem embutida.

| linha | laudos | em RTF | maior |
|---|---|---|---|
| TI-RADS | 4.321 | **116** (2,7%) | 815 KB |
| hepatologia | 5.374 | **231** (4,3%) | 1.039 KB |
| **cancer_estomago** | 753 | **105** (13,9%) | 33 KB |
| transplante_pulmao | 286 | **25** (8,7%) | 512 KB |

No TI-RADS esses 116 ocupam **63,4 dos 68,6 MB** do dia — 92% do volume de texto sai de 2,7% dos
registros.

✅ **A Gold tem o texto extraído:** o mesmo struct traz `laudo_transformado`, limpo e acentuado. Nos
laudos **não-RTF** os dois campos são **byte-idênticos** (4.047 de 4.047 por md5), e
`transformado` vazio com `original` cheio ocorre em **0 de 10.734**.

⚠️ O guia `boas-praticas/04` §5.2 **manda** o `original` ser o primeiro candidato — não é engano, é
orientação escrita. O candidato a avaliar é o `coalesce`.
⚠️ A troca **aumenta recall** (`nodul` no TI-RADS vai de 6 para 53 dos 116), então exige
re-homologar o delta, não subir direto.
ℹ️ Não corrige o mojibake: o `U+FFFD` **persiste** no `transformado`, e os dois fenômenos são
disjuntos.

**O que se pede.** Decidir se abre card e se o `coalesce` entra em avaliação.

---

### 3.4 🟡 `dt_execucao_modelo` em UTC, view filtra por data local — **sem card**

Run entre 21:00 e 00:00 (BRT) devolve a view de exportação **vazia, sem erro**. Aconteceu num run
às 22:16. O agendamento das 04:00 está fora da janela, então produção não sofre — a **única defesa
hoje é uma regra escrita no checklist**, que protege só quem a conhece.

---

### 3.5 🟡 Dedup da entrada usa `id_exame` puro

Sem `config_version` nem `engine_version`: janela já processada fica bloqueada e o run termina em
segundos, **com sucesso**, processando quase nada.

ℹ️ Resolvido na prática pelo widget `reprocess_enable`, que só funciona em dev.

---

### 3.6 🟡 A monitoria não tem nenhuma coluna de LLM

Só total, relevantes, taxa e confiança. O `alert_threshold_relevance_drop` **não detecta falha de
LLM**: no TI-RADS a taxa ficou 3,17% → 3,21% enquanto **4.703 chamadas falhavam**, porque a régua
sustenta o número.

ℹ️ Relacionado a 2.2: sem contabilidade de token na extração quantitativa, não há como monitorar
custo nem detectar degradação por lá.

---

## Tema 4 — Provisionamento

### 4.1 🔴 Schema `reumatologia` só existe em `dev` — card `300348`

Necessário em `hml` e `prd` para a linha migrada entrar. Criar schema é do time da Fábrica, por
procedimento próprio.

### 4.2 🔴 Schema `nlp_engine` não existe em produção — card `298600`

`gold_fabrica_ia` tem apenas `fhir` e `information_schema`. **É o destino que falta para resolver o
item 3.0** — sem schema em prd, não há para onde apontar o `embedding_model`.

✅ O MiniLM já foi copiado para `gold_fabrica_ia_hml/nlp_engine/nlp_engine_lib/st_models/` em 01/09;
falta o equivalente em produção.

⚠️ **A nota anterior dizia que "hoje responde, o risco é latente". Isso não se sustenta mais:** a
medição de 10/09 mostra o `FileNotFoundError` acontecendo em 86% a 100% dos laudos das três linhas.
O risco não é latente — está materializado desde antes.

ℹ️ O `mpnet-base-v2` fica **fora de escopo**: só existe no volume antigo e serve notebooks legados.

---

## Tema 5 — PRs e configs parados

| # | o quê | aguarda | desde |
|---|---|---|---|
| 5.1 | **PR 7231** — migração de reumatologia, 6 arquivos, adição pura, validada ponta a ponta em dev | revisão | 08/09 |
| 5.2 | **PR 7228** — doc do consumidor da `0.12.1`, 89 linhas, adição pura | revisão | 08/09 |
| 5.3 | **PR de config `0.9.0-tirads`** — não aberto, segurado pelo item 2.1 | este alinhamento | 09/09 |
| 5.4 | **`gold_filter` de punção** — não iniciado, ver abaixo | aval para começar | — |

**Sobre 5.4.** O exame que originou o relato do negócio sobre o TI-RADS **nunca chega ao motor**: o
`gold_filter` seleciona por `tireoide`/`pescoço`, e `punção aspirativa por agulha fina guiada por
ultrassonografia` não casa nenhum dos quatro termos.

São **67** punções citando TI-RADS 4 em 16 dias barradas na entrada, contra **36** rebaixadas pelo
gate — **a causa de fora é maior que a de dentro**. Custo de incluir: **+492 exames em 16 dias
(~31/dia)** sobre uma linha que processa ~3.600/dia.

⚠️ Filtro de entrada é **invisível para A/B local** — só se mede rodando. Por isso vai em PR
próprio, com run próprio.

---

## Tema 6 — SPEC 27 diverge do código — card `299238`

Card acumulador. A divergência mais cara já custou uma subida: a SPEC §6.1 afirma que **nada lê
`runtime`**, e `ntb_ia_loader.py:105-113` faz `llm_router.update(runtime_llm)` — o `runtime`
**sobrescreve** o `nlp`.

**O que se pede.** Alinhar de uma vez, junto do `283647`, em vez de acumular ajustes.

---

## Tema 6b — Proposta: definir o que é "validado" ao trocar versão da lib

### 6b.1 🟡 A palavra existe no acordo e não existe definida em lugar nenhum

**O que é.** O fluxo acordado e a pinagem por linha invocam *"nenhuma versão nova entra sem
validação prévia"*. **Essa palavra não tem definição escrita.** Sem ela, cada promoção reabre a
mesma discussão e a validação vira o que cada um entende por ela.

⚠️ **O playbook de paridade existente não cobre.** Ele trata de evoluir **config e régua** contra
gabarito. Na troca de versão a config fica congelada, só a engine muda, e **não existe gabarito** —
precisão e recall não são calculáveis. O que se mede é o **delta de decisão**.

**O que se leva.** Uma proposta escrita, em
[`_processo/procedimento-promocao-de-versao.md`](procedimento-promocao-de-versao.md): quatro números
obrigatórios, seis passos de medição, sete armadilhas que já invalidaram medição aqui, e critério de
aceite em três itens.

**O que se pede.** Que o time discuta e decida se adota. **Aprovada, a proposta vira página da wiki**
em `/Fábrica de IA` — regra e convenção do time não moram em card nem em repositório de projeto — e
os cards passam a citá-la por link em vez de reescrevê-la.

ℹ️ **Não cria passo novo.** Formaliza o que já se faz nas medições desta semana; dá nome e piso.

🔴 **Depende do item 1.1 para ser executável.** Hoje dev resolve o `pip` contra o feed de produção,
então só se valida em dev uma versão que **já está em produção**.

**Quem decide:** o time, com Ops e Ciência de Dados.

---

## Tema 7 — Governança: uso de agentes na revisão de código

### 7.1 🟡 Diretriz sem canal de emissão — o alcance foi esclarecido, o mandato não

**O que é.** Circulou restrição ao uso de agentes de IA sobre conteúdo de revisão de código. Houve
esclarecimento posterior de que o **alvo são times externos**, não os integrantes do próprio time —
o que resolve a aplicação imediata e **não fecha o item**.

**O que permanece em aberto, e é o que a pauta trata:**

- **A diretriz não foi emitida por nenhum canal com mandato para isso.** Não houve comunicação por
  PO, PMO ou Head. Nasceu e circulou entre pares.
- **As duas frentes envolvidas são lideranças técnicas de mesmo nível** — Ciência de Dados e MLOps,
  disciplinas distintas, nenhuma subordinada à outra. Orientação entre pares não vincula a frente do
  outro; para vincular, precisa vir de quem pode emitir.
- **Escopo esclarecido em conversa não é diretriz.** Enquanto o alcance ("times externos") não
  estiver escrito onde as demais diretrizes do time vivem, a próxima aplicação volta a depender de
  interpretação de quem a lê — e o esclarecimento não alcança quem não estava na conversa.
- **O lugar da declaração também é parte do problema.** Restrição anotada dentro de artefato de
  revisão não produz proteção: comentário de pull request não tem controle de acesso próprio — quem
  enxerga o PR enxerga o comentário.

ℹ️ Os agentes em uso rodam em **conta corporativa administrada pela própria empresa** — o conteúdo
não trafega para fora do controle dela. Qualquer restrição precisa declarar contra o que protege,
já que o vazamento para fora do perímetro não é o risco em causa.

**O que se pede.** Que a diretriz seja emitida — ou dispensada — por quem tem mandato, e registrada
com três coisas explícitas:

- **a quem se aplica** (o esclarecimento sobre times externos, por escrito);
- **o critério que a aciona** — tipo de conteúdo, ambiente ou classificação, não caso a caso;
- **onde ela se declara**, num lugar com controle de acesso compatível com o que pretende proteger.

Se a conclusão for que não há restrição, **registrar que não há** tem o mesmo valor: fecha o vazio
que hoje é preenchido por convenção individual.

**Quem decide:** não é decisão entre pares técnicos. Sobe para PO / PMO / Head, com registro onde
as demais diretrizes do time vivem.

---

## Pauta proposta — 80 minutos

| bloco | tempo | itens | saída esperada |
|---|---|---|---|
| **1. Desbloqueio** | 15 min | 1.1, 1.2 | data para o índice de dev; versão a pinar por linha |
| **2. Contrato** | 20 min | 2.1, 2.2, 2.3, 6 | aval dos campos novos; dono do contrato de entrada |
| **3. Embeddings em produção** | 15 min | **3.0**, 4.2 | destino do modelo em prd e prazo — é o item de maior impacto clínico |
| **4. Defeitos e provisionamento** | 10 min | 3.3, 3.4, 4.1 | quais viram card; prioridade relativa |
| **5. Fila de PRs** | 10 min | 5.1 a 5.4 | revisor e prazo para cada |
| **6. Definição de "validado"** | 5 min | **6b.1** | proposta escrita; o time adota? aprovada, vira página da wiki |
| **7. Governança** | 5 min | **7.1** | diretriz de uso de agentes: quem emite, a quem se aplica, onde fica escrita — ou registro de que não há |

**Preparação sugerida:** este documento circula antes. Os itens 1.1 e 1.2 podem ser decididos por
mensagem — se saírem antes, a agenda começa pelo bloco 2.

---

## O que já foi resolvido do nosso lado

Registro para a reunião não gastar tempo com o que já fechou:

- ✅ Defeito da esteira que pulava a publicação em produção — corrigido (item 1.4).
- ✅ `0.12.2` e `0.12.3` publicadas nos dois feeds, validadas em ambiente.
- ✅ **O LLM voltou a funcionar em produção** — 270 chamadas em 10/09, zero erro. Estava com
  8.058 tentativas e zero sucessos em 27/08.
- ✅ Migração de reumatologia validada ponta a ponta em dev: paridade de 99,42% em cinco medições
  independentes, cinco arquivos entregues e conferidos.
- ✅ 15 cards de higiene da `0.12.x` em *Pronto para QA*, com 67 evidências medidas.
