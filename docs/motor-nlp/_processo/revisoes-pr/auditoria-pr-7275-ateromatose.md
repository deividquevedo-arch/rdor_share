# Auditoria — PR 7275, ateromatose coronariana

> **Parecer local, não postado no DevOps.** 15/09/2026, atualizado às 21:30.
>
> PR `7275` — *[ATEROMATOSE_CORONARIANA] Migrar a régua legada para a plataforma: config 0.1.0, job
> e navegação* · branch `ateromatose_coronariana/feature/migracao-plataforma` → `hml` · revisor
> único, sem voto.
>
> Auditado contra a régua de config de 03/09, o POP-IA-08, o padrão de navegação de 11/09 e as
> armadilhas conhecidas do `nlp_engine`.

---

# 1. Veredito

**O PR é bom, e o material de evidência é o melhor que passou por aqui.** Seis arquivos, **817
inserções, adição pura** — confirmado contra o merge-base, não contra a ponta da `hml`.

🔴 **O principal é o sync com a `hml`**, e ele vem antes de tudo: a branch está **seis dias atrás**,
a `hml` avançou em mais de 38 arquivos, e sem o sync há risco de conflito e o ambiente local não
reflete o que o merge vai encontrar. **Sincronizar e atualizar o local antes de pedir nova revisão.**

Feito o sync, restam **cinco ajustes**, nenhum de régua clínica — todos em arquivos que a própria
branch criou, e que o sync **não corrige**.

| # | achado | natureza |
|---|---|---|
| 1 | `nlp_engine_version` entra **despinado** | 🔴 contraria o padrão das outras 6 linhas |
| 2 | `descriptografia` declarado na navegação | 🟡 vira bloco morto após o rebase |
| 3 | bloco `runtime` presente | 🟡 sai de todas as configs por decisão de 15/09 |
| 4 | `decision_mode: hybrid` com `use_embeddings: False` | 🟡 bloco declara o que não vale |
| 5 | `prd` com `send_email: True` e os destinatários de hml | 🔴 despausar entrega ao destinatário errado, em silêncio |

---

# 2. O que está correto — e é a maior parte

| verificação | resultado |
|---|---|
| Blocos não lidos — `catalog`, `monitoring`, `distribution`, `data.legacy` | ✅ **todos ausentes** — melhor que 4 das 6 configs em produção |
| `nlp.llm_router.enabled` declarado explicitamente | ✅ `False`, explícito |
| Coerência `nlp` × `runtime` no `llm_router` | ✅ ambos `False` — o merge não altera nada |
| `segmentation.mode` | ✅ `full_doc` — não cai na armadilha do `auto` |
| `findings` no formato v3 por-entidade | ✅ `label`, `terms`, `regex`, `exclude`, `unless`, `negation_direction`, `skip_organ_gate` por achado |
| `negation.direction_default` | ✅ chave correta — lida em `config_loader.py:444` |
| `specialty_id` × nome do arquivo | ✅ casam |
| Navegação: 22 colunas, 3 ocultas → 19 visíveis, `colunas_manuais` vazio | ✅ padrão de 11/09 |
| Os três ambientes idênticos em chave e rótulo | ✅ conferido por comparação de listas |
| `validacao` com o filtro etário do legado | ✅ `p.cli_idade BETWEEN 45 AND 70` |
| Job nasce `PAUSED` | ✅ primeira execução manual |
| Bloco `data` presente e completo | ✅ `gold_domains`, `filters.gold_filter`, `column_map` |
| Juiz LLM desativado | ✅ `enabled: False` em `nlp` **e** em `runtime` — sem contradição |
| Destinatários de `dev` e `hml` | ✅ dev só o desenvolvedor; hml com acompanhamento |
| `embedding_enable: 'false'` no job | ✅ coerente com `use_embeddings: False` |
| Cabeçalho e changelog no arquivo | ✅ 199 linhas de cabeçalho antes do `CONFIG` |

**Régua:** 3 achados, 112 termos, 39 frases de negação com janela 7, órgão-alvo `coracao`. Sem
quantitativo, sem RADS, sem `document_vet` — perfil léxico puro, coerente com `rule_only`.

---

# 3. Achados antes do merge

## 3.1 🔴 `nlp_engine_version` entra despinado

```json
"nlp_engine_version": "${nlp_engine_version}"
```

As **outras seis definições de job declaram `"0.12.3"` literal**, em `main` e em `hml`. O próprio
`jobs/ambientes/hml.json` registra: *"Em hml/prd prefira a versão exata: sem pin, uma versão nova
publicada no feed entra em produção sem validação."*

**Consequência:** a paridade foi medida com a engine `0.12.3`; sem pin, a próxima versão publicada
entra sem que a medição se aplique.

ℹ️ **Não é descuido:** o merge-base é de **09/09**, e o pin literal entrou nas seis definições
depois. É consequência do rebase pendente.

**Ajuste:** `"0.12.3"`, igual às demais.

## 3.2 🟡 `descriptografia` declarado — vira bloco morto após o rebase

Os três arquivos de navegação declaram o bloco. Na `hml` atual, o builder **não lê mais** — a view
aplica `security.prd.rdsl_decrypt` na projeção, e a documentação já diz que não há nada a declarar
na config.

ℹ️ O histórico mostra que ele **foi removido** (`4f6edf4`) e **reintroduzido** (`b5d1705`, *"padrão
da reumatologia"*) — correto para a base de 09/09, quando a reumatologia ainda declarava.

✅ **Atualização de 15/09 às 21:28:** o PR `7287` foi mergeado na `hml` e o TI-RADS passou ao mesmo
padrão — **22 colunas, 3 ocultas, sem `colunas_manuais` e sem `descriptografia`**, nos três
ambientes. Com reumatologia e TI-RADS já no formato, **o padrão está consolidado na árvore** e há
dois exemplos para copiar depois do sync.

🔴 **E isso muda a leitura de um limite declarado no PR.** O PR informa que *"as cinco colunas de
identificação saíram CIFRADAS no envio"* e atribui a falta de `USE CATALOG` no catálogo `security`.
Após o rebase, quem decifra passa a ser a view — **o teste precisa ser refeito**, e o diagnóstico
pode mudar.

## 3.3 🟡 O bloco `runtime` sai de todas as configs

Decisão de 15/09: o bloco perdeu a função — existia para sobreposição via widget, e nenhum dos 15
widgets do runner o alimenta. Sai das seis configs.

Este PR adiciona a sétima. **Não é erro** — é o padrão vigente, e o guia `boas-praticas/02` ainda
manda preenchê-lo. Mas cria trabalho de limpeza imediato.

**Sugestão:** já nascer sem o bloco, e a linha entra no formato final.
⚠️ Neste caso é seguro: `nlp` e `runtime` declaram o mesmo `enabled: False`, então remover tem
**delta zero** — diferente de hepatologia e transplante.

## 3.4 🟡 `decision_mode: hybrid` com `use_embeddings: False`

```python
'embeddings': {'use_embeddings': False,
               'decision_mode': 'hybrid',
               'embedding_model': '/Volumes/diamond_ia_hml/nlp_engine/...'}
```

Três problemas num bloco só:

- **O bloco é inerte** — com `use_embeddings: False`, nada ali vale. Pela régua de 03/09, bloco
  declarado e não consumido não passa.
- **`decision_mode: hybrid` engana** — quem lê conclui que a camada semântica participa.
- **`embedding_model` aponta para `/Volumes/diamond_ia_hml/`**, o volume do workspace antigo — o
  mesmo caminho dos cards `298600` e `305810`. Inerte aqui, mas propaga um valor que já é defeito
  conhecido em quatro linhas.

**Ajuste:** remover o bloco, ou declarar `decision_mode` coerente com o que executa e tirar o
caminho literal.

## 3.5 🔴 Produção nasce com `send_email: True` e os destinatários de homologação

| ambiente | `send_email` | `notificacao.ativo` | destinatários |
|---|---|---|---|
| dev | `True` | `True` | só o desenvolvedor — ✅ padrão correto |
| hml | `True` | `True` | desenvolvedor + acompanhamento |
| **prd** | **`True`** | **`True`** | **exatamente os mesmos de hml** — sem o negócio |

O PR declara: *"Prd nasce sem e-mail de negócio"*. A declaração está correta — **o risco é o
`send_email` estar ligado junto.**

🔴 **O que acontece se o job for despausado antes de a lista existir:** o arquivo é gerado, enviado
com sucesso, e o negócio **não recebe** — sem erro, com `sent=True` e `HTTP 202`. É a mesma classe
do caso do arquivo que não gerou e-mail na reumatologia: `202` é aceite assíncrono, não entrega, e
nada no run denuncia destinatário errado.

⚠️ **Não é vazamento** — os dois endereços são internos. É **entrega silenciosa ao destinatário
errado**, que só aparece quando alguém do negócio reclama que não recebeu.

ℹ️ **O padrão das linhas em produção é o oposto:** reumatologia e TI-RADS têm o negócio no `emailTo`
e o acompanhamento técnico no `emailCc`.

**Ajuste sugerido:** em `prd`, ou os destinatários do negócio entram agora, ou **`send_email: False`**
até que entrem. Assim despausar o job não pode produzir entrega ao destinatário errado.

---

# 4. Pontos a registrar, sem bloquear

## 4.1 A paridade não atinge o alvo da SPEC — e está declarado

`match_rate` **97,3%** contra alvo de 99%, com **50 divergências críticas**. O PR declara e abre as
475 uma a uma, com atribuição de causa. **A declaração é exemplar** — o que se pede da migração é
exatamente isto.

⚠️ **O número que merece leitura separada:** nos **positivos do legado** a concordância é **66,4%**.
O 97,3% vem de base quase toda negativa. Quem ler só o número de topo superestima a paridade.

ℹ️ **11 das 50 críticas são defeito desta versão**, adiadas para a `0.1.1` após resposta clínica —
declarado, com dono e destino.

## 4.2 🔴 A precisão contra o gabarito é 0,556

Gabarito de 44 laudos rotulados pela médica da linha: **39 de 44 iguais, precisão 0,556, recall
0,833**.

O PR argumenta, corretamente, que o critério deste merge é **paridade comportamental**, não acerto
clínico, e que o gabarito só cobre o que o legado navega.

⚠️ **Mas o número precisa estar visível na decisão de ligar o job**, que é outro momento e outra
régua. Precisão 0,556 significa que perto de metade do que se entrega não corresponde ao rótulo. O
job nascer `PAUSED` protege isso — e é a razão pela qual este ponto não bloqueia o merge.

## 4.3 `gold_filter` com regex de lookahead

```python
'keywords': ['^(?!.*(angi|biop))(?=.*(tc|tomo))(?=.*torax)']
```

✅ O lookahead negativo está **ancorado em `^`** — a armadilha conhecida (lookahead negativo sem
âncora não exclui nada) foi evitada.

⚠️ `torax` **sem acento**. Se a descrição do procedimento trouxer `TÓRAX`, o filtro não casa. A
execução de 15.647 laudos mostra que casa no ambiente medido — o risco é de corpus, não atual.

ℹ️ E vale contra a SPEC 27 §7, que afirma não haver caminho por config para regex com lookbehind.
**Há** — é mais uma divergência da SPEC, já registrada no card `299238`.

## 4.4 Pendências que o próprio PR declara

Unidades e destinatários de produção não traduzidos · schema `ateromatose_coronariana` só existe em
`dev` · `A2` cobre 26,5% do snapshot por desalinhamento de janela entre `dt_exame` e
`dataExecucaoModelo`.

✅ Todas declaradas no corpo do PR, com destino.

---

# 5. O efeito do sync com a `hml`

A branch parte de **`f569ee6`, de 09/09**. Desde então a `hml` avançou em **mais de 38 arquivos** —
a última entrada é de 15/09 às 21:28 —
incluindo: o pin literal nas seis definições de job · a view aplicando `rdsl_decrypt` · o builder do
Excel deixando de descriptografar · o padrão de navegação · e sete arquivos de documentação.

⚠️ **O sync não corrige nenhum dos cinco achados.** Os arquivos que os contêm — o job, a config e os
três de navegação — são **novos, criados por esta branch**, e um merge da `hml` não os toca.

**O que o sync faz:**

| efeito | consequência |
|---|---|
| alinha o **ambiente** | a view passa a aplicar `rdsl_decrypt` — o relato de colunas cifradas precisa ser reavaliado sobre a base nova |
| torna o **padrão visível** | as seis definições de job com `"0.12.3"` literal e **dois conjuntos completos de navegação no formato final** — reumatologia e TI-RADS, este mergeado em 15/09 às 21:28 pelo PR `7287` — passam a estar na árvore, prontos para copiar |
| evita **conflito** no merge | a branch deixa de estar seis dias atrás |

**Os cinco ajustes da §7 continuam sendo edições a fazer, depois do sync.**

## 5.1 O que reconferir depois do sync — para a segunda revisão ser curta

- [ ] `jobs/definicoes/ateromatose-coronariana-batch.json` → `"nlp_engine_version": "0.12.3"`
- [ ] Config sem o bloco `runtime`
- [ ] Config sem o bloco `embeddings` inerte, ou com `decision_mode` coerente e sem caminho literal
- [ ] Os três arquivos de navegação sem `descriptografia`
- [ ] `prd` com `send_email: False` **ou** com os destinatários do negócio
- [ ] **Envio refeito** sobre a base nova: as cinco colunas de identificação saem em claro?
- [ ] Diff confirmado **contra o merge-base**, não contra a ponta da `hml`

# 6. O que não consegui verificar

- **A bateria de 16 testes e o notebook de paridade** vivem em `ateromatose_coronariana/tests/bancada-paridade`, fora deste PR — não auditei.
- **O gate de 14 alvos** citado no commit `de53c1b` — não reproduzi.
- **As 475 divergências uma a uma** — li a classificação declarada, não a evidência por caso.
- **A tradução 1:1 contra o `CONFIG` do legado** — não comparei termo a termo.

---

# 7. Recomendação

## 🔴 Primeiro, e é o principal: sincronizar com a `hml`

A branch parte de **09/09** e a `hml` avançou em mais de 38 arquivos desde então — a última entrada
é de 15/09 às 21:28.

**Por que vem antes de tudo:**

- **evita conflito** no merge, em arquivos que várias frentes tocaram no mesmo dia;
- **atualiza o ambiente local** para o que o merge vai encontrar — o teste de envio rodou sobre uma
  base em que a view ainda não decifrava;
- **traz o padrão para a árvore** — reumatologia e TI-RADS já estão no formato final, e as seis
  definições de job já estão pinadas. Depois do sync é copiar, não inventar.

⚠️ **O sync não corrige nenhum dos cinco ajustes abaixo** — os arquivos são novos, criados pela
branch, e o merge não os toca. Ele muda o contexto, não o conteúdo.

## Depois do sync, cinco ajustes

| # | ajuste | onde |
|---|---|---|
| 1 | `"nlp_engine_version": "0.12.3"` literal | `jobs/definicoes/ateromatose-coronariana-batch.json` |
| 2 | remover o bloco `runtime` — delta zero neste caso | config da especialidade |
| 3 | remover o bloco `embeddings` inerte, ou torná-lo coerente e sem caminho literal | config da especialidade |
| 4 | remover `descriptografia` | os três arquivos de navegação |
| 5 | `prd`: `send_email: False` **ou** incluir os destinatários do negócio | navegação de prd |

## E refazer o teste de envio

Sobre a base nova, para saber se as cinco colunas de identificação saem em claro — o diagnóstico
registrado no PR pode mudar.

---

ℹ️ **O que está correto não é pouco:** o bloco `data` está completo, o juiz está desligado sem
contradição entre `nlp` e `runtime`, os blocos não lidos estão ausentes — melhor que 4 das 6 configs
em produção —, a navegação segue o padrão de 22 colunas nos três ambientes, e o job nasce `PAUSED`.

ℹ️ Os pontos da §4 não bloqueiam: são risco declarado, e o job nasce `PAUSED`. Mas a precisão de
0,556 contra o gabarito deve estar na mesa **quando se decidir despausar**, não agora.

---

# 8. Segunda revisão — 16/09/2026

> Motivo: o autor sinalizou ter executado o que foi pedido. A pergunta a responder é se houve
> **nova execução para coletar evidência** ou apenas edição de arquivo.

## 8.1 Resposta direta: foi ajuste, não nova medição — e o próprio PR declara isso

Ponta da branch em `35c006f` (15/09 às 21:43). A busca por evidência nova no cabeçalho devolve o
oposto, escrito pelo autor:

> `match_rate` da `0.2.0`: **pendente**.

> ATENCAO: toda a paridade publicada abaixo foi medida em `rule_only`, na `0.1.0`. A semantica
> alarga o recall e o juiz estreita: o perfil deixou de ser o que foi medido e a paridade precisa
> ser remedida no perfil novo antes de sustentar o merge.

E, sobre o envio:

> Reteste **depois do merge**.

✅ **Não há overclaim.** Nenhum número antigo foi reapresentado como se valesse para o perfil novo:
os dois títulos de paridade passaram de *"mesma `config_version`"* para *"config `0.1.0`, juiz
desligado"*, e a pendência está impressa onde o número aparece. O registro está correto.
🔴 **Mas a consequência permanece:** o critério de merge deste PR é paridade, e a paridade que
existe é de um perfil que a `0.2.0` não executa mais.

## 8.2 O sync foi feito — por rebase, não por merge

A branch foi **rebaseada** sobre `cf281e7` (ponta da `hml`, 15/09 às 21:28): os dez commits têm
data de commit `15/09 21:21` e data de autor preservada. `origin/hml` não tem nenhum commit fora da
branch. O ponteiro `de53c1b` citado na primeira auditoria deixou de existir.

ℹ️ Três commits são trabalho novo, por data de autor: `71b9e80` (20:53), `86cdf09` (21:09) e
`35c006f` (21:43). Os outros sete são a história anterior reescrita pelo rebase.

## 8.3 Os cinco ajustes — todos aplicados

| # | pedido | estado |
|---|---|---|
| 1 | `nlp_engine_version` literal | ✅ `"0.12.3"` |
| 2 | remover o bloco `runtime` | ✅ removido |
| 3 | ligar semântica + juiz + widget | ✅ as três chaves, no mesmo commit da remoção do `runtime` |
| 4 | `descriptografia` fora dos 3 arquivos de navegação | ✅ nos três |
| 5 | e-mail de prd | ✅ negócio no `emailTo`, acompanhamento técnico no `emailCc` |

O cabeçalho ganhou a seção *Perfil completo na 0.2.0*, com a regra das três chaves, o efeito do
`runtime` sobre o `nlp` e o critério de aceite da run (`semantic_score` não nulo em 100%, zero
`FALLBACK`, `llm_called` > 0). A `config_version` subiu para `0.2.0-ateromatose_coronariana-perfil-completo`.

## 8.4 Uma mudança não pedida, e ela está certa

As duas tasks de exchange perderam `"disabled": "${disabled?}"`. Não constava dos cinco itens.

✅ **Confere com `jobs/README.md` §3.3**, que reserva a chave para especialidade **já em produção** —
o comentário do próprio template diz *"só se já estiver em PRD"*. Linha nova nasce com o exchange
ligado em homologação, que é como se valida ponta a ponta. A justificativa está no changelog.

## 8.5 Dois pontos que a segunda passada abre

### 🟡 A lista de negócio de prd foi inferida, não confirmada

`larissa.vfernandes` e `rubia.linares` entraram no `emailTo` de prd por leitura dos últimos commits
das outras linhas em `origin/hml` (tirads `dcbe865`, cancer_rim `aac89ff`). É inferência de padrão
entre linhas de cuidado diferentes.

✅ **Exposição hoje é zero** — o job nasce `PAUSED` e o cabeçalho registra que a lista *"ainda
precisa de confirmacao escrita do PO antes de despausar"*. Fica como item de despausa, não de merge.

### 🟡 O grant da PII mudou de dono, e isso não foi verificado nesta linha

O cabeçalho passou a dizer que *"o grant passa a ser do principal que CRIA a view"*. A afirmação
está correta quanto ao mecanismo — `pipeline_e2e/nlp_ia_06_view.py` aplica `security.prd.rdsl_decrypt`
na projeção — e o comentário no código confirma: *"Exige o grant de execução da função para quem
cria a view."*

🔴 **O grant não desapareceu, mudou de sujeito.** Continua sendo a mesma pendência transversal
(`USE CATALOG security` + `EXECUTE`) que aparece no DII e é candidata a explicar o base64 medido em
89 de 89 linhas da view do TI-RADS. Postergar o reteste para depois do merge deixa o risco de a
view da ateromatose nascer entregando cifrado.

## 8.6 Recomendação atualizada

**O PR está tecnicamente pronto e a documentação é honesta.** O que falta não é edição, é execução:

1. 🔴 **Rodar a `0.2.0` no perfil completo em dev** e publicar os quatro números do aceite que o
   próprio cabeçalho fixou — `camada_rodou` = laudos, `degradou` = 0, `llm_called` > 0, e o
   `match_rate` remedido.
2. 🟡 **Refazer o teste de envio** na base sincronizada, para separar o que era descriptografia no
   exchange do que é grant na criação da view.
3. ✅ O resto fica para depois do merge, como já estava.

⚠️ **Sem o item 1, o merge apoia-se em paridade de um perfil que a config não executa mais.** É
exatamente a armadilha que a régua de entrega ao negócio descreve: homologação não transfere entre
perfis, e as duas camadas puxam em direções opostas.
