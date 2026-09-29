# TI-RADS — o que precisa ser ajustado em config

> Levantado em **14/09/2026**, contra o `origin/main` da plataforma — que é o que rodou hoje em
> produção (`0.8.0-tirads`, engine `0.12.3`, 3.410 laudos).
>
> Dois arquivos: a **config da especialidade** e o **arquivo de navegação** (3 ambientes).

---

## Resumo

| # | onde | o quê | depende de |
|---|---|---|---|
| 1 | navegação | 🔴 a coluna **"Achado" é sempre vazia** — o dado está na view, ao lado | **nosso** — trocar o mapeamento |
| 2 | navegação | 27 colunas manuais exportadas em branco | negócio |
| 3 | config | `gold_filter` não captura punção — **67 exames não chegam ao motor** | nosso, com medição |
| 4 | config | `embedding_model` aponta para o workspace antigo | nosso + schema em prd (Fábrica) |
| 5 | config | três blocos mortos: `catalog`, `monitoring`, `distribution` | nosso |
| 6 | config | `waive` para PAAF — config `0.9.0-tirads` pronta | aval de contrato (Ops) |

---

# 0. O que os dados sustentam — sem depender de relato

Medido em produção, 21/08 a 14/09: **58.651 laudos, 2.370 relevantes.**

## 0.1 O motor sempre entrega achado; o arquivo nunca entrega

| fato | número |
|---|---|
| relevantes com `findings` preenchido na tabela de saída | **2.370 de 2.370 (100%)** |
| `classificacao_rads` não-nulo na view de exportação (prd) | **0 de 89** |
| idem, em hml | **0 de 89** |

**Conclusão:** o achado é produzido, chega à view, e é descartado no mapeamento do arquivo. Não é
falha do motor nem da view — é a coluna errada mapeada.

## 0.2 "TR pelado" caiu 3,6× com os bumps — mas não zerou

Entregas cujo `findings` traz **só a categoria**, sem lesão nomeada (`nódulo`, `cisto`, `massa`,
`lesão`, `bócio`, `linfonodo`):

| engine | relevantes | só a categoria | % |
|---|---|---|---|
| **`0.9.4`** | 1.575 | 342 | **21,7%** |
| `0.10.0` | 72 | 6 | 8,3% |
| `0.10.1` | 169 | 8 | 4,7% |
| `0.11.2` | 235 | 16 | 6,8% |
| **`0.12.3`** | 319 | 19 | **6,0%** |

🔴 **A `0.9.4` entrega TR pelado em 21,7%.** É a versão que chegou a ser proposta como pin. A queda
para 4,7% coincide com a `0.10.1`, que corrigiu a legenda ACR descendente — causa conhecida e
medida à parte.

⚠️ **As versões rodaram em janelas diferentes, então o corpus não é o mesmo.** O que sustenta a
leitura é a ordem de grandeza (21,7% contra 6,0%, com n = 1.575 na `0.9.4`) e a coincidência com uma
causa já isolada, não a comparação pareada.

🟡 **Sobram 6,0%.** É o resíduo que o `waive` (§2.4) e o `gold_filter` (§2.1) endereçam — e é a
medida de quanto ainda falta.

## 0.3 O que ainda não está medido

- **Quantos dos 67 exames de punção passariam a entregar** se o `gold_filter` os capturasse — filtro
  de entrada só se mede rodando.
- **Qual o efeito real dos embeddings desligados** na decisão: sabe-se que 86% cai em
  `token_overlap`, não se sabe quantas decisões mudariam com eles funcionando.

---

# 0.4 ✅ Validado em ambiente — 15/09

Arquivo gerado pelo envio, regional SP, **55 registros**, com a branch
`tirads/feature/exchange-achado-prd`.

| verificação | resultado |
|---|---|
| run concluído, arquivo gerado | ✅ |
| **22 colunas, na ordem do padrão**, com os rótulos definidos | ✅ |
| bloco manual removido | ✅ zero colunas de trabalho — seriam 49 no layout antigo |
| 🔴 **"Achados do Modelo" preenchido** | ✅ **55 de 55** |
| "Evidência do Achado" preenchido | ✅ 55 de 55, até 156 caracteres |

**Qualidade do conteúdo:** 42 `TR4` · 13 `TR5` · **55 de 55 com lesão nomeada** — zero "TR pelado"
nesta amostra. O maior valor de `Achados do Modelo` tem 39 caracteres, o que sustenta `size-285`; a
evidência chega a 156, o que sustenta `size-425` com `wrap`.

🔴 **PII em base64 — 55 de 55** em `Nome Paciente`, `CPF`, `Telefone` e `Médico Solicitante`; 45 de
45 em `CRM`. É o estado do ambiente: a view não decifra e o `rdsl_decrypt` só existe na `hml` do
repositório, ainda não promovido. **Não é regressão desta mudança** — mas registra que, se esse
arquivo fosse ao negócio hoje, não teria nome de paciente.

⚠️ **O que o arquivo entregue em CSV não prova:** colunas ocultas, larguras e `wrap` são formatação
do Excel e não sobrevivem à conversão. Ficam não verificados.

ℹ️ **O nome do arquivo carrega só a data**, sem hora (`..._SP_2026_09_15`). Re-run no mesmo dia
colide no mesmo caminho — mesmo achado já registrado na reumatologia.

---

# 1. Arquivo de navegação

## 1.1 🔴 A coluna "Achado" é sempre vazia — e o dado existe

O arquivo entrega a coluna **N — "Achado"**, mapeada para `classificacao_rads`.

A view materializa essa coluna assim (`plataform/pipeline_e2e/nlp_ia_06_view.py:133-137`):

```python
"observacoes":        "CAST(NULL AS STRING)",
"complexidade":       "CAST(NULL AS STRING)",
"classificacao_rads": "CAST(NULL AS STRING)",
"resultado":          "CAST(NULL AS STRING)",
"achados":            "CAST(NULL AS STRING)",
```

**Ou seja: todo laudo entregue sai com "Achado" em branco, por construção.** Não é falha
intermitente — é 100% das linhas, sempre.

⚠️ **E o dado existe.** A tabela de saída tem a coluna `findings` preenchida — é o que a lib produz
no formato `TR4 - Nódulo (1,8 cm)`. O que falta é a view ligar uma coisa na outra.

✅ **MEDIDO EM 14/09 — e a correção é nossa, de uma linha.**

| medição | resultado |
|---|---|
| relevantes em 24 dias (21/08–14/09) com `findings` preenchido | **2.370 de 2.370 — 100%** |
| a view de exportação carrega `findings`, `findings_spans`, `findings_match` | **sim**, são colunas da view |
| `classificacao_rads` não-nulo na view de **prd** | **0 de 89** |
| `classificacao_rads` não-nulo na view de **hml** | **0 de 89** |

🔴 **O achado existe até a última etapa e é descartado no mapeamento do arquivo.** A view entrega o
achado em `findings`; o arquivo de navegação mapeia `classificacao_rads`, que a view materializa
como `CAST(NULL AS STRING)` — uma coluna ao lado.

**Correção:** mapear `"findings": "Achado"` no arquivo de navegação, em vez de
`"classificacao_rads"`. É **config, nossa alçada**, nos três ambientes. **Não precisa tocar na
view.**

ℹ️ Com o mapeamento corrigido, a `validacao.classificacao_rads` — hoje desativada de propósito,
porque `NULL IN ('4','5')` zerava o DataFrame e o notebook encerrava com *0 arquivo(s), sem erro* —
volta a ser possível, sobre `findings`.

⚠️ **O que isto NÃO afirma.** Não afirma que foi isto que o negócio observou. Afirma que, no arquivo
entregue, **a coluna "Achado" vem vazia em 100% das linhas** — inclusive nas 83,5% cujo laudo tem
lesão nomeada. É fato medido, independente de qualquer relato.

## 1.2 As 27 colunas manuais

O bloco `colunas_manuais` declara **27 colunas (P..AP)** exportadas em branco: *Achado Relevante*,
*Oncologia*, *Comentarios*, *Cadastro Convenio*, *Data 1º Contato*, *Paciente Navegado*, *Mes*,
*Ano*, *Diagnostico*, *Nome do Medico*, *Data da Cirurgia*, *Observacoes*, entre outras.

É exatamente o bloco que a **reumatologia removeu** em 11/09, por definição do negócio — o arquivo
passou a entregar só as colunas da view, e os três arquivos caíram de ~640 para ~165 linhas.

**Pedido ao negócio:** a mesma pergunta que já foi respondida na reumatologia — as colunas de
trabalho ficam ou saem? Se a resposta for a mesma, é uma edição mecânica.

## 1.3 ✅ O que já está certo

`descriptografia` já declara os cinco campos: `nome_paciente`, `cpf_paciente`,
`telefone_paciente`, `medico_solicitante`, `crm_solicitante`.

---

# 2. Config da especialidade

## 2.1 🔴 `gold_filter` não captura punção

```python
'gold_filter': {'keywords': ['tireoide', 'tireóide', 'pescoco', 'pescoço'], 'mode': 'any'}
```

Nesta plataforma o filtro é `proced_descricao rlike '(?i)<kw>'` — casa contra a **descrição do
procedimento**, não contra o laudo. A descrição de uma punção aspirativa por agulha fina guiada por
ultrassonografia **não contém nenhuma das quatro palavras**.

**Efeito medido:** **67 exames citando TI-RADS 4 em 16 dias nunca chegam ao motor** — contra 36 que
o gate rebaixava. É a causa **maior** do caso do negócio.

**Ajuste:** acrescentar a palavra-chave da punção.
⚠️ **Só sobe com medição:** o filtro de entrada é invisível ao A/B local — mede-se rodando, contando
o volume adicional e quantos dos 67 passam a entregar.

✅ **É nossa alçada** — `gold_filter` vive no config da especialidade, quadro *"você edita"* da
Figura 2 do POP-IA-08. Segue por PR de config, sem depender de ninguém.

## 2.2 🔴 `embedding_model` aponta para o workspace antigo

```python
'embedding_model': '/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/st_models/paraphrase-multilingual-MiniLM-L12-v2'
```

Caminho **literal e idêntico nos três ambientes**, para o volume do workspace antigo. Em produção
não existe: em 10/09, **1.348 dos 1.568 laudos (86,0%)** caíram em `token_overlap` com
`FileNotFoundError`. A config declara `decision_mode: hybrid` e o que executa é outra coisa.

**Ajuste:** resolver o caminho **por ambiente** — mesma classe do `base_url` do LLM, já resolvida no
PR 7135. Card `298600`.
🔴 **Depende de terceiro:** o schema `nlp_engine` não existe em `gold_fabrica_ia` (produção).

## 2.3 🟡 Três blocos mortos

| bloco | linha | por quê sai |
|---|---|---|
| `catalog: 'diamond_tirads'` | 615 | não é lido; o catálogo vem do `EnvironmentConfig`. **O próprio comentário do arquivo diz que a plataforma nem acessa esse catálogo** |
| `monitoring` | 690 | nenhuma chave é lida, e a `metrics_table` declarada não é a tabela usada |
| `distribution` | 693 | `outbound_volume`, `outbox` e `inbox` só aparecem dentro da própria config; `{catalog}` e `{run_id}` ficam literais |

**Ajuste:** remover os três. É a régua de config já aplicada ao `cancer_rim` e pedida no PR 7191.

## 2.4 🟡 `waive` para PAAF — pronto, aguardando aval

Config **`0.9.0-tirads`** já escrita, na branch `tirads/feature/waive-paaf` (`f8a16d7`), com cópia
em `_versoes-estaveis/`. **O PR não foi aberto**: a chave `waive` é nova e a saída ganha
`gate_waived_by` e `gate_waived_error` — mudança de contrato, que o Ops avaliza antes.

**Medido, duas vezes por caminhos independentes, mesmo número:** 3 promovidos, 0 rebaixados, 28
dispensas aplicadas. Inerte enquanto nenhuma config declarar a chave.

## 2.5 ✅ O que NÃO muda

**O juiz LLM desligado é deliberado e está documentado no arquivo, com número.** Medido no run de
18/08 (13.194 laudos): chamado 3.786 vezes, em 3.769 apenas confirmou negativo, e nas 17 restantes
**promoveu laudos fora do escopo** — 14 sem categoria, 2 com TR1, 1 com TR3.

O bloco `nlp.llm_router` fica intacto; religar é trocar um `False` por `True` no `runtime`.
**Não mexer sem medição nova.**

---

# 3. O que dá para juntar na história das colunas

A história de ajuste das colunas do arquivo de navegação já existe. **Cabem nela, sem abrir nada
novo:**

- ✅ **1.1** — trocar o mapeamento da coluna "Achado" de `classificacao_rads` para `findings`
- ✅ **1.2**, as 27 colunas manuais — mesma decisão de negócio da reumatologia
- ✅ é um PR só, nos três ambientes

**Não cabem, e por quê:**

| item | por quê fica fora |
|---|---|
| **2.1 `gold_filter`** | muda o que entra no motor; exige run medido antes, e o PR carrega a medição |
| **2.2 `embedding_model`** | bloqueado por schema em produção |
| **2.4 `waive`** | bloqueado pelo aval de contrato |
| **2.3 blocos mortos** | poderia ir junto, mas **misturar limpeza com mudança de entrega confunde a revisão**. Melhor um PR de higiene separado, que é aprovação rápida |

---

# 4. Ordem sugerida

1. **Perguntar ao negócio** — as 27 colunas saem? E o que ele espera ver em "Achado"?
2. **Cruzar o caso reportado** com a coluna nula, antes de atribuir tudo ao gate.
3. **PR das colunas** (história existente) — 3 ambientes, adição e remoção puras.
4. **PR de higiene da config** — remover os três blocos mortos.
5. **`gold_filter` da punção** — medir e abrir PR com o número.
6. Aguardando terceiros: `waive` (aval) e `embedding_model` (schema).
