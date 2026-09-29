# Defeitos abertos — organização, vínculos e roadmap de fecho

> **19 defeitos abertos** no projeto `IA`, lidos do board em **15/09/2026**. Sete são da Central de
> Captação e pertencem a outra frente; **doze** são nossos ou tocam o motor e a plataforma.
>
> Para cada um: **o que fecha**, **o que falta** e **o que bloqueia**. Onde o card não tem critério
> de aceite escrito, a proposta vem marcada *(proposto)* — e precisa ser gravada no card.

---

# 1. Prioridade — a ordem de ataque

| # | card | título | por que nesta posição |
|---|---|---|---|
| 1 | `300200` | [NLP Engine] Âncora ausente sai do gate | **entregue** — só falta mover |
| 2 | `299525` | Erro ao baixar o modelo do HuggingFace | **P1, cinco linhas paradas desde 03/09** |
| 3 | `305810` | [Plataforma NLP] Modelo de embeddings sem caminho válido em produção | quatro linhas em perfil não homologado |
| 4 | `283644` | [NLP Engine] Juiz LLM ligado por contorno não documentado | **P1, 25 dias sem medição** |
| 5 | `285305` | [NLP Engine] TI-RADS entrega TR falso | **P1** — metade entregue, precisa ser partido |
| 6 | `281894` | [Transplante de Pulmão] Critério não aplicado a pacientes pediátricos | **P2, 27 dias sem dono** — defeito de régua nossa |
| 7 | `300202` | [NLP Engine] Hepatologia descarta 86% na segmentação | P2 sem dono, exige A/B |
| 8 | `298275` | [Rim] Sem envios para o hospital SAMER | sem dono, sem prioridade, hipótese formulada |
| 9 | `298600` | [NLP Engine] Ajustar embedding_model nas configs | **bloqueado** pelo `305810` |
| 10 | `300201` | [Plataforma NLP] Texto de entrada duplicado 2n+1 vezes | plataforma — cruzar com o bug 2 do POP-IA-08 |
| 11 | `299238` | [Plataforma NLP] SPEC 27 contradiz o código | plataforma, sem dono — destrava o PR 7228 |
| 12 | `298596` | [Plataforma NLP] `limit_rows` não limita a fila | plataforma, com dono |
| — | `140612` | Exames ausentes e pacientes duplicados | **244 dias** — provavelmente já resolvido |

---

# 2. Nossos — o que fecha cada um

## `300200` — Âncora ausente sai do gate · **Desenvolvido**

**O que fecha:** A/B com 4 rebaixados `1→0`, zero promovidos, 4 de 4 com
`require_measure_no_anchor`, pré-condição de 306 laudos exercitando o caminho.
✅ **Atendido.** Entregue na `0.12.2`, tagueada e publicada nos dois feeds.
**O que falta:** **mover no board.** Nada técnico.

## `283644` — Juiz LLM ligado por contorno · **P1, 25 dias**

**O que é:** hepatologia e transplante_pulmao em **produção** com o juiz ligado por contorno —
`nlp.llm_router` não declara `enabled`, e a lib assume `False` na ausência.
**O que falta:** *(proposto)* `enabled` declarado explicitamente nas duas configs **e medição do
delta de decisão** ao tornar explícito — a mudança pode alterar entrega, e sem medir não se arbitra.
**Bloqueio:** nenhum. É trabalho nosso, parado por fila.
🔴 **P1 aberto há 25 dias sem medição registrada** é o item mais antigo da nossa fila.

## `285305` — TI-RADS entrega TR falso · **P1** · 🔴 **partir em dois**

**Carrega dois defeitos, e por isso não fecha:**

| | estado |
|---|---|
| **1 — legenda ACR não filtrada** | ✅ corrigido na `0.10.1`, medido por A/B: 156 → 133 entregas, 23 removidas, zero acrescentadas, 23 de 23 mencionam ACR e PAAF |
| **2 — medida associada ao nódulo errado** | 🔴 aberto. `gate_mets` é por critério, não por menção: não existe vínculo lesão↔medida. É a `0.15.0` |

**Proposta:** fechar o atual com a evidência do defeito 1; abrir um novo para o vínculo
lesão↔medida, alocado na `0.15.0` e vinculado ao `298598` (plano de bumps).

## `298600` — Ajustar `embedding_model` nas configs · **bloqueado**

**O que fecha:** configs sem caminho literal de volume, e run de produção das quatro linhas com
zero `FileNotFoundError`, com a pré-condição impressa. *(gravado no card em 15/09)*
🔴 **Bloqueado pelo `305810`** — enquanto o mecanismo não for definido pela plataforma, não existe
caminho correto a escrever. Vínculo *related* já criado.

## `300202` — Hepatologia descarta 86% na segmentação · **sem dono**

**O que é:** `segmentation_coverage < 1,0` em **3.867 de 4.507** laudos, com 3.196 cabeçalhos
descartados. Única linha em `mode: auto`. No ca-rim a mesma correção recuperou +25 laudos em 6 dias.
**O que falta:** *(proposto)* A/B medindo o delta de decisão ao trocar para `full_doc`, na mesma
janela, com a pré-condição impressa.
⚠️ **Não trocar antes de medir:** sem gabarito clínico não há como arbitrar o delta, e a mudança
só aumenta entrega.
**Ação de board:** **atribuir dono** — é nosso e está órfão.

## `281894` — Transplante de Pulmão, critério não aplicado a pediátricos · **27 dias sem dono**

**O que é:** em `Transplante pulmão_Unidades_SP_2026_08_18.xlsx`, paciente < 18 anos em que o
critério de doença supurativa não foi aplicado — deveria ter achado de doença supurativa **e**
doença intersticial.
**Critério de aceite no card:** *"aplicação da regra corrigida"* — 🔴 **é vago e não fecha**.
**O que falta:** *(proposto)* reproduzir o caso pelo `id_exame`, identificar se a régua não casou ou
se o laudo não chegou ao motor, e medir o efeito da correção na janela que contém a população.
⚠️ **Contexto que muda a leitura:** a linha entrega **zero** em produção — toda a relevância vem de
`on_met: promote` nos critérios quantitativos, que dependem do LLM. Antes de tratar como defeito de
régua, confirmar se o laudo chegou ao motor.
**Ação de board:** atribuir dono e escrever o aceite.

## `298275` — [Rim] Sem envios para o hospital SAMER · **sem dono, sem prioridade**

**O que é:** o hospital não aparece com envios nos últimos 3 meses no algoritmo de Rim.
**Hipótese, não verificada:** é **filtro de entrega**, não régua — `gold_filter` na entrada, ou lista
branca de `id_unidade` no bloco `validacao` do arquivo de navegação.
**O que falta:** *(proposto)* localizar o `id_unidade` do SAMER nas três camadas — entrada, saída e
view de exportação — e dizer em qual delas ele some. É a mesma investigação que já resolveu casos
equivalentes.
**Ação de board:** atribuir, priorizar e escrever o aceite.

---

# 3. Plataforma — o que está pedido

| card | título | o que se pede | estado |
|---|---|---|---|
| `305810` | [Plataforma NLP] Modelo de embeddings sem caminho válido em produção | definir e implementar o mecanismo de caminho por ambiente | **criado em 15/09**, sem dono |
| `299238` | [Plataforma NLP] SPEC 27 contradiz o código | atribuir dono; destrava o PR `7228`, hoje `-10` | Novo, **sem dono há 12 dias** |
| `300201` | [Plataforma NLP] Texto de entrada duplicado 2n+1 vezes | ⚠️ **cruzar com o bug 2 do POP-IA-08** (dedup fixa em hepatologia/dev) antes de tratar como card novo | Novo, sem dono |
| `298596` | [Plataforma NLP] `limit_rows` não limita a fila | em refinamento com dono | Em Refinamento |
| `299525` | Erro ao baixar o modelo do HuggingFace | mapear os modelos por algoritmo e aplicar a correção do `298141` | **Desenvolvido** — confirmar se as cinco linhas voltaram |

🔴 **`299525` e `305810` são o mesmo tema por dois caminhos:** modelo indisponível em execução. O
primeiro é o legado baixando do HuggingFace; o segundo é a plataforma nova lendo de Volume.
**Vincular os dois** — quem resolver um vai querer ver o outro.

---

# 4. Vínculos a criar

| de | para | tipo | por quê |
|---|---|---|---|
| `298600` | `305810` | related | ✅ **já criado** |
| `299525` | `305810` | related | mesmo tema — modelo indisponível em execução |
| `285305` (novo, defeito 2) | `298598` | related | o vínculo lesão↔medida é a `0.15.0` |
| `283644` | `298598` | related | entra no ciclo de bumps |
| `300201` | — | comentário | registrar o cruzamento com o bug 2 do POP-IA-08 |

---

# 5. Roadmap — defeito × versão

| versão | defeitos que fecha | estado |
|---|---|---|
| **entregue** | `300200` (`0.12.2`) · `285305` defeito 1 (`0.10.1`) | ✅ falta mover no board |
| **`0.13.0`** | nenhum defeito — é estrutura | 🟡 2 de 6 cards, branch sem PR |
| **`0.14.0`** | `283648` [P0-29] juiz sem evidência · contabilidade de tokens | 🔴 não iniciado |
| **`0.15.0`** | `285305` defeito 2 — vínculo lesão↔medida | 🔴 não iniciado |
| **config, sem bump** | `300202` segmentação · `281894` pediátricos · `298275` SAMER · `gold_filter` da punção | 🟡 nossa alçada, exige medição |
| **bloqueado por terceiro** | `298600` embeddings | 🔴 espera `305810` |

⚠️ **Nenhum defeito depende da `0.13.0`.** Se a prioridade é fechar defeito de decisão clínica, a
`0.14.0` e a `0.15.0` vêm antes do que sobra da `0.13.0` — que é higiene interna.

---

# 6. Ações de board, em ordem

1. **Mover `300200`** — entregue e validado.
2. **Partir `285305`** — fechar o atual com o defeito 1; abrir o defeito 2 na `0.15.0`.
3. **Atribuir dono** a `300202`, `281894` e `298275` — três defeitos nossos, órfãos.
4. **Escrever critério de aceite** em `283644`, `300202`, `281894` e `298275` — hoje nenhum fecha.
5. **Criar os vínculos** da §4.
6. **Comentar no `300201`** o cruzamento com o bug 2 do POP-IA-08.
7. **Confirmar o `299525`** — está em *Desenvolvido*; verificar se as cinco linhas voltaram a rodar.
8. **Triar o `140612`** — 244 dias; a dedup da view foi corrigida no PR 7075, provavelmente já morreu.

---

# 7. O que fica de fora, por decisão

**`283647` — Declarar contrato de entrada e saída entre a lib e a plataforma.** O contrato está
definido na prática; ajustar agora produz acordo provisório. Reabrir **depois** de fechar os
defeitos e o ciclo de bumps, quando a intenção de evolução já estiver conhecida e o fecho puder ser
proposto com base nela.
