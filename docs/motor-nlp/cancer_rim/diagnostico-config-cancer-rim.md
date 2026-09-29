# Diagnóstico — config `cancer_rim` na plataforma nova

**Base:** `ntb_ia_cancer_rim_config.py`, branch `cancer_rim/feature/migracao-config-motor`,
`config_version: 0.4.0-cancer_rim` · **Data:** 2026-08-14 · **Para:** Leandro

> ⚠️ A config na branch é a **0.4.0**. A `0.5.0` com as 18 regras da Dra. Carol ainda não foi
> commitada ("falta commit + PR", resumo executivo §7). Onde este diagnóstico citar a v0.5, é do
> resumo — pode já estar resolvido localmente.

---

## 1. O que está bem feito

Registro porque é raro e vale manter em outras especialidades:

- **`segmentation.mode: full_doc` com a medida junto** (+25 laudos na janela 03–08/08). Foi a
  descoberta que virou a `0.8.3` da lib.
- **`findings_ignore_sections`** com a auditoria citada (~9 FPs vinham da indicação clínica).
- **`findings_skip_organ_gate`** para Bosniak/ccLS e sinais vasculares, com o trade-off escrito
  ("recall > precisão aqui").
- **Regex com *lookahead* de negação embutido** — `(?!(?:não|sem|nem)\b)` dentro do padrão.
- **O aviso sobre nunca terminar padrão com `|`** — alternativa vazia casa em toda posição e
  promove tudo. Vale para todas as especialidades.
- **Cada regra diz de onde veio** (task, auditoria ou Dra. Carol).

---

## 2. Corrigir — anula uma camada

### 2.1 `hybrid` declarado e desligado

```python
'use_embeddings': False,
'decision_mode': 'hybrid',        # nunca acontece sem embeddings
'embedding_model': 'PREENCHER',   # ⚠️ armadilha
```

`decision_mode: hybrid` sem embeddings é inerte. Pior: se alguém ligar `use_embeddings: True` com
`embedding_model: 'PREENCHER'`, **o fallback de embeddings é SILENCIOSO** — degrada sem erro e sem
log. É débito técnico registado na referência da lib (§6).

**O quê:** preencher o caminho real no Volume, ou deixar `decision_mode` coerente com o que roda.
**Verificar:** `semantic_backend` e `semantic_score` na saída — se vierem vazios, não rodou.

### 2.2 A banda não acompanha o cenário

`uncertainty_band: [0.35, 0.65]` é o default. O seu próprio comentário já diz o certo:

> *"religar só com llm_http + banda retunada (~[0.55,0.70]); sem embeddings deflaciona score
> 0.9 → ~0.58-0.68"*

Só que a config ficou com o default. **Se o teste do juiz rodou assim, ele foi injusto:** o juiz só
opinou sobre a fatia que caiu na banda — que não é a fatia onde a decisão é duvidosa.

No ca-estômago isso apareceu do outro lado: com embeddings ligados o score infla para 0,75–0,94, e a
banda precisou ir a `[0.35, 0.95]` para o juiz rodar onde importava.

**Verificar:** distribuição de `confidence_score` dos relevantes e a contagem de `llm_called`.

---

## 3. Alinhar — o prompt do juiz está atrás da régua

O `specialty_context` (na 0.4.0) diz:

> *"...ou sinais de extensão/invasão (ex.: invasão de gordura perirrenal, trombo tumoral na veia
> renal, **invasão venosa**, trombose de veia cava inferior)"* → **relevante = true**

E o resumo executivo diz que a v0.5 removeu isso: *"'invasão venosa' aparece em qualquer tumor —
remover"*. **O prompt manda o juiz promover o que a régua decidiu descartar.**

Sobre Bosniak, o prompt cobre `I/II` (benigno) e `III/IV/V` (relevante) e **não menciona IIF** — que
o resumo diz estar fora. O regex, por sua vez, **inclui `iif` como relevante**. Prompt, regex e régua
divergem entre si.

**Isto explica a conclusão registrada no resumo:** *"ele julga pela medicina geral e discorda das
políticas da linha"*. Não é o modelo julgando por conta própria — é o contexto que demos a ele,
desatualizado.

**O quê:** reescrever o `specialty_context` a partir das 18 regras da v0.5, item a item, e incluir a
política da linha ("na dúvida, capturar") — que hoje já está lá e é o único ponto alinhado.
**Verificar:** rodar o juiz sobre os casos que ele descartava e conferir se ainda descarta.

---

## 4. Aproveitar — o que resolveria o seu problema principal

### 4.1 Bosniak e ccLS deveriam ser `rads_extraction`, não regex ⭐

Hoje:

```python
'classificações oncológicas': [r"bosniak\s*:?\s*(categoria\s+)?(iif|iii|iv|v|3|4|5)\b", ...]
```

**Bosniak é um sistema ordinal, igual ao TI-RADS.** A lib tem camada própria para isso, com:

- normalização de romanos e variantes de grafia
- negação específica de categoria
- agregação `max_category` (laudo com duas lesões → vale a maior)
- promoção **por política** (`promote_categories: ['III','IV','V']`) — trocar IIF de lado vira uma
  linha, não um regex novo
- e o decisivo: **`rads_promotion` é protegida do juiz** desde a 0.8.2/0.8.4

O último item muda o jogo. Hoje o juiz pode derrubar um Bosniak IV; como `rads_extraction`, não pode.

### 4.2 `quantitative_criteria` — tamanho da lesão

Está comentado. Se tamanho entra na régua (nódulo sólido pequeno vs. grande), vira critério
determinístico — e **também fica protegido do juiz**.

**Por que os dois itens acima importam juntos:** a proteção da 0.8.4 cobre **medida e categoria**,
não achado léxico. Hoje toda a sua régua é léxica, então o juiz pode derrubar qualquer coisa — que é
exatamente o que você mediu (3 a 8 confirmados descartados). Migrando Bosniak/ccLS para ordinal e o
tamanho para quantitativo, **você pode ligar o juiz e ficar com os ~92% de precisão sem perder esses
casos**, porque ele deixa de poder mexer neles.

### 4.3 `findings_exclusion_terms` — descartar benigno deterministicamente

O comentário diz *"benigno explícito é descartado na validação"* — ou seja, depende do juiz ou de
revisão manual. `exclude: ['simples', 'típico']` com `unless: [...]` faz isso na regra, antes.

### 4.4 `emit_decision_trail: true` durante calibração

Mostra, por achado, o que disparou e o que foi descartado **com o motivo**. Desde a 0.8.3 inclui os
descartes pelo gate de órgão, que antes eram mudos.

---

## 5. Medir antes de mexer

| o quê | como | por quê |
|---|---|---|
| quanto o gate de órgão descarta | contar `n_organ_gate_spans` (0.8.3) | `finding_organ_max_chars: 220` com `skip_organ_gate` em só 2 dos 5 achados — pode estar perdendo |
| `finding_organ_max_chars` 220 → 120 | simular sobre o gabarito | no ca-estômago removeu um FP sem perder TP |
| cobertura da segmentação | `segmentation_coverage` | deve dar 1,0 com `full_doc` — confirma a correção |

---

## 6. Boa notícia sobre os bugs de plataforma

Os bugs **#1, #2 e #3** do resumo executivo são do **runner antigo**. Verifiquei na plataforma nova:
ela usa `apiUrl()`/`apiToken()` e trata explicitamente execução manual — **não quebra em job**.
Resolvem-se com a migração, sem precisar de correção.

O **#4** (segmentação descartando conclusão) virou a **`0.8.3`**: `segmentation_coverage` e
`segmentation_dropped_headers` aparecem quando parte do laudo não chega à régua, e a §4 da
referência ganhou o aviso com as duas saídas. O comportamento não mudou — trocar o `mode` muda
decisão e é escolha clínica — mas deixou de ser invisível, que era a sua queixa.

Vale ajustar a escalada de "aberto" para "observabilidade entregue na 0.8.3; falta decidir o default".

---

## 7. Ordem sugerida

1. **Prompt do juiz alinhado à v0.5** — é o que invalida a conclusão sobre o LLM
2. **Banda retunada** para o cenário real
3. **Bosniak/ccLS → `rads_extraction`** — maior ganho estrutural
4. Reavaliar o juiz com 1–3 aplicados; se recuperar os ~92% sem perder confirmado, ligar
5. `findings_exclusion_terms` e `quantitative_criteria`, se a régua pedir

Os passos 1 e 2 custam um run. O 3 custa um run e vale por si, mesmo com o juiz desligado.
