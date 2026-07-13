# Decisões — Expansão xxRADS (PI-RADS / TI-RADS) — v0

**Data:** 2026-06-23 · Contexto: expansão do motor para próstata (PI-RADS) e tireoide (TI-RADS), bancada A/B vs referência do lake (`diamond_pirads`/`diamond_tirads`, classificador independente).

---

## 1. Categoria TR6 (TI-RADS) — VÁLIDA

`TI-RADS 6` **existe** na Rede D'Or = paciente com câncer **confirmado por biópsia** (análogo ao BI-RADS 6). 
- ✅ **Aplicado:** `TR6` adicionado às `categories` e a `promote_categories` do config tireoide (promove relevância). Captura o "6" explícito no laudo.
- 📌 **Pendente (léxico):** quando o laudo **não** escreve "TI-RADS 6" mas há biópsia/malignidade confirmada, o TR6 é **achado clínico**, não extração RADS. Deve vir do léxico de *findings* (ex.: "carcinoma confirmado", "biópsia compatível") promovendo `fl_relevante` — **não** se força o extrator RADS a inferir. Item de enriquecimento de léxico do tireoide.

## 2. Promoção da categoria 3 (zona cinza) — CONFIGURÁVEL POR ESPECIALIDADE

PI-RADS 3 / TI-RADS 3 são "zona cinza".
- **Default atual:** 3 **não** promove relevância (só 4/5/6).
- **Política:** é **decisão pontual de cada especialidade** — alguns casos 3 podem não promover mas ainda ser necessários ao time de navegação. Knob: `relevance_policy.promote_categories` por config. Ajustar conforme alinhamento clínico de cada especialidade.

## 3. Categoria 0 — NÃO é anomalia

`pirads=0`/`tirads=0` na referência = **termo RADS presente, sem número acionável** (ex.: "categorização PI-RADS [sem número]", "optado pela não aplicação da classificação", "Categoria T:"). Idêntico ao BI-RADS 0 do legado (keyword sem número → 0). Significa, na linguagem do laudador, **"não relevante"**.
- ✅ **Aplicado (semântica da bancada):** categoria acionável = **1–6**. `0` e `-1` tratados como "sem categoria acionável / não relevante" — fora do exact-match de categoria (entram só como presença/relevância). O motor retorna `-1` nesses casos (sem achado, sem categoria promovível) e **concorda na relevância** (não relevante).
- 📌 **Opcional (futuro):** replicar literalmente o valor "0" do legado exigiria um modo determinístico `alias_without_category → 0` no extrator (hoje esse caso é o gatilho do LLM fallback). Não necessário para paridade de relevância.

---

## Estado da bancada (2026-06-23)

| Sistema | Exact-match categoria (1–6) | Padrão dos desacordos |
|---|---|---|
| PI-RADS | 98,99% (repr) / 99,46% (strat) | super-agregação de **legenda de categorias**; ref com `0` não-acionável |
| TI-RADS | 96,48% (repr) / 97,06% (strat) | super-agregação de **tabela de pontos/legenda ACR**; lista "TI-RADS 3 e 4" |

**Erro dominante do motor (PI/TI):** super-agregação a partir de **legendas/tabelas de categorias** embutidas no laudo (mais frequente que no BI-RADS). Prioridade nº 1 de evolução — ver `draft-llm-desambiguacao-categoria-rads-v0.md` e a discussão de scoping por conclusão (`Relatorio-homologacao-birads-bancada-v1.md` §5/§6).
