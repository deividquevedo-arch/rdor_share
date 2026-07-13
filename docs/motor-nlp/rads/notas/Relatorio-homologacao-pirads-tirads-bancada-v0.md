# Relatório de Homologação — Motor NLP vs Referência do Lake — PI-RADS / TI-RADS

**Motor:** `nlp-engine 0.1.1` · **configs:** `prostata` (PI-RADS) e `tireoide` (TI-RADS), patterns evoluídos do BI-RADS.
**Data:** 2026-06-23 · **Estágio:** Bancada A/B local · revisão clínica formal pendente.

---

## 1. Resumo executivo

> Expansão do motor para **PI-RADS** (próstata) e **TI-RADS** (tireoide). Comparação restrita à **extração de categoria** contra a **referência do lake** (`diamond_pirads`/`diamond_tirads`) — que é um **classificador independente** (não é o nosso motor: 0/53.426 laudos têm assinatura do nlp-engine). Onde ambos extraem categoria acionável (1–6), o motor concorda **~99% (PI-RADS)** e **~96–97% (TI-RADS)**. O erro dominante do motor é **super-agregação de legendas/tabelas de categorias** embutidas nos laudos.

| Pergunta | PI-RADS | TI-RADS |
|---|---|---|
| Concordância de categoria (1–6) | **98,99% / 99,46%** (repr/strat) | **96,48% / 97,06%** |
| Relevância (≥4) — MCC vs referência | **0,968 / 0,973** | **0,926 / 0,911** |
| Erro próprio do motor | super-agregação de legenda | super-agregação + lista "3 e 4" |

---

## 1.1 Método e limitações

- **Tarefa:** categoria xxRADS (inteiro; TI: TRn → n). **Acionável = 1–6.**
- **Referência = classificador independente do lake** (coluna `pirads`/`tirads`), **não verdade clínica** → métricas medem *concordância*, não *correção*. Onde divergem, foi feita adjudicação manual pela conclusão do laudo.
- **`0`/`-1` = "sem categoria acionável / não relevante"** (termo sem número, ou classificação não aplicada) — fora do exact-match de categoria; entram só na relevância. Ver `decisoes-expansao-rads-pi-ti-v0.md`.
- **Pares comparáveis baixos** (PI 397/1000; TI 199/1000) porque a referência tem muitos `-1` (sem categoria). A divergência de *presença* é dimensão de escopo (como no BI-RADS), separada da acurácia de categoria.
- Sem revisão clínica formal ainda.

---

## 2. Métricas — concordância de categoria (acionável 1–6)

| Cenário | Exact-match | Pares comparáveis | Divergências (cat+relev) |
|---|---|---|---|
| PI-RADS representativa (1000) | **98,99%** | 397 | 7 |
| PI-RADS estratificada (259) | **99,46%** | 184 | 3 |
| TI-RADS representativa (1000) | **96,48%** | 199 | 11 |
| TI-RADS estratificada (290) | **97,06%** | 170 | 13 |

## 3. Métricas — relevância "≥4" (motor `fl_relevante` vs referência ≥4)

| Métrica | PI-RADS repr | PI-RADS strat | TI-RADS repr | TI-RADS strat |
|---|---|---|---|---|
| TP / TN / FP / FN | 102/892/4/2 | 78/178/1/2 | 68/922/5/5 | 80/199/1/10 |
| Precision | 0,962 | 0,987 | 0,932 | 0,988 |
| Recall | 0,981 | 0,975 | 0,932 | 0,889 |
| Specificity | 0,996 | 0,994 | 0,995 | 0,995 |
| Accuracy | 0,994 | 0,988 | 0,990 | 0,962 |
| F1 | 0,972 | 0,981 | 0,932 | 0,936 |
| F2 | 0,977 | 0,977 | 0,932 | 0,907 |
| MCC | 0,968 | 0,973 | 0,926 | 0,911 |

> **TI-RADS strat recall 0,889 (10 FN):** parte são **TR6 inferido de biópsia** (referência marca 6 sem "TI-RADS 6" escrito; o motor só lê o RADS explícito — TR6 inferido é **achado clínico**, item de léxico) e parte super-agregação. Não são erros de extração.

---

## 4. Adjudicação dos desacordos de categoria (estratificada)

| Sistema | ref/motor | Laudo | Veredicto |
|---|---|---|---|
| PI-RADS | 2 / 5 | Conclusão "PI-RADS 2"; motor pegou "5" da **legenda** | Ref melhor — **super-agregação** |
| TI-RADS | 3 / 2 | "Nódulo TI-RADS 2 no lobo direito" | **Motor melhor** (ref errou) |
| TI-RADS | 2 / 5 | Tabela de pontos/legenda ACR | Ref melhor — **super-agregação** |
| TI-RADS | 4 / 3 | "nódulos TI-RADS **3 e 4**" | Ref melhor — motor pegou só o 3 (**lista 1 alias**) |
| TI-RADS | 6 / 5 e 6 / 2 | TR6 por biópsia (sem "6" escrito) | **TR6 inferido** — fora do escopo de imagem |

> Os CSVs `bancada/divergencias_{pirads,tirads}_{repr,strat}.csv` (com laudo completo, Excel-safe) estão prontos para **avaliação manual do time clínico**.

---

## 5. Erro dominante — super-agregação de legendas/tabelas

Laudos de PI/TI-RADS frequentemente **embutem a escala completa** (descrição de cada categoria 1–5, ou a tabela de pontos ACR). A política `max_category` do motor pega o maior número da legenda, não a categoria do paciente. **É o mesmo problema de super-agregação do BI-RADS, porém mais frequente aqui.**
- **Prioridade nº 1 de evolução** (transversal BI/PI/TI). Duas abordagens já testadas no BI-RADS regrediram (exclusão por contexto; scoping por conclusão) — ver `Relatorio-homologacao-birads-bancada-v1.md` §5.
- Alternativa promissora: **LLM de desambiguação** gatilhado só em ambiguidade, em shadow → `draft-llm-desambiguacao-categoria-rads-v0.md`.

---

## 6. Decisões clínicas incorporadas (ver `decisoes-expansao-rads-pi-ti-v0.md`)

- **TR6 válido** (câncer confirmado por biópsia) → no config; inferência por biópsia = léxico de findings.
- **Cat 3** não promove (default); configurável por especialidade (navegação decide).
- **Cat 0** = "termo sem número / não relevante" (não é anomalia).

---

## 7. Pendências

| # | Ação | Tipo |
|---|---|---|
| 1 | **Avaliação manual** dos CSVs de divergência (clínico) | Clínico |
| 2 | **Super-agregação** (legendas/tabelas) — solução transversal | Técnico (motor) |
| 3 | **Léxico de findings** do tireoide p/ TR6 inferido por biópsia | Léxico |
| 4 | Política de promoção da cat 3 por especialidade | Clínico/produto |
| 5 | Gerar configs `.py` do runner p/ rodar E2E em HML (hoje só YAML) | Técnico |

---

## 8. Recomendação

> Na extração de categoria, o motor concorda fortemente com um classificador **independente** (~99% PI-RADS, ~96–97% TI-RADS) e, nos desacordos, divide-se entre acertos do motor, super-agregação e TR6-por-biópsia (fora de escopo de imagem). A expansão xxRADS **generaliza bem** via config. **Antes de avançar para HML:** avaliação clínica dos CSVs + tratamento da super-agregação (item transversal). A referência do lake **não é verdade clínica** — sign-off depende do time médico.

---

*Fonte: `nlp-engine-lib/bancada/run_ab_rads.py`, amostras `sample_{pirads,tirads}_{repr,strat}.jsonl` (lake, gitignored). Decisões em `decisoes-expansao-rads-pi-ti-v0.md`.*
