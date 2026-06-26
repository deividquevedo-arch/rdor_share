# Homologação manual TI-RADS — conclusão parcial (2026-06-25)

> **Atualização 2026-06-26:** homologação completa das **12 divergências consolidadas** (BI/PI/TI)
> + escala oficial do DS + Fix D (romano). Ver seção "★ Homologação completa" ao final.

Revisão manual das divergências TI-RADS extraídas do `divergencias_consolidado.csv`
(motor v3 — com fix ™ + filtro de legenda — vs classificador do lake). Foco nos
`FN_vs_ref` (motor não marcou relevante; referência marcou). Coluna `homologação`
preenchida pelo revisor: **`legado`** = referência correta, motor falhou;
**`errados`** = ambos falharam.

## 1. Tabela de métricas — original × ajustada pela HML × projetada

| Fatia / visão | exact-match | TP / TN / FP / **FN** | match relevância |
|---|---|---|---|
| **TI-RADS repr** (n=1000) | | | |
| Original (vs lake-ref) | 0,989 | 70 / 926 / 1 / **3** | 0,996 |
| Ajustado (ref corrigida) | 0,989 | 70 / 926 / 1 / **3** | 0,996 |
| Projetado (pós fix A/B/C) | **0,9927** | 73 / 926 / 1 / **0** | **0,999** |
| **TI-RADS strat** (n=290) | | | |
| Original (vs lake-ref) | 0,9801 | 83 / 200 / 0 / **7** | 0,9759 |
| Ajustado (ref corrigida) | 0,9801 | 83 / **202** / 0 / **5** | **0,9828** |
| Projetado (pós fix A/B/C) | **0,9853** | 87 / 202 / 0 / **1** | **0,9966** |

- **Ajustado** = corrige os casos em que a *referência* errou (bucket D: super-agregou a legenda ACR; verdade = TR1). Na strat, 2 "FN do motor" viram acertos do motor (FN 7→5; o motor estava certo na relevância). repr não teve erro de referência nos casos revisados.
- **Projetado** = ajuste + fixes determinísticos A/B/C (motor passa a captar a verdade). O 1 FN residual da strat é o único `categoria+FN` ainda não revisado manualmente.
- As divergências do TI-RADS são **dominadas por FN** (recall), não por FP — o filtro de legenda já zerou/quase-zerou os FP. A revisão manual abaixo explica **todos** esses FN.

## 2. Achados por causa-raiz

| # | Causa-raiz | Exames | Veredito | ref | motor |
|---|---|---|---|---|---|
| A | **Negação falso-positivo** — `"sem calcificações (TIRADS® 4)"` / `"(TIRADS® 1), sem fluxo"`: o "sem" (referente a calcificações/fluxo) nega indevidamente o grau RADS | `OBSTASYHSL…0681/0682` (N1+N2 do mesmo exame; aparece em repr e strat) | **motor errou** (ref certa) | 4 | −1 |
| B | **Composto "N e M"** — `"nódulos TI-RADS 3 e 4"`: motor captura só o primeiro (3), não agrega o 4 | `OBSWPDHEO…3158` (repr e strat) | **motor errou** (ref certa) | 4 | 3 |
| C | **Palavra "Categoria" entre alias e número** — `"ACR TI RADS: Categoria 5 - Chammas 2"`: o token "Categoria" fica entre o alias e o dígito e o pattern não tolera → não casa | `OBSWPDHMEMO…7241` | **motor errou** (ref certa) | 5 | −1 |
| D | **Algarismo romano + referência super-agregando** — achado real é `"(TIRADS I)"` (=TR1, cisto benigno); motor não capta romano (`roman_to_arabic` off) e a **referência marcou 5** (super-agregou a legenda ACR `TI-RADS I…V`) | `OBSWPDCMREAL…9577/9578` | **ambos erraram** (verdade = TR1) | 5 | −1 |

## 3. Leitura cruzada (revisão × métricas)

- **Os FN do TI-RADS não são ruído aleatório — são 4 padrões determinísticos e nomeáveis.** Três (A, B, C) são **defeitos reais do motor** corrigíveis por regra/regex; um (D) é **erro da referência somado a um gap do motor**.
- **A referência do lake não é ground-truth confiável para grau alto:** no caso D ela marcou TI-RADS 5 a partir da **tabela-legenda ACR** (mesma super-agregação que corrigimos no motor). Logo, parte das "divergências FN" penaliza o motor contra uma referência que está, ela própria, errada.
- **Impacto nas métricas:** o exact-match de categoria (0,989 / 0,980) e o recall reportado **subestimam** a qualidade real do motor — após corrigir A/B/C o recall sobe, e o caso D deixa de ser "erro do motor vs referência correta" para virar "ambos divergem da verdade clínica".

## 4. Conclusão parcial

1. **A homologação manual valida a direção da v3** (fix ™ + filtro de legenda): os FP de super-agregação foram eliminados e as divergências remanescentes do TI-RADS se reduzem a **4 causas-raiz claras**, não a falhas difusas.
2. **3 gaps de recall determinísticos e corrigíveis** foram confirmados, em ordem de clareza:
   - **(A) Negação falso-positivo** — restringir a negação para **não negar grau RADS** (o grau é asserção; "sem calcificações/fluxo" descreve o nódulo, não nega a categoria). *Maior bucket de FN.*
   - **(C) Palavra "Categoria"** — tolerar `Categoria`/`Cat` entre o alias e o número no pattern (análogo ao grupo de modalidade US/USG/ECO).
   - **(B) Composto "N e M"** — capturar o segundo número em enumerações `"3 e 4"` para a agregação `max`.
3. **(D) Romano + referência falha:** o motor deveria captar `TIRADS I→V` (ligar `roman_to_arabic` no tireoide, como já é no BI-RADS), **mas** a referência também precisa de correção — isto reforça que a **homologação clínica manual é necessária** e que a concordância com o lake não pode ser a única métrica.
4. **Recomendação de processo:** tratar A, B, C como o próximo lote de fixes determinísticos (mesma abordagem controlada dos fixes ™/legenda: validar na bancada A/B + teste sintético, sem regredir baseline). D fica condicionado a decisão clínica/correção da referência.

## 5. Pendências / próximos passos
- Implementar fixes **A (negação), C (Categoria), B (composto)** — candidatos a "Fix C/D/E" no mesmo fluxo da bancada.
- Reavaliar A/B do TI-RADS após os fixes (esperado: FN repr 3→~1, strat 7→~2-3, restando o caso D).
- Estender a revisão manual a **PI-RADS** (poucos FN) e à amostra **BI-RADS** (261 FP_vs_legado — diferença de definição de relevância, já caracterizada).
- TR6 por biópsia segue **em aberto** (sem definição de negócio).

---

## ★ Homologação completa das 12 divergências consolidadas (2026-06-26)

### Escala oficial xxRADS (informada pelo DS que desenvolveu o TI-RADS)
| cat | significado |
|---|---|
| -1 | não localizado menção de RADS |
| 0 | inconclusivo (precisa exame complementar) |
| 1 | negativo (nada) |
| 2 | achado benigno |
| 3 | provavelmente benigno (acompanhamento curto) |
| 4 | suspeito (recomenda biópsia) |
| 5 | altamente suspeito de malignidade |
| 6 | malignidade comprovada por biópsia (paciente já em tratamento) |

**Regra TR6:** só vale se aparecer **escrito no laudo** (`xxRADS 6`). Não se infere de "biópsia".

### Veredito das 12 divergências (validação manual)
| # | Sistema | ref | motor | veredito | causa |
|---|---|---|---|---|---|
| 1 | BI-RADS | 5 | 2 | **motor correto** | legado pegou "5" de *"5th ed."* (edição bibliográfica) |
| 2 | PI-RADS | 2 | 2 | igual | categoria igual (relevância: ver observação) |
| 3 | PI-RADS | 2 | 1 | **motor correto** | legado pegou "2" de *"PI-RADS v2.1"* (versão) |
| 4 | PI-RADS | 5 | 2 | **motor correto** | legado super-agregou a legenda 1..5 |
| 5 | PI-RADS | 5 | 2 | **motor correto** | idem #4 (mesmo exame) |
| 6 | TI-RADS | 3 | 2 | **motor correto** | legado pegou "3" de dimensão (cm³) |
| 7 | TI-RADS | -1 | 4 | **motor correto** | legado falhou; motor capturou `ACR-TIRADS™: 4` (fix ™+C) |
| 8 | TI-RADS | 5 | -1→**TR1** | **corrigido (Fix D)** | romano `(TIRADS I)`; legado pegou legenda |
| 9 | TI-RADS | 5 | -1→**TR1** | **corrigido (Fix D)** | idem #8 |
| 10 | TI-RADS | 3 | 2 | **motor correto** | idem #6 |
| 11 | TI-RADS | 6 | 5 | **motor correto** | legado pegou "6" de dimensão; sem "RADS 6" escrito (regra TR6) |
| 12 | TI-RADS | 6 | 2 | **motor correto** | laudo só tem TR1/TR2; legado indicou 6 sem "6" escrito |

### Conclusões
1. **O motor está CORRETO em 11 das 12 divergências** (9 desde o início + 2 corrigidos pelo Fix D).
   A única ressalva é #2 (relevância, ver observação).
2. **A referência do lake é sistematicamente não-confiável:** captura **números que não são categoria**
   — edição (`5th ed.`), versão (`v2.1`), dimensões (`cm³`) — e super-agrega legendas. As métricas
   "vs lake" **subestimam** a acurácia real do motor; parte das "divergências" é erro DA REFERÊNCIA.
3. **Regra TR6 validada no motor:** o motor só extrai 6 quando "RADS 6" está escrito (#11/#12 o motor
   acertou 5/2; o legado inferiu 6 indevidamente).
4. **Fix D (romano):** TI-RADS passa a capturar `I..VI` (isonômico com BI-RADS); a legenda romana
   `I..V` é barrada pelo `aggregation_legend_filter`. Determinístico, sem regressão real.

### Observação (decisão pendente) — caso #2 (relevância PI-RADS)
`(PI-RADS 2)` com `fl_relevante=1`: a relevância foi disparada pelo léxico de **achado clínico**
("nódulo prostático" na indicação), não pela categoria. Pela escala, cat 2 = benigno = **não
relevante**. Para **isonomia com o BI-RADS** (achados desativados → relevância só por cat≥4),
o PI/TI poderiam adotar o mesmo critério, o que zeraria esse FP. **Pendente de decisão** (não
aplicado — o revisor marcou #2 como "igual").
