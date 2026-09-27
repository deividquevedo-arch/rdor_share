# DII — métricas legado × atual (config único v0.2.0)

> **Para que serve.** Responder ao pedido de 14/09: acurácia, recall, sensibilidade, precisão, F1 e
> MCC, comparando o algoritmo legado (job `ia-dii`) com o config único `v0.2.0`, em tabela de 3 colunas.
> Escrito em **2026-09-14**. Detalhe da medição em `dii-resultado-medicao.md` (§9.5 e §10).

---

## 1. A tabela

**Base:** 18.480 laudos do legado, 3 dias (21/08, 24/08, 31/08/2026) — 15.778 de imagem (TC/RM/
enterografia) + 2.702 de colonoscopia. **206 relevantes** na referência (§3). Lib `nlp-engine==0.12.3`
(a que `latest` resolve em produção). Config: `ntb_ia_doenca_inflamatoria_intestinal_config.py`,
branch `doenca_inflamatoria_intestinal/feature/migracao-config-motor`, commit `dda71a7`.

| métrica | legado | atual (v0.2.0) |
|---|---|---|
| acurácia | 0,9955 | **0,9990** |
| recall (= sensibilidade) | 0,6165 | **0,9951** |
| especificidade | **0,9998** | 0,9991 |
| precisão | **0,9695** | 0,9234 |
| F1 | 0,7537 | **0,9579** |
| MCC | 0,7712 | **0,9581** |

ℹ️ `sensibilidade` e `recall` são a mesma métrica (TP / (TP + FN)); aparecem numa linha só.
`especificidade` (TN / (TN + FP)) foi incluída porque acurácia e MCC dependem dela.

### 1.1 A matriz de confusão por trás

| | legado | atual |
|---|---|---|
| TP — relevante e marcou | 127 | **205** |
| FP — não relevante e marcou | **4** | 17 |
| FN — relevante e não marcou | 79 | **1** |
| TN — não relevante e não marcou | 18.270 | 18.257 |

### 1.2 Por corpus

| corpus | relevantes | legado F1 / MCC | atual F1 / MCC |
|---|---|---|---|
| imagem (15.778) | 157 | 0,712 / 0,735 | **0,946 / 0,946** |
| colonoscopia (2.702) | 49 | 0,874 / 0,879 | **1,000 / 1,000** |

---

## 2. Como ler

- O **atual vence em recall, F1 e MCC** em qualquer leitura (§4): recupera 205 dos 206 relevantes
  contra 127 do legado.
- O **legado vence em precisão e especificidade**: marca menos (131) e o que marca é quase todo
  certo (4 erros). O atual marca 222 e erra 17.
- O 1 FN do atual é um "persiste espessamento parietal difuso… **infiltração da gordura**" — parede
  ativa escrita sem a palavra "densificação", que o regex exige. Recuperável na 0.2.1 (a medir).

**Os 17 FP do atual** (todos na imagem):

| causa | laudos | exemplo |
|---|---|---|
| negação a distância que a janela 6 não alcança | 8 | *"Não há evidências de atividade inflamatória, estenoses, **fístula** ou abcessos"*, *"Fístula perianal: **ausente**"* |
| anatomia fora do trato intestinal | 3 | pieloureteral, duodeno/piloro, vesicovaginal |
| referência a exame/procedimento passado, coleção resolvida | 4 | *"clipagem recente da fístula"*, *"estudo de colonoscopia de 26/08/2026"* |
| só recomendação de RM para fístula, sem achado | 2 | *"RM com protocolo para fístula poderá trazer informações"* |

**Os 79 FN do legado:**

| causa | laudos |
|---|---|
| achado no corpo/conclusão que a segmentação `auto` descartava | 39 |
| menção só na indicação/história clínica | 19 |
| recomendação de colonoscopia (achado por decisão do negócio, 11/09) | 10 |
| "processo inflamatório intestinal" / enterocolite | 9 |
| trajeto fistuloso eventual ou sequelar | 2 |

---

## 3. Contra o quê — a referência

Não existe gabarito clínico do DII. Usar o legado como verdade zeraria a coluna do legado (recall e
precisão 1,0 por definição). A referência foi construída em duas partes:

1. **Onde legado e atual concordam** — 18.379 laudos (99,5%) — assume-se que ambos estão certos.
   ⚠️ Isso infla a acurácia dos dois igualmente (é o TN gigante); por isso F1 e MCC pesam mais.
2. **As 101 divergências** (5 só-legado + 96 só-atual) foram **auditadas laudo a laudo pela evidência**,
   com o critério **"a régua do legado aplicada corretamente"**:

| conta como relevante | não conta |
|---|---|
| menção **não negada** a DII / Crohn / retocolite / colite ulcerativa — **inclusive na indicação**, como o legado faz | negação explícita, mesmo a distância |
| complicação intestinal ou perianal: fístula, abscesso, coleção, estenose, trajeto fistuloso | anatomia fora do trato (pieloureteral, duodeno, vesicovaginal) |
| "processo inflamatório intestinal" (o legado casava por expansão semântica) | referência a exame ou procedimento passado; coleção resolvida |
| parede ativa persistente | recomendação de RM para fístula **sem** achado |
| recomendação de colonoscopia numa TC (decisão clínica do negócio, 11/09) | os casamentos por fuzzy do legado (`concêntrico`~`crônico`, lipoma → parede ativa) |

Resultado da auditoria: dos 96 só-atual, **79 relevantes e 17 não**; dos 5 só-legado, **1 relevante e
4 não** (os 4 são o fuzzy). Lista completa, com bucket e motivo por laudo:
`anexos-medicao/referencia_auditada_divergencias.csv`.

---

## 4. O quanto o resultado depende do critério

Dois pontos da referência são discutíveis e movem a precisão do atual:

| se… | relevantes | legado F1 / MCC | atual F1 / MCC | atual precisão |
|---|---|---|---|---|
| referência como em §3 | 206 | 0,754 / 0,771 | **0,958 / 0,958** | 0,923 |
| menção **só na indicação/história** não conta (−19) | 187 | 0,799 / 0,810 | **0,910 / 0,912** | 0,838 |
| indicação **e** enterocolite aguda não contam (−28) | 178 | 0,822 / 0,830 | **0,885 / 0,889** | 0,797 |

A ordem não muda em nenhuma variante. O que o negócio decidir sobre **indicação** e **enterocolite**
move a precisão do atual entre **0,80 e 0,92** — e, se decidir que não contam, é ajuste de config
(`findings_policy.ignore_sections`; retirar o regex `processo inflamatório intestinal`), a medir.

---

## 5. O que estas métricas NÃO são

- **Não são a homologação.** Medem "o que a régua decide" sobre laudos do legado. O filtro de entrada
  da plataforma (2,8% dos exames do legado não alcançados, `dii-resultado-medicao.md` §1) e a view de
  exportação ficam fora — só o e2e em dev os testa.
- **Não são verdade clínica.** A referência é a régua do legado aplicada com rigor, auditada por quem
  fez a migração, não por médico. Os 101 laudos divergentes estão listados para revisão.
- **3 dias.** Colonoscopia tem 49 relevantes na referência; um dia normal tem 2. Os números da
  colonoscopia (1,000) são de coorte pequena.

---

## Anexos (`contexto/dii/anexos-medicao/`)

- `referencia_auditada_divergencias.csv` — as 101 divergências: `id_exame`, corpus, legado, atual, referência, bucket, motivo
- `referencia_auditada_completa.csv` — os 18.480 laudos: legado, atual, referência
- `final_imagem_extras.csv` / `final_colonoscopia_extras.csv` — os 96 só-atual, com evidência
- `final_imagem_legado_nao_marcados.csv` — os 5 só-legado
- `relatorio_0.12.3.md` — tabelas por dia de todas as medições

Reprodução: `scratchpad/medicao/` (harness `comparador.py`, saída `saida/0.12.3/<corpus>__final_v020.parquet`),
bancada `C:\bancada_0123` (nlp-engine 0.12.3).
