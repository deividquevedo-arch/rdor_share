# Base ouro Câncer de Estômago V1 — régua, composição e métricas (consolidada 2026-07-28)

> Piloto em iteração. Esta base consolida o que **já temos validado** (homologação da Carol sobre a
> config `0.1.3`) enquanto aguardamos o E2E da `0.1.4` e uma amostra com **positivo confirmado**.

## Régua de relevância V1 (SPEC de negócio — EDA)
Exame: **Endoscopia Digestiva Alta (EDA)**. Um laudo é **relevante** quando há, no estômago, um achado
**REAL (não negado)** de malignidade/suspeita:

- **neoplasia / tumor / câncer / suspeita para neoplasia**;
- **classificação de Bormann** (I, II, III ou IV);
- **lesão infiltrativa** (+/- difusa); **lesão vegetante**; **lesão úlcero-vegetante**;
- **linite plástica** ("estômago em garrafa de couro");
- **lesão estenosante**; **lesão deprimida** (+/- avermelhada); **lesão elevada com depressão central**;
- **massa / processo expansivo / lesão ulcerada com bordas elevadas/irregulares**.

**Não são condição de relevância:**
- exame **normal** ou sem lesão estrutural;
- achados **benignos/inflamatórios puros**: gastrite, pangastrite, metaplasia intestinal, enantema,
  erosões superficiais benignas, úlcera péptica de aspecto típico sem displasia;
- **pólipos gástricos** benignos (hiperplásicos/glandulares/fúndicos/sésseis) removidos em polipectomia;
- diagnósticos benignos objetivos (lipoma, hemangioma, cisto de duplicação, acantose glicogênica,
  glândulas sebáceas ectópicas, xantoma); alterações **funcionais** (refluxo, hérnia de hiato);
- achado maligno em **contexto negado / de vigilância / educativo** (ver mecanismo abaixo).

### Fronteira do escopo V1 — FECHADA pelo head (2026-07-28)
Três decisões que encerram a ambiguidade da SPEC:

1. **Câncer precoce entra**, mas **só** pelas 7 morfologias de gravidade listadas acima
   (infiltrativa, vegetante, úlcero-vegetante, linite, estenosante, deprimida, elevada c/ depressão central).
2. **Displasia (qualquer grau, inclusive ALTO grau), metaplasia intestinal e adenoma NÃO entram** —
   mesmo sendo pré-câncer. Razão do negócio: *"achados sutis e comuns, aumentariam muito os FP"*; o negócio
   não pediu displasia/metaplasia, e adenoma **é pólipo**, fora das palavras-chave.
3. **Morfologia Paris nua não é relevante** (0-Is, 0-IIa, "lesão plano elevada"). Só conta com
   **câncer/neoplasia maligna/tumor explícito ou suspeita de malignidade**.

> ⚠️ **Exige MALIGNIDADE:** o head foi explícito — *"neoplasia/tumores malignos no caso"*. Logo
> "tumor **benigno**" / "neoplasia **benigna**" → **não relevante** (mecanismo: `exclude:[benigno,benigna]`
> com `unless` para termos já malignos — carcinoma, adenocarcinoma, Bormann, linite, maligno, suspeita).

## Composição da base ouro V1 (LGPD-safe)
- **Arquivo:** `dados/base-ouro-cancer-estomago-2026-07-28.csv` (`id_exame, verdade, fonte`; **sem laudo**).
- **100 laudos, todos `verdade=0`** (nenhum positivo real nesta amostra — Carol).
- **SHA** (id_exame + verdade, ordenado): `d7de33665b3213ad`.

| fonte | Qtd | O que é | Confiança |
|---|---|---|---|
| `carol_review_fp` | 4 | Motor `0.1.3` marcou relevante; **Carol revisou e reprovou** ("Não") | Alta — revisão humana explícita |
| `motor_negativo` | 96 | Motor `0.1.3` = não-relevante; não anotados 1-a-1 | Média — consistente com "nenhum positivo na amostra" (Carol); pendente confirmação em amostra positiva |

## Métricas do motor vs base ouro V1 (evolução das configs)
| Config | Perfil | TP | FP | FN | TN | Acurácia | Especificidade |
|---|---|---|---|---|---|---|---|
| `0.1.3` | hybrid/llm_http | 0 | **4** | 0 | 96 | 0,9600 | 0,9600 |
| `0.1.4` | *(no-op — ver abaixo)* | 0 | **4** | 0 | 96 | 0,9600 | 0,9600 |
| **`0.1.5`** | **llm_http** | 0 | **0** | 0 | 100 | **1,0000** | **1,0000** |

> **Precisão / Recall / F1 / MCC não são computáveis**: a base tem **0 positivos**.
>
> ⚠️ **Leitura honesta da especificidade 1,0:** a base é **100% negativa**, então um classificador
> trivial que responde sempre "não-relevante" atingiria **exatamente a mesma nota**. O resultado
> confirma que os FP conhecidos foram eliminados **pelo mecanismo correto** (ver abaixo), mas **não**
> é evidência de que o motor detecta câncer. Isso só se resolve com positivos confirmados.

### `0.1.4` — NO-OP (registro do erro, para não repetir)
Resultado **byte-idêntico** ao `0.1.3` (confidence igual em 16 casas). Causa: a config declarou
`findings_ignore_sections` no **top-level** enquanto já existia `findings_policy.ignore_sections`;
o `normalize_config` **atribui** (não faz merge) → o `notas` foi **descartado em silêncio**.
Corrigido no `0.1.5` movendo `notas` para dentro de `findings_policy.ignore_sections`.
Doc da lib atualizada com o aviso de precedência (`REFERENCIA-PARAMETROS.md` §2).

### `0.1.5` — os 4 ex-FP, mecanismo confirmado no E2E
| Laudo | `0.1.4` | `0.1.5` | O que atuou |
|---|---|---|---|
| `OBSTASYVNS` ×2 | `fl=1` conf 0,936 `llm=false`<br>4 findings (c/ `adenocarcinoma`, `Tumor`) | `fl=0` conf **0,876** `llm=true`<br>2 findings | **(a)** `notas` removeu o educativo (0,936→0,876) + **(b)** banda 0,95 → juiz rodou → `llm_router_llm_negative` |
| `OBSTASYHSL` ×2 | `fl=1` conf 0,754/0,769 `llm=false` | `fl=0` (mesma conf) `llm=true` | **(b)** banda 0,95 → juiz rodou e leu "sem­úlceras" corretamente → negativo |

Guarda anti-no-op: confidence mudou em **2/100** laudos (exatamente os 2 com bloco `NOTAS`) → a
config **foi aplicada**. Custo do juiz: `llm_called` 26 → **30** de 100 (+4, os ex-FP).

### Os 4 FP da `0.1.3` (diagnóstico — não era embeddings; semantic 0,59–0,75 < threshold 0,92)
| id (prefixo) | Gatilho (regra) | Contexto real | Mecanismo |
|---|---|---|---|
| `OBSTASYVNS` ×2 | neoplasias ×2, Tumor, adenocarcinoma | "NBI **não observando**… neoplasias" (vigilância) + "**adenocarcinoma**… são comuns" (bloco `NOTAS:` educativo) | negação por janela (13 > `window` 10) + texto educativo; embeddings inflava score → juiz não rodava |
| `OBSTASYHSL` ×2 | tumorações | "**sem­úlceras** ou tumorações" (negação **grafada colada** — falta espaço na origem) | token "sem" não casa; juiz não rodava |

### Régua final da `0.1.5` (validada no E2E)
1. `findings_policy.ignore_sections` += `notas` — bloco `NOTAS:` educativo não gera achado (determinístico,
   nível-regra, atua sob **qualquer** perfil).
2. `runtime.profile: llm_http` — perfil de produção (decisão do head). `rule_only` **não** resolveria:
   sem juiz, o resíduo de negação persiste ("sem­úlceras" — `sem` puro não está nas `negation_phrases`).
3. `uncertainty_band: [0.35, 0.95]` — sob `llm_http` o `apply_runtime_profile` (fabrica-ia-lib) **força**
   `use_embeddings=True` e o hybrid infla o score dos FP a 0,75–0,94; a banda larga garante que o juiz **rode**.
4. Prompt do juiz: 3 regras de contexto com prioridade sobre a lista de termos (vigilância/NBI negada;
   menção educativa/de risco; **negação grafada colada**) + escopo do viés "na dúvida → true".

> **Nota de arquitetura:** não existe perfil "sem embeddings **com** juiz" — `rule_only` desliga os dois,
> `hybrid` liga embeddings sem juiz, `llm_http` liga ambos. Por isso a banda precisa cobrir o score inflado.

## Casos controlados (sanity, fora do CSV por id)
Validação sintética/controlada da régua (não são laudos reais com id): maligno afirmativo → `1`;
pólipo / pangastrite / metaplasia / achado negado → `0`. Passou em `0.1.3` e `0.1.4`.

## `0.1.7` — fechamento da régua (commit `62e4a77`; validado local, **sem E2E ainda**)
Duas frentes, entregues juntas (a `0.1.6` intermediária **não** foi commitada — só existe a `0.1.7`):

| Frente | Mudança | Validação local |
|---|---|---|
| **A — recall** | **Plural** nas 7 morfologias. Os regex eram *singular-only*: `lesões vegetantes` / `infiltrativas` / `deprimidas` davam **MISS** = FN puro | 12/12 controlado; **0 laudos** mudaram nos 100 reais |
| **B — precisão** | Exige **malignidade** (`exclude:[benigno,benigna]` + `unless` p/ termos já malignos); displasia/metaplasia/adenoma e Paris nua **explícitos como fora de escopo** no prompt; removido o "sem displasia" que **implicava** o contrário da decisão | 13/13 controlado; regressão 4/100 **inalterada** |

> O ganho da frente A **não aparece** na amostra de 100 — ela não tem lesão positiva. É correção de
> **FN latente**, só exercitável com laudos de câncer confirmado.

## Amostra 2 — janela 2026-06-01 a 2026-06-30 (em execução)
Segunda janela, rodada com a `0.1.7`. **Laudos diferentes** da amostra 1 (que não tem data declarada
nesta base) → o cruzamento por `id_exame` com a base ouro V1 tende a ser **parcial ou nulo**; esta
janela forma uma **amostra nova**, a ser anotada pela Carol e consolidada como base ouro V2.
**Objetivo principal: encontrar POSITIVOS e finalmente medir recall.** (Resultados a preencher.)

## Gaps e próximos passos
1. 🔴 **BLOQUEANTE — sem positivo confirmado → recall NÃO medido.** É o único gap que impede homologar.
   Pedir à Carol amostra com **câncer gástrico afirmativo** (Bormann / lesão vegetante / infiltrativa /
   linite confirmadas). Sem isso não se distingue a `0.1.5` de um classificador que nega tudo.
   **Risco específico a testar:** o juiz é agora o gatekeeper dos 30% em banda — ele rejeitou 4/4 casos
   negados; falta provar que **aceita** um positivo real (o prompt tem 3 regras de rejeição novas).
2. ~~Política Paris/precursora~~ — **FECHADA** pelo head em 2026-07-28 (ver "Fronteira do escopo V1").
3. Avaliar na lib **merge em vez de clobber** no `normalize_config` (muda comportamento → PR separado).
4. Rodar E2E da `0.1.7` para confirmar que a exclusão de benigno não mexeu nos 100 (esperado: 0 relevantes).

## Rastreabilidade
- Homologação: `mini_homologação_3.xlsx` (Carol, config `0.1.3`) — fonte dos rótulos; laudo bruto **não** entra no repo (LGPD).
- E2E: `ntb_ia_motor_e2e_ca_estomago_v0.1.3/0.1.4/0.1.5.csv` (Downloads; fora do repo).
- Configs na branch `release/cancer_estomago`: `0.1.4` = `033006d` (no-op), **`0.1.5` = `8b2ad8a`**.
- Doc da lib (precedência `ignore_sections`): `5bc6ca2` na branch local `docs/atualiza-lib-0.6.3`.
