# Estado das frentes

> **Documento vivo.** É a única fonte de "onde cada coisa está agora". Carregado em toda sessão
> via `@` no `CLAUDE.md`, então sobrevive à compactação.
>
> **Regra:** aqui vai **estado** (o que está feito, o que falta, de quem depende). Fato durável e
> lição aprendida vão para a memória (`/memory`). Se uma linha aqui não muda há meses, ela é fato —
> mova para lá. Se uma memória tem data e "estado atual", ela é estado — mova para cá.
>
> Atualizado em **2026-08-18** (fim do dia).

---

## nlp_engine (lib) — `nlp-engine-lib`

**0.9.3 na `hml`**, tags `v0.9.0`–`v0.9.3` publicadas, wheels no Volume. Gate: ruff, format, mypy,
**575 testes**, `release-check`.

- `0.9.0` **`gates_ordinal_promotion`** (critério de gate declara QUAIS categorias condiciona) +
  **nomenclatura ordinal completa** (rename limpo) + **blob = núcleo mínimo + trilha sempre**.
- `0.9.1` laudo de uma linha deixou de ser apagado — ver [[laudo-de-uma-linha-e-apagado]].
- `0.9.2` `findings_policy.display` era engolido pelo carregador (a config declarava e sumia).
- `0.9.3` categoria ordinal sai **uma vez**, no achado que ela qualifica.

🔴 **CI de release quebrado — espera Diego.** Host já corrigido para `adb-7405607882166874`;
falta o **`DATABRICKS_TOKEN`** do variable group `nlp-engine-lib-hml`. **Três releases subidas à
mão** por isso. `nlp_engine-latest` segue na **0.8.4**, de propósito.

## Tireoide V2 — `fabrica-ia-nlp-platform`, branch `tirads/feature/v3-sangue`

✅ **Régua fechada e validada em bancada.** Config `0.8.0-tirads`. Sem PR — o combinado é PR só
depois de homologada. Exige `nlp_engine >= 0.9.3`.

- **Escopo:** só TR5 e TR4 ≥1 cm (`ordinal_only`), sangue em `annotate_only`, **juiz desligado**
  (medido: 3.786 chamadas, 17 promoções todas fora do escopo), **sangue fora da captação**
  (71,4% da entrada gerava zero).
- **Validado em bancada:** `findings` sai `TR4 - Nódulo (1,8 cm)`, categoria uma vez só, zero
  driver de juiz. Queda de **8,03% → 2,90%** no mesmo dia (~64% menos apontamentos).
- 🟡 **Volumetria definitiva** depende do run da janela cheia (17/06–11/07) — ou o número do dia
  único basta, decisão do Natan.
- 🔴 **`model_version` de bancada precisa voltar a `v0` antes do PR** (hoje `bench0818full`).
- ⚠️ 26% dos entregues saem só com a categoria (`TR5`), sem nome de lesão: 9 de 15 por gate de
  órgão, 6 sem termo na régua. Não mexer sem medir — afrouxar o gate traz outro órgão.
- 🔴 **Aguardando Carol:** planilha de 500 laudos, desde 13/08.
- ✅ Backup das 3 variantes de escopo em `_versoes-estaveis/` + matriz no cabeçalho da config.

## Transplante de pulmão

V1 **entregue** (2026-08-06, card 246669). V2 **especificado e parado**.

🔴 **V2 NÃO autorizada — em backlog até liberação do Natan.** Não trabalhar nela sem esse aval.

- 🟡 Quando for liberada: o bloqueio "promoção sempre vence o gate" foi **parcialmente resolvido**
  na `0.8.5` (`gates_ordinal_promotion`), mas ali a blindagem levantada é a **ordinal**. Falta
  avaliar se o pulmão precisa também de `on_met: demote` para expressar contraindicação — hoje um
  paciente com VEF1 < 30% **e** FEVE < 40% seria encaminhado sendo contraindicado.

## Hepatologia

🔴 **Única especialidade em `segmentation.mode: auto`** — provavelmente descarta IMPRESSÃO/CONCLUSÃO
de todo laudo. **Nunca medido.** Mensurável desde a 0.8.3 (`segmentation_coverage`).
No ca-rim, a mesma correção recuperou **+25 laudos em 6 dias**.

## Câncer de estômago — branch `cancer_estomago/feature/migracao-plataforma`

✅ **Migrado para a plataforma nova** (`0.2.0`, régua byte-idêntica) **e régua da úlcera fechada**
(`0.3.0`). Exige `nlp_engine >= 0.9.1`.

- ✅ **Recall 0,533 → 1,000, FN=0** no lote de 37, contra o gabarito revisado pela decisão do
  negócio. Achado `ulcera` **separado** do `ulcera_suspeita`, para a fila distinguir os dois.
- 🔴 **Precisão NÃO medida** — a avaliação offline roda sem o juiz, que é quem filtra. Úlcera
  gástrica é achado comum: **exigir run antes de comprometer data.**
- 🔴 **Não rodado na plataforma nova ainda.** Migração e régua estão commitadas e pushadas
  (`3858ed8`, `ba049b1`), sem run.
- ⚠️ `…10142484` aparece como FP com `Úlcera`, mas é candidato a TP (lesão ulcerada com necrose e
  bordas friáveis, biopsiada) — foi marcado "Não" **antes** da decisão do Targa. Reconfirmar.

✅ **Negócio respondeu (Targa, 18/08): dos pré-malignos, só ÚLCERA entra.** Isso reclassifica 5 dos
7 FN — sobram 2 (úlcera gástrica real). **Recall 0,533 → 0,800.**

- 🔴 **A régua V1 não detecta úlcera** — é mudar config e rodar, não reclassificar planilha. Úlcera
  gástrica é achado muito comum: **exigir volumetria antes de comprometer data**.
- 🔴 **Migrar para a plataforma nova.** Config vive em `apps/databricks/nlp_engine/
  ntb_ia_cancer_estomago_config.py`, branch `release/cancer_estomago` do `fabrica-ia-plataforma`.
  Sugestão: migrar primeiro **sem** mexer na régua, para separar "mudou de plataforma" de "mudou
  de resultado".
- ⚠️ 1 dos 3 FP é candidato a virar TP (`…10142484`, lesão ulcerada com necrose e bordas friáveis).

## Câncer de rim — Leandro

✅ **Ativou o juiz LLM e subiu para >91%** (2026-08-18). Já tinha resolvido a segmentação com
`full_doc`; os 3 avisos dele viraram 0.8.3/0.8.4, e o da versão do motor se resolve com a migração
(branch `cancer_rim/feature/migracao-config-motor`).

- ❓ **Confirmar qual métrica subiu.** O juiz estava desligado porque **derrubava 3 a 8 pacientes
  confirmados** — precisão já era ~92% com ele. Se os >91% forem precisão, a pergunta que decide é
  se o **recall** se manteve em 1,000. Em rastreio, precisão comprada com paciente perdido é
  regressão, não ganho.
- O diagnóstico escrito para ele (`docs/motor-nlp/cancer_rim/diagnostico-config-cancer-rim.md`)
  ainda não foi enviado; se ele alinhou o prompt à v0.5 por conta própria, parte dele já venceu.

## Contexto do paciente — card 280008

Estudo e desenho **concluídos** (doc macro, drawio, nota de review, 2 comentários no card).
🔴 **Pendente: SPEC da fase 0** — exclusão/refutação no escopo do laudo. É o critério que fecha o card,
e não depende de nenhuma das 6 decisões em aberto.

## Plataforma / MLOps

- ⚠️ **Dedup da entrada usa `id_exame` puro**, sem `config_version` — janela processada uma vez fica
  bloqueada. Falha em silêncio (lote vazio com sucesso). Contorno acordado com o Gabriel:
  `model_version` de bancada. Diego e Fabio vão refinar conosco.
- `persist_input` é **código morto** — declarado, lido, nunca consumido.
- 🔴 Schema `tirads` não provisionado em `_hml` nem produção.

## Dívidas transversais

- 🔴 **`main` do `nlp-engine-lib` está 167 commits atrás da `hml`**, parada na `0.1.0` desde 10/07 —
  e é a **branch default**, ou seja, a landing page do repo mostra versão e requisito de Python
  errados. Decisão de repositório (trocar o default para `hml`, ou mergear). Cinco outras branches
  têm README e `pyproject` divergentes.
- 🔴 **9 CSVs com texto de laudo** nos commits locais da raiz — bloqueia push de `docs/`, que
  portanto está **sem backup**.
- `git-steward` ainda é 78 linhas sempre carregadas; candidato a virar regra curta + skill.
