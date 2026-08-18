# Estado das frentes

> **Documento vivo.** É a única fonte de "onde cada coisa está agora". Carregado em toda sessão
> via `@` no `CLAUDE.md`, então sobrevive à compactação.
>
> **Regra:** aqui vai **estado** (o que está feito, o que falta, de quem depende). Fato durável e
> lição aprendida vão para a memória (`/memory`). Se uma linha aqui não muda há meses, ela é fato —
> mova para lá. Se uma memória tem data e "estado atual", ela é estado — mova para cá.
>
> Atualizado em **2026-08-18**.

---

## nlp_engine (lib) — `nlp-engine-lib`

**0.8.4 na `hml`** (último deploy). **0.9.0 pronta e commitada, não pushada** — branch
`feat/gate-condiciona-promocao-ordinal`. Gate verde: ruff, format, mypy, 561 testes, release-check.

- `0.8.5` **`gates_ordinal_promotion`**: um critério de gate declara QUAIS categorias condiciona.
  Destrava "TR4 só aprova com nódulo/cisto ≥1 cm" — e é o mesmo bloqueio do transplante de pulmão V2
  ("promoção sempre vence o gate"). Também: `findings_policy.display` monta `TR4 - Nódulo (1.8 cm)`.
- `0.9.0` **nomenclatura ordinal completa** na saída e no código (rename limpo, sem chave dupla —
  medimos que não há consumidor a jusante) + **blob = núcleo mínimo + trilha sempre emitida**
  (−7% no laudo negativo, que é o caso comum).
- Guarda nova: `test_sem_definicao_duplicada` — Python aceita redefinição em silêncio, e uma
  edição malfeita duplicou 3 funções com os 528 testes verdes.
- Dívida: release notes de 0.7.4 a 0.8.1 nunca escritas.

## Tireoide V2/V3 — `fabrica-ia-nlp-platform`, branch `tirads/feature/v3-sangue`

Config **`0.7.0-tirads`** commitada, **não pushada**. **Sem PR** — o combinado é PR só depois de
homologada. Exige `nlp_engine >= 0.8.5`.

- ✅ **Régua V2 fechada** (decisão do negócio, 18/08): **só TR5 e TR4 ≥1 cm**. `relevance_mode:
  ordinal_only` — achado léxico não promove mais. Razão: a operação não absorve o volume.
- 🔴 **Volumetria do Natan desatualizada nas duas direções** — a régua estreitou (medido: 1.389 dos
  2.815 relevantes saem, ~49%) e o TR4 voltou condicionado. **Só um run resolve.**
- 🔴 **Aguardando Carol:** planilha de 500 laudos entregue (400 do que é entregue + 100 candidatos a FN).
- ⚠️ **Achado novo (18/08):** o gate de órgão descarta achado em **101 laudos** TR4/TR5+ (226 spans).
  Neles o paciente não se perde porque a categoria promove — mas o mesmo gate roda em laudo **sem**
  categoria, e lá não há rede. Candidato direto a FN; cai no grupo de 100 da planilha da Carol.
  Contornado nos critérios de tamanho com `anchor.text`.
- Defeito conhecido: 31 de 1.492 entregues sem achado nem categoria; 10 sequer citam "tireoide".

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

## Câncer de estômago

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

- 🔴 **9 CSVs com texto de laudo** nos commits locais da raiz — bloqueia push de `docs/`, que
  portanto está **sem backup**.
- `git-steward` ainda é 78 linhas sempre carregadas; candidato a virar regra curta + skill.
