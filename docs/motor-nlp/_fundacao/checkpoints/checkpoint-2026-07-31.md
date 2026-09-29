# Checkpoint — 2026-07-31

Ponto de retomada. Cobre três frentes que andaram em paralelo: **P0 do MLOps** (fechado), **câncer de estômago** (aguardando Carol) e **tireoide** (aguardando reunião de régua).

---

## 1. P0 do MLOps — FECHADO

**`[P0-01]` #253559** (`api_key` na config) e **`[P0-02]` #253568** (redação de credencial), entregues na **`nlp_engine` 0.7.1** — tag publicada, wheel em `/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/`.

**Validação em workspace real** (`adb-7405607882166874`), notebook `plataform/tests/ntb_ia_validacao_lib.py` na branch `feature/validacao-plataforma` de `fabrica-ia-nlp-platform` — **sem merge, por decisão do head**: versão 0.7.1 · credencial pela config · chamada real ao serving endpoint · `missing_api_key` correto · zero vazamento na saída.

**Ressalva declarada:** o DoD pedia validação com Secret Scope; foi feita com **token de sessão**, porque o workspace **não tem nenhum scope**. Confirmado com Gabriel (MLEng): é questão de acesso, e o time usa o mesmo `apiToken().get()`.

**Breaking:** `missing_env:<VAR>` → `missing_api_key`. Gabriel confirmou que o valor é apenas persistido no blob (`diamond_ia_dev.{especialidade}.tb_mod_diamond_{especialidade}_saida_v0`), sem query filtrando. Nenhuma migração.

**Nada pendente do nosso lado.** Próximo bloco do backlog deles: `[P1-03]` a `[P1-06]` (setup.py obsoleto, fonte única de versão, doc divergente, `py.typed`).

---

## 2. Câncer de estômago — aguardando Carol (segunda)

Config **`0.1.8`** em `release/cancer_estomago` (`a0c8ec1`), com as respostas dela incorporadas.

**Onde chegamos:** as 3 correções dela dissolveram o único FN (era úlcera de **duodeno** — fora do estômago pela regra 4 dela). Resultado no lote 1:

| | TP | FP | FN | precisão | recall | MCC |
|---|---|---|---|---|---|---|
| 0.1.7 | 8 | 4 | 0 | 0,667 | 1,000 | 0,804 |
| **0.1.8** | 8 | **1** | 0 | **0,889** | 1,000 | **0,939** |

**Com ela:** `Downloads/homologacao-cancer-estomago-lote2-v0.1.8.xlsx`, 44 laudos.

**Ao receber, olhar nesta ordem:**
1. **Blocos E/F** (22 negativos com termo de malignidade, nunca revisados) — se algum voltar "Sim", há FN real e o recall cai. É o teste que os números ainda não fizeram.
2. **Bloco A** (2 positivos novos) — comportamento não observado.
3. **Bloco D** — a resposta dela sobre apertar ou não o filtro de órgão para o último FP.

Se E/F voltarem todos "Não" e A/B/C confirmarem, a 0.1.8 é candidata a homologação formal.

---

## 3. Tireoide — aguardando reunião de régua

Planilha `Downloads/pauta-revisao-spec-tireoide-V2-2026-07-30.xlsx`, 9 abas. Spec de origem registrada em `docs/motor-nlp/tireoide/spec-negocio-tireoide-discovery-v1.md` (**não commitada**).

**Três decisões, nesta ordem:**
1. **Ratificar** TR4 <1 cm sai da régua — 29 casos, 100% consistente. Não é discussão.
2. **Decidir** a faixa 1,0–1,5 cm em TR4 — 15 laudos, 11 Sim / 4 Não no mesmo estado. Nada separa; é a única ambiguidade com volume.
3. **Definir escopo** — 17 laudos com achado extra-tireoidiano. A spec diz `Órgão: Tireoide` **mas lista TC de pescoço** como exame; a contradição é da spec.

⚠️ **Gate obrigatório:** depois das decisões, **re-rotular a base ouro** (`verdade_v3`) **antes** de mexer na config. Validar config nova contra gabarito antigo não significa nada.

**Contexto:** o motor está em precisão 0,969 · recall 1,000 · MCC 0,980 contra `verdade_v2`. A divergência com o avaliador (73,2% de concordância) é **evolução de régua**, não defeito.

---

## Estado dos repositórios

| repo | branch | estado |
|---|---|---|
| `nlp-engine-lib` | `hml` | 0.7.1, tags `v0.6.4`→`v0.7.1` publicadas |
| `fabrica-ia-plataforma` | `release/cancer_estomago` | 0.1.8 pushada (`a0c8ec1`) |
| `fabrica-ia-nlp-platform` | `feature/validacao-plataforma` | notebook de validação, **sem merge** |

**Local, não commitado:** `docs/motor-nlp/tireoide/spec-negocio-tireoide-discovery-v1.md`.

---

## Lições desta rodada

**Teste verde não prova nada.** O `T8` do P0-01 passava **vazio** — o fallback nunca disparava e as asserções não rodavam. Só apareceu ao exigir pré-condição explícita. E o teste de mutação (8 mutantes) revelou 1 sobrevivente: o caminho `llm.connection` estava descoberto.

**Testar a função isolada não é testar o caminho.** O bug mais grave (`api_key` descartada em `quantitative._shared_llm_defaults`) sobreviveu a três revisões porque os testes chamavam a função direto, não o trajeto `config → resolve → call`.

**Documentação envelhece com o código.** A docstring do `_scrub` descrevia a ordem **errada** — exatamente o bug corrigido. Quem a seguisse reintroduziria o vazamento.

**Citação de versão precisa de gate automático.** Ficou desatualizada em **duas releases seguidas**, em 5 lugares. Virou `scripts/check_release.py` + `make release-check`.

**Planilha que mostra só divergência enviesa o revisor.** A Carol sugeriu remover o achado de linfonodo com base nos FP que via — teria custado 16 TP que a planilha não mostrava.

**Simular antes de subir config.** Pegou duas coisas na 0.1.8: um achado que não resolvia nada e um termo (`corpo estranho`) que mataria um TP, porque no corpus é instrumento (`pinça de corpo estranho`), não lesão.

**Verificar o ambiente antes de afirmar sobre ele.** Consultei secret scopes no workspace errado e afirmei ao head algo cuja evidência não valia.
