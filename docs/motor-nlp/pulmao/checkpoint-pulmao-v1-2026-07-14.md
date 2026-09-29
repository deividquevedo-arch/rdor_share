# Checkpoint — Transplante de Pulmão V1 + estado da lib (2026-07-14)

Ponto de retomada. Ler junto com `spec-negocio-transplante-pulmao-v1.md` e a memória
`pulmao-linha-cuidado-story-v1`.

## Estado da lib `nlp_engine` (branches, NÃO mergeadas)
- **`refactor/decision-state-pipeline`** (de origin/hml): refactor byte-compat `engine.process` →
  pipeline `DecisionState` (`decision_pipeline.py`) + step-registry + slot categorias genérico.
  Commits `01d0be7`+`d56844e`. PUSHADA. Gate: golden 3/3 (tirads 39dc3da6 / hepato b4dc2076 /
  pirads d648e5b) + suíte 300. **Release futuro v0.4.1** (byte-compat; sem urgência p/ pulmão).
- **`feat/quantitative-extraction-agnostic`** (de origin/hml): prompt de extração quantitativa
  AGNÓSTICO + `extraction_hint` config-in. Bump **v0.4.0** (`8396502`), tag `v0.4.0`. PUSHADA.
  Gate: golden 3/3 + suíte 298. **PR→hml A MERGEAR (head)** → publica wheel `nlp_engine-0.4.0`.
  **Pulmão DEPENDE deste wheel** (usa extraction_hint; runner fixa `nlp_engine_version=0.4.0`).
- HF-no-Volume já resolvido (config v22.6 tirads / hepato 0.1.13, ambos pushados).
- Harness de regressão golden: `.claude/jobs/56d76a3e/tmp/regress_golden.py` (roda os 3 configs
  rule_only e compara SHA). Helper SQL: `.claude/jobs/56d76a3e/tmp/dbsql.sh`.

## Escopo (decisão do head)
Mexer SÓ em `nlp_engine` + (mínimo) runner e2e `ntb_ia_motor_e2e.py`. **NÃO tocar `fabrica-ia-lib`.**
Features novas = globais/config-in/reusáveis, sem proliferação de funções.

## Pulmão V1 — o que já sabemos
- **Dados:** `gold_corporativo_ia.corporativo.tb_gold_mov_exame` (="mov_exame_ia") tem ~54.756 exames
  de função pulmonar; laudo no struct `proced_lista_exames` (extract_laudo lê laudo_original/
  transformado). Paciente/idade em `tb_gold_mov_paciente` (`cli_idade`). Eu consigo consultar ambas.
- **Formato:** "X% previsto" (inline ou `[76]% previsto` nos wate). NÃO confundir com relação VEF1/CVF.
- **HTML/base64 (~58%):** `to_plain` da lib já limpa (genérico).
- **Sinônimos (G7):** VEF1=Volume Expiratório Forçado 1º seg=FEV1; CVF=Capacidade Vital Forçada;
  DLCO=Difusão do Monóxido de Carbono=DCO. Enumerar em `anchor.text` + `extraction_hint`.

## Critérios V1 (escopo: só função pulmonar; eco/cateterismo = V2)
- `funcao_vef1`: VEF1 % previsto `< 40` (cobre DPOC<30 + supurativa adulto), on_met promote.
- `funcao_intersticial`: `any_of` CVF `< 70` **ou** DLCO `< 40` % previsto, on_met promote.
- Pediátrico (<18 → VEF1<50) FORA do V1 → planejar **age-conditioning global** (limiar por campo da
  linha, ex.: cli_idade) como feature futura, reusável.

## Arquitetura de execução (relevância = só quantitativo)
- Pulmão roda `profile=rule_only` + `embeddings.use_embeddings=False` + `llm_router.enabled=False` +
  `quantitative_criteria` (promote). A camada quantitativa roda independente de perfil (usa LLM só p/
  extrair medida). **Falta o base_url do LLM chegar ao extrator** — `apply_runtime_profile`
  (fabrica-ia-lib, fora do escopo) só injeta em `llm_http`. **Solução no escopo:** ~3 linhas no runner
  (após `apply_runtime_profile`, se há `quantitative_criteria`, setar
  `nlp_cfg["llm_router"]["base_url"]`/`api_key_env` a partir do `llm_base_url` da linha 135).

## PRÓXIMOS PASSOS (ordem)
1. **[PENDENTE autorização]** ajuste mínimo no runner (injeção do base_url quando há
   `quantitative_criteria`).
2. Escrever config `ntb_ia_transplante_pulmao_config.py` na `feature/transplante_pulmao` (template →
   rule_only + quant, 2 critérios com extraction_hint + sinônimos + anchor.text).
3. Head mergeia PR v0.4.0 (nlp_engine) → wheel no Volume.
4. Puxar amostra de laudos (dbsql) p/ calibrar extraction_hint (siglas/extenso/colchetes) + montar
   base-ouro de negócio.
5. E2E com nlp_engine_version=0.4.0 → homologação (xlsx 8 colunas, formato TI-RADS).

## Git — repos e branches (para reorientar após reinício)
- Plataforma (`fabrica-ia-plataforma`): estava em `test/rads-e2e-hml` (docs stashed: "wip-docs auto:
  entra pulmao"); agora em `feature/transplante_pulmao`. Config tirads v22.6 e template já pushados.
- nlp-engine-lib: branches refactor + feat/quant pushadas (ver acima). `fix/finding-organ-scope-block`
  = resetada p/ origin (80b22ce) + rascunho serving stashed lá.
