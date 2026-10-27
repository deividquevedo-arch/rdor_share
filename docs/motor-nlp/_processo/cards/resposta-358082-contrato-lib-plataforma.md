# Resposta ao card 358082 — [Plataforma NLP] Alinhar contrato de entrada e saida e a SPEC 27 com o time de Ops

> Rascunho de comentário, 26/10/2026. Responde às perguntas do comentário de detalhamento, na ordem.
> Fontes: `nlp-engine-lib` (`contracts.py` e `REFERENCIA-PARAMETROS.md`, diff `v0.12.3..v0.15.4`), `main` da
> plataforma, tabelas de saída de prd.

**1. Quem produz e quem consome o payload**
- **Produz:** a lib `nlp_engine`, chamada pelo runner `ntb_ia_motor_e2e` (etapa `nlp_ia_04_process`), nas 13 linhas.
- **Onde fica:** tabela `diamond_fabrica_ia.<linha>.tb_mod_diamond_<linha>_saida_v0`. Colunas planas (`fl_relevante`,
  `confidence_score`, `findings`, `findings_spans`, `findings_match`, `engine_version`, `config_version`) e o
  **payload completo em JSON** na coluna texto `exm_laudo_resultado` (com `decision_trail` e `quantitative`).
- **Consome:** a view `vw_mod_diamond_<linha>_export_v0` (`nlp_ia_06_view`) → envios OneDrive/API da navegação; a
  randomização (`nlp_ia_07`, via view); o monitoramento. Para o negócio, o campo lido é **`findings`** (coluna de
  achado do arquivo de navegação). A view também expõe `exm_laudo_resultado`.

**2. Campos novos (0.12.3 → 0.15.4)** — todos **condicionais** (só aparecem quando têm conteúdo) e **aditivos**:
nenhum removido ou renomeado. Ficam **dentro do JSON** de `exm_laudo_resultado`; nenhuma coluna da tabela muda.

| campo | tipo | onde | versão | significado |
|---|---|---|---|---|
| `semantic_promoted` | bool | topo | 0.14.0 | a camada semântica levantou `fl` de 0 para 1 |
| `decision_source = llm_promotion_without_rule_evidence` | valor novo de string | topo | 0.14.0 | a guarda de evidência reverteu promoção do juiz |
| `llm_prompt_tokens`, `llm_completion_tokens` | int | `quantitative.<criterio>` | 0.14.0 | tokens da extração de medida (mesmo nome do caminho do juiz) |
| `measure_lesion_linked` | bool | `quantitative.<criterio>` | 0.15.0 | a medida usada é da lesão categorizada (`false` = não deu para atribuir) |
| `measure_lesion_window` | string (`linha`/`sentenca`) | idem | 0.15.0 | janela que ligou medida e lesão |
| `measure_lesion_skipped` | string (`criterio_composto`/`categoria_ausente`) | idem | 0.15.0 | por que o vínculo não foi tentado |
| `require_measure_sem_vinculo`, `require_measure_skipped_infra`, `require_measure_unmet` | bool | idem | 0.15.0 | qual das três causas deixou `met` sem decisão |
| `measure_lesion_candidatas` | int | idem | 0.15.1 | quantas medidas da categoria disputaram o máximo |
| `measure_lesion_max_refuta` | bool | idem | 0.15.1 | o maior do laudo já refuta o limiar |
| `require_measure_categoria_orfa`, `require_measure_categoria_ausente` | bool | idem | 0.15.3 | categoria sem dono (protege) × ausente verificada (rebaixa) |
| `measure_lesion_maximo_documento` | float | idem | 0.15.4 | maior medida do documento, preservada |
| `start`, `end` | int | `ordinal_mentions[]` | 0.15.0 | posição da menção ordinal no texto |

ℹ️ O card citava 14; a declaração do contrato da lib tem os 16 acima (contagem anterior feita só sobre o que
apareceu em run real). Mudança de semântica, não de schema: em blocos protegidos `met` passa de `false` a `null`.

**3. Chave nova de entrada:** `nlp.lesion_linking.janela` (0.15.2), opcional. Valores `linha`, `sentenca`,
`linha+sentenca`; **ausente → `linha+sentenca`** (o default medido). Valor fora do domínio **levanta erro** (não cai
em default silencioso). Nenhuma config declara hoje. A chave `waive` já existe desde a 0.12.3 (ver card `305875`).

**4. Campo de medida que pode vir vazio:** é a medida exibida em **`findings`** (ex.: `TR4 - Nódulo (1,8 cm)` passa a
`TR4 - Nódulo`) quando a lib não consegue atribuir a medida à lesão categorizada (`measure_lesion_linked = false`).
Antes, a medida exibida podia ser **de outra lesão** (defeito do card `306034`). Medido no TI-RADS, 16.021 laudos:
**277 `findings` mudam — 165 perdem a medida, 108 mudam de valor, 4 ganham**. Tratamento: exibir sem medida;
nenhuma coluna nova. Só o TI-RADS tem critério que gera esse caso hoje.

**5. Projetos impactados pela troca 0.12.3 → 0.15.4**
- **TI-RADS** (único com critério de medida que condiciona categoria): medido **133 rebaixados, zero promovidos**
  em 16.021 pares (taxa 5,85% → 5,02%) e os 277 `findings` acima.
- **Linhas com semântica/juiz** (guarda de evidência da 0.14.0): hepatologia **não deve adotar antes de
  `segmentation.mode: full_doc`** — a guarda rebaixaria verdadeiro positivo; ateromatose é o alvo real da guarda.
- **0.13.0:** delta zero medido (cancer_rim, 4.172 laudos).
- **Demais linhas:** sem medição ponta a ponta — cada uma precisa de um run comparativo antes do pin.

**6. As cinco divergências da SPEC 27** (`docs/usage/specs/27-config-especialidade.md`, conferido na `main`)
1. §2/§6.1 dizem que `runtime.llm_router` é **ignorado** — o `ConfigLoader` faz `self.llm_router.update(runtime_llm)`
   (`plataform/config/ntb_ia_loader.py`): o `runtime` **sobrepõe** o `nlp`. 11 das configs declaram `runtime`.
2. Catálogo efetivo descrito como `diamond_ia_dev`/`diamond_ia_hml`/`diamond_ia` — o real é `diamond_fabrica_ia_*`.
3. "Regex sofisticado com lookbehind: não há caminho por config" — lookahead/negação estão em produção no
   `gold_filter.keywords` (ex.: reumatologia, DII).
4. "O CONFIG é um dicionário literal e nada mais" — configs em produção têm `assert` de consistência no fim
   (ex.: câncer de cólon), que é uma prática melhor e deveria ser a regra.
5. `gold_query` ignorado (corrigido no transplante de pulmão; o texto da SPEC continua válido como alerta).

**7. O que alterar**
- **SPEC 27:** reescrever §2 e §6.1 (o `runtime` é lido e vence o `nlp`); corrigir catálogos; atualizar o trecho do
  lookbehind; trocar "dicionário literal e nada mais" por "dicionário literal com validação opcional".
  Seção nova com o contrato de saída (os campos acima) e a chave `lesion_linking.janela`.
- **boas-práticas 02** (Passo 6, l. 222) e **09** (checklist, l. 41): retirar "mesmo sem efeito técnico" — tem efeito e vence.
  Recomendação de fundo: declarar o efetivo em `nlp.llm_router` e **remover o bloco `runtime`** das configs, nessa
  ordem (inverter desliga o juiz da hepatologia em silêncio).

**8. Quem aponta para a 0.9.4:** nenhuma linha roda a 0.9.4 — 12 declaram `0.12.3` literal e o câncer de cólon usa
`${nlp_engine_version}` (rodou `0.15.4` em prd). A 0.9.4 aparece só **como exemplo na documentação**:
`specs/04-config-dependencies.md` (l. 42–76), `specs/01-runner-motor-e2e.md` (l. 122). Atualizar o exemplo.

**9. Evidência para validar o contrato da 0.15.4**
- `tests/test_contrato_de_saida` na lib, que desde a 0.15.x desce ao bloco `quantitative`, e
  `tests/chaves_observadas_em_run_real.json` (chaves de 1.500 blobs reais) como piso;
- golden local (corpus sintético, delta byte a byte contra a versão anterior);
- A/B em coorte real do TI-RADS (runs `866752815785087` e `879276610970353`).

**10. "Aceite do contrato por quem consome", tecnicamente**
(a) em dev/hml com a 0.15.4, view e envios rodam sem erro e **as colunas entregues não mudam**; (b) quem lê
`exm_laudo_resultado` ignora chaves desconhecidas e não depende de `met = false`; (c) o negócio aceita `findings` sem
medida quando a lib não atribui a medida à lesão.

**11. Pin da 0.15.4:** alteração técnica pequena e **separada** — card `358081`: trocar
`"nlp_engine_version": "0.12.3"` por `"0.15.4"` nas definições de job e o placeholder do câncer de cólon por literal,
**linha a linha, com run comparativo em hml antes**. Não entra nesta história.

**Antes × depois (resumo)**

| | 0.12.3 (prd hoje) | 0.15.4 |
|---|---|---|
| colunas da tabela de saída | — | **iguais** |
| JSON `exm_laudo_resultado` | sem os campos acima | +16 campos condicionais, `met` pode ser `null` |
| `findings` | medida podia ser de outra lesão | medida da lesão categorizada, ou **sem medida** |
| entrada | — | `lesion_linking.janela` opcional |
| decisão | — | TI-RADS: −133 em 16.021, 0 promovidos; demais a medir |
