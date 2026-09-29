# Framework de Homologação — Motor NLP (template genérico, E2E + HITL)

**Objetivo:** formalizar, num único processo reaplicável a qualquer especialidade, o fluxo de
homologação — motor × legado → HITL (escopo varia por caminho) → groundtruth compartilhado →
matriz de confusão real → "base ouro" congelada — mais o retorno periódico do time de negócio, que
alimenta o mesmo groundtruth e sinaliza drift. **Sem código novo nesta v0** — só o processo e o
diagrama (`Mapa do Sistema Rede D'Or NLP Base.drawio`, aba "Motor NLP — Framework de Homologação
(E2E + HITL)"). Motor **agnóstico**: qualquer especialidade futura segue o mesmo template.

---

## 1. Onde isto entra no e2e

`ntb_ia_motor_e2e.py` roda `setup → entrada → process → homolog → monitoring → distribuicao`
(um único notebook por especialidade, `specialty` no widget). O estágio `4/6 homolog` chama
`run_homolog()` (`fabrica_ia.nlp_platform.batch.homolog`), que decide sozinho qual caminho seguir
a partir do config:

```
run_homolog(cfg, ...)
    └─ cfg.data.tables.legacy.enabled?
        ├─ True  → Caminho A (compare → write_detail → write_summary) — parte automática JÁ IMPLEMENTADA
        └─ False → summary degenerado (skipped=True) — homolog automática é PULADA
```

O gate `legacy.enabled` só decide a parte **automática** (motor×legado). A partir daí, os dois
caminhos convergem para o mesmo princípio: **HITL sempre existe**, só muda o escopo do que é
revisado — e o retorno periódico do time de negócio (seção 4) se aplica aos dois, independente do
gate.

## 2. Caminho A — Automático motor × legado + HITL sobre divergências

Usa-se quando o legado é aceito como referência confiável na maior parte dos casos (ex.: colon,
hepatologia).

**Parte automática** (já implementada, `fabrica_ia/nlp_platform/batch/homolog.py`):
1. `compare()` — join por `id_exame` (left motor→legado, cohort fixado nos ids do motor; dedupe
   motor por última gravação, dedupe legado).
2. `write_detail()` / `write_summary()` — `homolog.detail`/`homolog.summary` (Delta): snapshot do
   dia + série para tendência semana/sprint.
3. `confusion_matrix()` — 4 quadrantes (`concordancia_relevante`, `concordancia_nao`,
   `motor_rel_legado_nao`, `motor_nao_legado_rel`). **Isto mede CONCORDÂNCIA com o legado, não é
   verdade** — `match_rate = n_match / n_motor` é uma métrica de acordo, não de acerto.

**Parte HITL (a formalizar):**
4. Recorte dos **casos DIVERGENTES** (`motor_rel_legado_nao` ∪ `motor_nao_legado_rel`) — 100% vai
   para revisão.
5. **Amostra de RECALL sobre os casos CONCORDANTES** — a concordância motor×legado não é revisada
   integralmente, mas também não fica cega: uma amostra periódica cobre o risco de **erro conjunto**
   (motor e legado concordam e os dois estão errados), que a revisão só-de-divergência não pega.
6. **HITL** — médico/time arbitra 100% das divergências e audita a amostra de recall, produzindo um
   rótulo de verdade (quem estava certo: motor ou legado) que alimenta o groundtruth compartilhado
   (seção 3.1).

## 3. Caminho B — HITL sobre amostra completa

Usa-se quando o legado **não existe ou não é gabarito confiável** (achados narrativos que o legado
não captura, ou critério mais restrito que o negócio pediu — ex.: tireoide, pulmão). Baseado no
processo já rodado em pulmão (`homologacao-pulmao-v1-metricas.md`) e tireoide
(`homologacao-manual-tirads-parcial-v0.md`).

> **Nota (confirmado 2026-07-17 — resolve D3):** dentro do Caminho B há **dois sub-casos** para o
> papel do legado:
> - **(a) legado existe, motor é evolução** — espera-se que o motor seja melhor que o legado; o
>   legado vira **baseline/contexto** (régua para checar que o motor não piorou), nunca gabarito.
> - **(b) legado não existe** — é algoritmo novo (ex.: transplante de pulmão), sem baseline nenhum;
>   o HITL parte do zero, sem nada para comparar.
>
> Em ambos os sub-casos a comparação de VERDADE é motor × **spec de negócio** (não motor × legado) —
> o legado, quando existe (sub-caso a), é só um sinal de monitoramento, não entra no rótulo gold.

| # | passo | detalhe |
|---|---|---|
| 1 | saída motor (dia/run) | avaliada contra a spec de negócio; legado (quando existe) só como baseline de monitoramento |
| 2 | divergências sobre spec de negócio | sem par confiável (motor×legado) para decidir sozinho — por isso a amostra, não só um recorte |
| 3 | amostragem para revisão | **View A** — amostra dirigida (positivos do motor + amostra de negativos); **View B** — consolidado do run completo (recall amostral sobre os negativos) |
| 4 | **HITL** — revisão da amostra completa | revisor humano marca gold (Sim/Não) por laudo, nas duas views; não há segundo classificador apontando onde olhar, por isso a amostra é mais ampla que no Caminho A |

**Por que duas views:** View A dá o par positivo/negativo balanceado para métricas de qualidade;
View B garante que o volume real do run (majoritariamente negativo) não é subestimado pela
amostragem de A.

## 3.1 Groundtruth — hub compartilhado (A + B + negócio)

Independente de qual caminho gerou o rótulo humano, tudo converge para o mesmo hub:

1. **groundtruth (gabarito consolidado)** — recebe: divergências arbitradas (Caminho A) · amostra
   revisada (Caminho B) · retorno periódico do negócio (seção 4). Reconciliação de overlap/dedup por
   `id_exame` acontece aqui quando as fontes se sobrepõem.
2. **matriz de confusão REAL (vs. verdade humana) + métricas** — TP/FP/FN/TN · acurácia · precisão ·
   recall · F1 · F2 · MCC. Diferente da `confusion_matrix()` do Caminho A (que mede concordância com
   o legado), esta mede acerto real.
3. **congelar BASE OURO** — versão + data (ex.: pulmão v1 jun/2026, tireoide v22.5); os números
   CONGELADOS viram gabarito de regressão.
4. **harness de regressão** — recomputa a matriz sobre os números congelados = gate de
   **comportamento** (não byte) para mudanças futuras nas Fases 3/4 da arquitetura alvo.

## 3.2 Tabela física — `tb_mod_monitoramento_retorno` (JÁ EXISTE)

O hub de groundtruth acima **não precisa de tabela nova** — já existe:
`diamond_ia_hml.fabrica_ia.tb_mod_monitoramento_retorno`, **cross-solução** (usada por modelos em
PRD, não só o motor NLP), já ligada ao MLflow. Schema real (ver `.alt.doc/schema_retorno.md`):

- Identificação: `id_rotulo` · `id_exame` · `id_paciente` · `id_encontro` · `id_origem`.
- Ligação MLflow: `id_execucao_modelo` (run) · `id_modelo` (experimento).
- Origem: `cd_modelo` · `cd_sistema_origem` · `cd_sub_origem` · `cd_fluxo_origem` (nosso caso =
  `laudo`) · `cd_canal_origem` · `cd_tipo_input`.
- Rótulo: `fl_relevancia` (bool) · `dsc_rotulo_referencia` · `dsc_descricao_referencia` ·
  `cd_prioridade_referencia` · `cd_versao_rotulo`.
- Revisão: `nm_revisado_por` · `cd_status_revisao` (`pendente`/`revisado`/`aprovado`) ·
  `dsc_comentario_revisao` · `cd_ajuste_rotulo` · `dt_revisao`.

**O que isso resolve/simplifica em relação ao desenho anterior desta v0:**
- **Fila de revisão** (eu tinha proposto uma tabela `wrk_fila_revisao` separada) — **não precisa
  existir**: é só `WHERE cd_status_revisao = 'pendente'` nesta mesma tabela.
- **Path oficial (item D1 do backlog, seção 7)** — já está definido; a tabela É o path. O que falta
  é só a convenção de preenchimento (próximo bloco).
- **`motivo_divergencia`** (distinção erro-de-leitura-do-laudo vs. inelegibilidade do paciente, seção
  3.1) — não precisa de coluna nova; `cd_ajuste_rotulo` é o campo já existente pra isso. Proposta:
  vocabulário controlado (ex.: `ERRO_LEITURA_LAUDO`, `INELEGIBILIDADE_PACIENTE` quando essa dimensão
  for modelada), reaproveitando o campo em vez de alterar schema de uma tabela cross-solução.

**Decisões ainda em aberto — são convenção de preenchimento, não schema novo:**
- **`fonte`** (divergência arbitrada A / amostra revisada B / retorno periódico do negócio, seção
  3.1) também não precisa de coluna nova — mapeia em `cd_sistema_origem` / `cd_sub_origem` /
  `cd_fluxo_origem`, que já existem exatamente pra diferenciar de onde veio cada rótulo.
- Como mapear `specialty_id` / `nlp_engine_version` / `specialty_config_version` (que nosso domínio
  precisa) nos campos genéricos existentes (`cd_modelo`, `id_modelo`, `cd_versao_rotulo`)? **DS
  propõe a convenção; MLOps valida que não colide com outros modelos que já usam a tabela** (é
  compartilhada entre soluções).
- "Base ouro" não vira tabela nova — é uma **view agregada** sobre `tb_mod_monitoramento_retorno` +
  `tb_mod_saída`; o congelamento em si fica marcado como tag/versão no **MLflow Model Registry**,
  seguindo a proposta original do engML de usar MLflow como camada de versionamento.

## 4. Homologação de negócio (contínua, periódica)

Fluxo independente do gate `legacy.enabled` e desacoplado do run diário — aplica-se igualmente aos
dois caminhos:

1. DS envia **diariamente** o batch classificado (toda a saída do motor, não é amostra).
2. O time de negócio audita **periodicamente** (assíncrono — não é 1:1 com o run do dia).
3. Retorno anotado: casos marcados como **FP/FN** (erro do modelo) ou **TP/TN confirmado** (acerto) —
   este é o "HITL de negócio", uma terceira fonte de groundtruth.
4. **Canal emergente (em construção com o time de negócio, ainda não formalizado):** outras
   plataformas/algoritmos que consomem os dados classificados também devolvem feedback de uso real.
   Não está formalizado neste framework — só sinalizado como direção futura.
5. **Monitoramento de drift** — o retorno periódico, olhado ao longo do tempo, é o sinal natural de
   queda de acerto em produção. Isto **sinaliza** a necessidade de reabrir/versionar a base ouro —
   não é automático, é uma decisão humana informada por esse sinal.

## 5. Governança do groundtruth

**Hoje (estado atual):**
- Gabarito humano em planilha `.xlsx` **fora do repo** (LGPD — sem texto de laudo, sem `id_exame`
  bruto; só agregados entram no repo, ex.: `homologacao-pulmao-v1-metricas.md`).
- Harness de regressão **hardcoded por especialidade** (ex.: `baseohro_pulmao.py`) — não reutilizável
  entre especialidades.
- Congelamento sem versionamento formal — a versão vive no nome do arquivo/planilha.

**Próximo passo (confirmado, ver seção 3.2):**
- **Path oficial resolvido**: `diamond_ia_hml.fabrica_ia.tb_mod_monitoramento_retorno` já existe e
  já serve pra isso — não é infra nova, é reaproveitamento de tabela cross-solução já em PRD.
- **Consumo dos revisores resolvido pro Caminho A/B interno**: MLflow Evaluation (anotação lê
  `tb_mod_saída`, grava em `tb_mod_monitoramento_retorno` em tempo real). Negócio continua por canal
  próprio (seção 4), mesma tabela de destino.
- Falta fechar: convenção de preenchimento de `specialty_id`/versões e de `fonte` nos campos
  genéricos da tabela (seção 3.2) — é isso que resta decidir, não mais "onde gravar".
- Harness genérico parametrizado por `specialty` (config-in, sem hardcode por especialidade) — na
  forma de um **notebook genérico para runs**, seguindo a diretriz de
  [[lib-evolucao-global-reusavel]]. "Base ouro" vira view agregada + tag no MLflow Model Registry,
  não tabela nova.
- Histórico de congelamentos rastreável via versionamento do MLflow Model Registry (tag por versão
  de base ouro), permitindo auditoria de qual base ouro validou qual mudança de código.

Esta evolução não muda a parte automática do Caminho A nem o schema de
`homolog.detail`/`homolog.summary` — é aditiva, só formaliza onde o gabarito humano mora.

## 6. Quando aplicar a uma nova especialidade

1. `legacy.enabled=True`? → parte automática roda sozinha (Caminho A); HITL revisa só as
   divergências (passos 4–5 da seção 2).
2. `legacy.enabled=False` (ou legado existe mas não é gabarito)? → HITL revisa a amostra completa
   (seção 3). Reaproveita os mesmos nomes de artefato (View A / View B / groundtruth / base ouro /
   harness) para manter consistência entre especialidades e permitir comparação cross-piloto.
3. Em ambos os casos, conectar ao canal de homologação de negócio (seção 4) desde o início — não é
   opcional, é o que sustenta o monitoramento de drift depois do congelamento.
4. Documentar o congelamento no mesmo padrão de `homologacao-<especialidade>-v1-metricas.md`
   (fontes, matriz, métricas, reconciliação, erros/limites, backlog aberto).

## 7. Backlog de sprint

Sequenciado por dependência: primeiro as **decisões** que desbloqueiam o resto, depois **construção**,
depois **piloto/rollout**. Sem estimativa de pontos (não é a prática do time hoje) — só prioridade e
dependência.

> Versão em formato ágil (histórias de usuário + tasks + MoSCoW, organizada por sprint) no mesmo
> diagrama: aba "Motor NLP — Backlog Ágil (Épicos · Histórias · Sprints)".

### 7.1 Decisões (bloqueantes — fazer primeiro)

| # | tarefa | desbloqueia | origem |
|---|---|---|---|
| ~~D1~~ | ~~Definir o path oficial da tabela de groundtruth~~ — **RESOLVIDO**: `tb_mod_monitoramento_retorno` já existe. Ver seção 3.2. | — | confirmado 2026-07-17 |
| ~~D2~~ | ~~Definir o consumo dos revisores~~ — **RESOLVIDO pro interno**: MLflow Evaluation. Negócio segue por canal próprio (seção 4). Ver seção 3.2. | — | confirmado 2026-07-17 |
| D5 | Definir convenção de preenchimento de `specialty_id`/`nlp_engine_version`/`specialty_config_version`/`fonte` nos campos genéricos de `tb_mod_monitoramento_retorno` (`cd_modelo`, `id_modelo`, `cd_versao_rotulo`, `cd_sistema_origem`/`cd_sub_origem`/`cd_fluxo_origem`) — DS propõe, MLOps valida (tabela é compartilhada com outros modelos) | T2, T3 | seção 3.2 |
| D6 | Confirmar reaproveitamento de `cd_ajuste_rotulo` para `motivo_divergencia` (erro de leitura vs. inelegibilidade do paciente) | T4 | seção 3.1/3.2 |
| ~~D3~~ | ~~Confirmar o papel do legado no Caminho B~~ — **RESOLVIDO**: dois sub-casos, (a) legado existe e motor é evolução → baseline/contexto de monitoramento; (b) legado não existe → algoritmo novo, sem baseline. Ver seção 3. | — | confirmado 2026-07-17 |
| D4 | Definir volume/cadência da amostra de recall (Caminho A) e da amostragem View A/B (Caminho B) por especialidade | T1, T5 | gap fechado nesta revisão (seção 2) |

### 7.2 Construção

| # | tarefa | depende de |
|---|---|---|
| T1 | Implementar a amostra de recall sobre casos concordantes no Caminho A (hoje só existe no diagrama/doc, não no código) | D4 |
| T2 | Implementar a convenção de preenchimento definida em D5 (specialty/versões/fonte) escrevendo em `tb_mod_monitoramento_retorno` — não é criar tabela, é ajustar o que grava nela | D5 |
| T3 | Implementar a view `vw_groundtruth_consolidado` (reconciliação/dedup entre as 3 fontes, filtrando `cd_status_revisao`) sobre `tb_mod_monitoramento_retorno` | T2 |
| T4 | Generalizar o cálculo da matriz de confusão REAL + métricas (hoje harness manual por especialidade, ex. `baseohro_pulmao.py`) numa view (`vw_metricas_por_especialidade`) + notebook genérico parametrizado por `specialty`; segmentar/excluir `cd_ajuste_rotulo='INELEGIBILIDADE_PACIENTE'` do cálculo de precisão do motor (D6) | T3, D6 |
| T5 | Padronizar a rotina de View A / View B do Caminho B como componente reutilizável (hoje ad hoc por especialidade); tratar os dois sub-casos do legado (a/b, seção 3) — quando existe, calcular também o match-rate vs. legado como sinal de monitoramento (não gold) | D4 |
| T6 | Formalizar o contrato do retorno do negócio (schema do que volta: FP/FN/TP/TN) | — |
| T7 | Implementar o sinal de monitoramento de drift a partir do retorno periódico do negócio, conectado ao estágio `5/6 monitoring` do e2e | T6 |

### 7.3 Piloto / rollout

| # | tarefa | depende de |
|---|---|---|
| P1 | Validar o template completo em 1 especialidade do Caminho A (ex.: hepatologia ou colon) | T1–T4 |
| P2 | Validar em 1 especialidade do Caminho B que já tem harness manual (pulmão, por ter o processo mais maduro) | T2–T5 |
| P3 | Mapear o canal emergente (outras plataformas/algoritmos que consomem dados classificados) — reunião de alinhamento com o time de negócio | — (paralelo, sem bloqueio) |
| P4 | Atualizar este doc e o diagrama com as decisões fechadas em D1–D4 e o resultado dos pilotos | P1, P2 |

## Referências

- Diagrama: `Mapa do Sistema Rede D'Or NLP Base.drawio` — aba "Motor NLP — Framework de Homologação
  (E2E + HITL)".
- Código Caminho A (parte automática): `fabrica-ia-lib/src/fabrica_ia/nlp_platform/batch/homolog.py`.
- Runner e2e: `fabrica-ia-plataforma/apps/databricks/nlp_engine/ntb_ia_motor_e2e.py`.
- Precedentes Caminho B: `docs/motor-nlp/pulmao/homologacao-pulmao-v1-metricas.md`,
  `docs/motor-nlp/tireoide/homologacao/homologacao-manual-tirads-parcial-v0.md`.
- Schema real da tabela física do groundtruth (seção 3.2): `.alt.doc/schema_retorno.md`
  (`diamond_ia_hml.fabrica_ia.tb_mod_monitoramento_retorno`).
