# Mapa de gaps — lib × plataforma

**Data:** 2026-08-21 · **Fontes:** agendas "NLP Platform - Metodologia" (42min) e "NLP - Alinhamento"
(2h24), documento `PROPOSTA_CONFIG`, revisão do PR 7102, auditoria de código dos dois repositórios.

> Documento de trabalho. Cada item tem **evidência**, **dono proposto** e **solução**. O que não tem
> evidência não entrou.

---

## 1. O que aconteceu

Pipeline do TI-RADS caiu **em produção** em 20/08. Causa: a esteira publica produção a partir da
`main` do `nlp-engine-lib`; a branch estável do time de DS era a `hml`, e a `main` estava parada na
`0.1.0`. **O fluxo nunca foi acordado por escrito.**

Resolvido no mesmo dia (main sincronizada, `0.9.4`). Verificado: TI-RADS rodando em produção em
21/08 com `0.8.0-tirads` + engine `0.9.4` — 31.418 laudos, 997 relevantes.

O incidente expôs um problema maior, que as duas agendas nomearam igual: **falta contrato e falta
comunicação**. Os dois lados remendaram em silêncio — nós com paridade de bit e compatibilidade
retroativa, eles desligando lint, removendo checagem de tipo e escrevendo contornos.

---

## 2. Decisões que MUDARAM hoje

Registrar isto é o mais importante do documento: duas premissas que valiam ontem já não valem.

| antes | agora | fonte |
|---|---|---|
| produção usa **`latest`** | produção usa **versão FIXADA por especialidade**; `latest` fica para o time de DS | Diego, 1:21:54 |
| fluxo de branch informal | **PR para `hml`** publica a wheel no volume de **dev** (validação nossa) · **PR para `main`** com code review publica em **hml/prd** | Deivid 1:27:26 + Diego |
| — | **todo PR na lib passa por code review** de Diego, João ou Gabriel, com matriz de impacto | Diego, agenda da manhã |

⚠️ Consequência prática: a partir de agora, **cada especialidade declara qual versão da lib usa em
produção**. Não é mais "sempre a última".

---

## 3. Gaps — por prioridade

### 🔴 P0 · Quebra produção ou já quebrou

**G1. Juiz LLM ligado por contorno não documentado (hepatologia e pulmão, em produção)**
`nlp.llm_router` das duas não declara `enabled`; a lib assume **`False`**. Só funciona porque o
runner mescla `runtime.llm_router` (`ntb_ia_loader.py:105-113`) — bloco que a spec 27 lista como
**não lido**. Quem limpar confiando na spec desliga o juiz em produção, sem erro e sem log.
**Solução:** declarar `enabled: True` explícito nas duas configs (mesmo valor, zero mudança de
comportamento) e decidir com o Gabriel se o merge do `runtime` fica documentado ou sai.
**Dono:** DS (configs) + Gabriel (runner). **Card `283644`.**

**G2. Fluxo de branch e publicação não está escrito**
Foi a causa raiz. A `main` foi sincronizada, mas o acordo continua verbal — então repete.
**Solução:** redigir e publicar o processo já definido na §2.
**Dono:** Diego. **Card `283645`.**

**G3. A esteira não valida**
Gabriel desativou o **lint do Ruff** e removeu a **checagem de tipo** para conseguir rodar. Hoje
nada é barrado antes do merge.
**Solução:** reativar, com a checagem aceitando os **dois** formatos de `findings`.
**Dono:** Gabriel/João. **Possivelmente card `260895`.**

**G4. Doc do consumidor errada + mensagem de erro que induz correção destrutiva**
`27-config-especialidade.md` documenta só o formato plano e cita o TI-RADS como exemplo dele — o
TI-RADS usa o outro. Somado à mensagem `nlp.findings values must be list[str]`, levou à proposta de
achatar a config, que teria desmontado a régua V2 em hml.
**Solução:** documentar os dois formatos, marcar o por-entidade como canônico desde a `0.6.0`,
corrigir o exemplo; e reescrever a mensagem para nomear os dois formatos aceitos e o que recebeu.
**Dono:** DS. **Sem card — PR separado já planejado.**

### 🟠 P1 · Risco regulatório ou de escala

**G5. Compliance/DPO do envio de laudo ao LLM**
Três especialidades enviam excerto; **duas em produção**. Sem validação registrada. Desligar o juiz
**não** interrompe: o extrator quantitativo não depende de `llm_router.enabled`.
**Medido (ca-estômago, 10.783 laudos):** 342 enviados (**3,2%**), **nenhum identificador de paciente
no payload**, 0 CPF, **210 de 342 (61%) com nome de médico**, 3 com nome de acompanhante, truncado
em 8.000 chars, modelo servido pelo próprio Databricks.
**Solução:** levar os números ao DPO com quatro perguntas (contrato cobre prompt e retenção?
endpoint no tenant? nome de profissional está no escopo? foi validado quando hepato e pulmão
subiram?). **Mitigação sem desligar nada:** o `text_pipeline` já remove boilerplate — dá para
remover as linhas de identificação profissional antes do envio.
**Dono:** Diego/Fabio escalam · DS fornece os números. **Card `283646`.**

**G6. Vazamento de memória no `process()`**
Gabriel: *"mando alto volume e ele escala memória até estourar"* — 32 GB em 3 clusters não seguraram.
Contornou com lotes.
**Causa localizada:** `engine.py:76` → `return [run_row(row, ctx) for row in rows]`. A lib recebe a
sequência inteira e **constrói a lista inteira de saída em memória**. Não há streaming.
**Solução:** oferecer variante geradora (`iter_process`) que faz `yield` por linha, e/ou chunking
interno com liberação do doc spaCy. O `run_row` já é por linha — a mudança é de fronteira, não de
algoritmo.
**Dono:** DS. **Sem card.**

**G7. A lib não emite log nenhum no caminho NLP**
João: *"ficamos no escuro"*. Gabriel: *"não consigo debugar, tenho que entrar no teu código"*.
**Confirmado:** nenhum módulo de `nlp_engine/nlp_engine/*.py` importa `logging`. Existe
`monitoring/logging.py` com `StructuredLogger`, `redact` e `mask_ref` — **e o núcleo não o usa.**
**Solução:** injetar o `StructuredLogger` já existente nas fronteiras (`process`, `step_*`), com
`redact` para não vazar texto clínico. O componente existe; falta ligar.
**Dono:** DS. **Sem card.**

**G8. Juiz pode promover sem evidência de regra**
Invariante de arquitetura. Existe `_tem_evidencia_dura` impedindo o juiz de **derrubar** evidência
dura; falta a simétrica, impedindo que ele **crie** relevância do nada. Hoje contornado por banda na
config, o que é frágil — **hepatologia está em produção com banda `[0.35, 0.65]`**, a mesma faixa em
que o ca-estômago tinha 2.861 laudos sem evidência indo ao juiz.
**Solução:** guarda em código no router. Antes disso, **medir a hepatologia** (quantos entregues com
`findings` vazio) — barato e diz se o risco é teórico ou concreto.
**Dono:** DS. **Card `283648`.**

### 🟡 P2 · Estrutural — impede a repetição

**G9. Contrato de entrada e saída não declarado**
Consenso das duas agendas. Hoje a plataforma **supõe** o que o motor devolve e a lib **supõe** o que
recebe.
**Solução:** declarar `CONTRATO_DE_ENTRADA` e `CONTRATO_DE_SAIDA` testáveis dos dois lados. A lib já
tem `output_invariants.py` como base; o card `[P2-09]` (TypedDict de `contracts.py`) é adjacente.
**Dono:** compartilhado, primeiro movimento é do DS. **Card `283647`.**

**G10. `embedding_model` aponta para o Volume antigo**
As três configs apontam para `diamond_ia_hml`. **Hoje responde** e o modelo está íntegro
(verificado). No dia em que sair, as três degradam para `token_overlap` **sem erro e sem log**.
Deivid sinalizou ao Diego quando o modelo foi transferido; foi despriorizado.
**Solução:** transpor para `gold_fabrica_ia_hml` e apontar as configs.
**Dono:** Diego (Volume) + DS (configs). **Sem card.**

**G11. Base ouro sem lugar oficial**
Gabarito vive em planilha, e-mail e arquivo temporário, sem `spec_version` nem data. Já custou uma
conclusão errada de métrica em 20/08.
**Solução:** tabela por especialidade no schema que já existe, com `id_exame`, veredito,
`anotado_por`, `dt_anotacao`, `spec_version`, `config_version`, `lote`.
**Dono:** DS + Diego (a sandbox do Datahub não cobre: é artefato que já nasce oficial).
**Sem card.**

**G12. Padronização do formato de config entre especialidades**
hepato e pulmão no formato plano; tirads e ca-estômago por entidade. Ponto do João, e ele tem razão:
o padrão só existe como convenção.
**Solução:** migrar as duas para o por-entidade (o carregador já transpõe) **ou** declarar
formalmente que os dois convivem e por quê. Recomendação: migrar.
**Dono:** DS. **Sem card.**

### ⚪ P3 · Documentação e dívida da lib

**G13.** 7 dos 15 módulos sem SPEC (`engine`, `llm_router_backend`, `scoring`, `semantic_expand`,
`contracts`, `output_invariants`, `quality_guard`). Agora com justificativa externa: **sem SPEC, o
code review obrigatório é carimbo ou bloqueio.** **Dono:** DS.

**G14.** `engine_min_version` não existe — o requisito de motor vive só em comentário. Teria dado
diagnóstico imediato no incidente. **Dono:** DS (declarar) + plataforma (validar no runner).

**G15.** Os 28 cards `[P0-01]`→`[P3-28]` do OPS são engenharia de software da lib (`setup.py`,
`py.typed`, `ruff`, cobertura, `CONTRIBUTING`). **Nenhum teria evitado o que aconteceu.** Seguem no
ritmo deles.

---

## 4. Auditoria — números

**`nlp-engine-lib`:** 15 módulos, 7.124 linhas no núcleo · 51 arquivos de teste, **484 testes**
(o `ESTADO.md` dizia 577 — desatualizado) · 7 SPECs de módulo · **zero logging no caminho NLP** ·
`process()` sem streaming · maiores módulos: `decision_pipeline` (1.523), `quantitative` (1.490),
`llm_router_backend` (1.021).

**`fabrica-ia-nlp-platform`:** lint do Ruff **desativado** · checagem de tipo **removida** ·
`runtime.llm_router` mesclado por contorno não documentado · `base_url` hardcoded no runner ·
lotes montados na plataforma para contornar o consumo de memória da lib · doc do consumidor
descrevendo formato antigo.

---

## 5. Lista compacta

| # | item | dono | prio | card |
|---|---|---|---|---|
| G1 | juiz por contorno em hepato/pulmão (produção) | DS + Gabriel | **P0** | `283644` |
| G2 | publicar o fluxo de branch e publicação | Diego | **P0** | `283645` |
| G3 | reativar lint e checagem de tipo na esteira | Gabriel/João | **P0** | `260895`? |
| G4 | corrigir doc do consumidor + mensagem de erro | DS | **P0** | — |
| G5 | compliance/DPO do envio ao LLM | Diego/Fabio + DS | P1 | `283646` |
| G6 | vazamento de memória no `process()` | DS | P1 | — |
| G7 | lib não emite log no caminho NLP | DS | P1 | — |
| G8 | juiz promove sem evidência (guarda na lib) | DS | P1 | `283648` |
| G9 | contrato de entrada e saída | compartilhado | P2 | `283647` |
| G10 | `embedding_model` no Volume antigo | Diego + DS | P2 | — |
| G11 | base ouro sem lugar oficial | DS + Diego | P2 | — |
| G12 | padronizar formato de config | DS | P2 | — |
| G13 | 7 SPECs de módulo restantes | DS | P3 | — |
| G14 | `engine_min_version` declarado e validado | DS + plataforma | P3 | — |
| G15 | 28 cards de engenharia da lib | OPS | P3 | `253559-594` |

**Sem card e P0/P1:** G4, G6, G7 — as três são **nossas** e as três têm solução localizada.
