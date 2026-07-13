# SPEC — Camada de Critérios Quantitativos (medidas/limiares) no `nlp_engine`

**Status:** rascunho v0 (pré-código) · **Piloto:** Tireoide `nódulo > 1 cm` (V2)
**Relacionado:** [[xxrads-status-e-bancada-ab]], plumbing LLM existente (`llm_router_backend.py`, perfil `llm_http`).

---

## 1. Objetivo

Dar ao motor a capacidade de **distinguir medidas/valores numéricos contra um limiar clínico**, inclusive **compostos**, de forma **config-driven, opt-in e auditável**. Exemplos-alvo do negócio:

- `nódulo > 1 cm` (Tireoide V2) — medida única associada a um achado
- `VEF1 < 30%` (espirometria) — percentual
- `PMAP > 25 mmHg` **E** `RVP > 3 Wood` (HP pré-capilar) — **composto, cross-sentença**

Hoje o motor decide por **presença/ausência** de termos (rule engine, escopo **sentença**) e por **categoria RADS** (regex). Nenhum dos dois lê **número + unidade + limiar**, e o rule engine — por ser sentença-a-sentença — **não consegue** avaliar condição composta cujas medidas estão em linhas diferentes. Esta camada preenche essa lacuna.

## 2. Princípios de design (direcionamento do time)

1. **Simples de configurar, sem engenharia de padrões.** Um critério = poucas linhas declarativas de YAML. **Nada** de manutenção de grandes listas de regex nem tuning contra datasets longos.
2. **Extração pelo LLM row-level; comparação em código.** O LLM lê o laudo inteiro e **extrai o valor + unidade + span de evidência**. O **limiar (`>`, `<`, `≥`) e a lógica composta (`all_of`/`any_of`) são avaliados em Python** — determinístico, auditável e à prova de alucinação de lógica.
3. **Opt-in e no-op por ausência.** Sem o bloco `quantitative_criteria`, o comportamento do motor é **idêntico** ao atual (byte-compatível). Especialidades sem critério não pagam nada.
4. **Acionamento gated (custo controlado).** Um critério só chama o LLM quando (a) está **configurado** e (b) sua **âncora** existe no laudo (ex.: `nódulo > 1 cm` só dispara se o achado `nodulo` foi detectado). Laudo de tireoide normal (sem nódulo) **nunca** aciona LLM.
5. **Auditável e reproduzível.** Saída estruturada com valor + evidência; `temperature=0`. A **decisão fica em código** (o LLM só extrai o número; o Python aplica o limiar) → reprodutível. O negócio homologa **o número extraído**, não uma opinião. **Sem cache** (removido por decisão — evita estado/dúvida de "de onde veio o valor"; cada laudo é avaliado na hora).
6. **In-tenant / LGPD.** Row-level = enviar texto clínico ao modelo → **obrigatoriamente** o serving Databricks (`databricks-claude-haiku-4-5`), nunca provedor externo.

## 3. Onde encaixa na arquitetura

- **Novo componente da lib:** `nlp_engine/nlp_engine/quantitative.py` (nome provisório).
- **Ponto de execução:** no `engine.process`, **após** o rule engine/RADS produzirem achados e categoria, **antes** de consolidar `fl_relevante` (para poder atuar como gate — ver §6).
- **Reuso:** o backend HTTP já existe — `call_openai_compatible_chat` (`llm_router_backend.py`), com `json_response_format`. Precisamos de **uma função nova de extração estruturada** (`extract_quantitative_llm`) com prompt+schema próprios (o router atual só devolve `{"relevante": boolean}`).
- **Independente do perfil:** funciona sob `rule_only` também — o acionamento é pelo **critério configurado**, não pelo perfil. (Não confundir com o `llm_http` band-gated de relevância; são hooks distintos que compartilham o backend.)

## 4. Contrato de configuração

Bloco novo em `nlp:` (opt-in). Um critério é **declarativo**:

```yaml
nlp:
  quantitative_criteria:
    nodulo_maior_1cm:                     # id do critério
      description: >                      # instrução em linguagem natural p/ o LLM
        Maior dimensão (em cm) do maior nódulo tireoidiano descrito no laudo.
        Se houver medidas "A x B x C", use a maior. Ignore volume da glândula.
      anchor: { finding: nodulo }         # só dispara se este achado foi detectado
      measure: { name: nodulo_max, unit: cm }
      threshold: { op: ">", value: 1.0 }
      applies_to_exam_type: [US_tireoide, ...]   # opcional (condicionamento)
      on_met: gate_relevance              # gate_relevance | annotate_only | promote
      llm:
        model: databricks-claude-haiku-4-5
        api_key_env: DATABRICKS_TOKEN
        temperature: 0
```

Composto (cross-sentença) — sem `anchor` de achado único, aciona pelo id:

```yaml
    hp_precapilar:
      description: Extraia PMAP (mmHg) do cateterismo e RVP (Wood/UW).
      measures:
        - { name: pmap, unit: mmHg }
        - { name: rvp,  unit: wood }
      condition:
        all_of:
          - { measure: pmap, op: ">", value: 25 }
          - { measure: rvp,  op: ">", value: 3 }
      on_met: promote
      llm: { model: databricks-claude-haiku-4-5, temperature: 0 }
```

**Campos:** `description` (NL, o "prompt" do critério), `anchor` (gating opcional), `measure(s)`, `threshold`/`condition` (avaliados em código), `applies_to_exam_type` (condicionamento por tipo de exame), `on_met` (efeito), `llm` (backend/params).

## 5. Saída estruturada e audit

O LLM retorna (schema forçado via `json_response_format`):

```json
{ "measures": [ { "name": "nodulo_max", "value": 1.6, "unit": "cm",
                  "evidence": "medindo 1,6 x 1,3 x 1,4 cm", "found": true } ] }
```

O motor adiciona ao `exm_laudo_resultado` (audit, sem PHI além do span):

```json
"quantitative": {
  "nodulo_maior_1cm": { "met": true, "value": 1.6, "unit": "cm",
    "op": ">", "threshold": 1.0, "evidence": "medindo 1,6 x 1,3 x 1,4 cm",
    "source": "llm", "llm_model": "databricks-claude-haiku-4-5" }
}
```

Campos de rastreabilidade: `source` (`llm` | `llm_error` | `skipped_no_anchor` | `skipped_exam_type`), `llm_called`, `llm_error`. Se o LLM falhar/timeout → `met=null`, `source="llm_error"`, **sem** alterar a decisão determinística (fail-safe).

## 6. Efeito na relevância (`on_met`)

- `**annotate_only**` — só grava o audit; não muda `fl_relevante`. (Levantamento/observabilidade.)
- `**gate_relevance**` — o achado-âncora **só conta como relevante se o critério for atendido**. É o caso da **Tireoide V2**: `nódulo` promove **apenas se `> 1 cm`**. Se `met=false`, o achado `nodulo` é rebaixado (não promove).
- `**promote**` — atender o critério **promove** `fl_relevante=1` por si (ex.: HP pré-capilar composto).

`decision_source` ganha valor `quantitative_gate` / `quantitative_promote` quando o critério é decisivo.

## 7. Governança / não-determinismo'

- `temperature=0` → determinismo prático; **sem cache** (decisão de design — evita estado/dúvida de "de onde veio o valor"; cada laudo é avaliado na hora). A homologação registra o **valor extraído + evidência**, não a chamada.
- **Custo:** limitado por gating (âncora + tipo de exame + só-configurados). Estimar por run = nº de laudos com o achado-âncora, não o total.
- **LGPD:** in-tenant obrigatório; `max_input_chars` para truncar (reusa config do backend); evidência gravada é um **span curto**, não o laudo inteiro.

## 8. Piloto — Tireoide `nódulo > 1 cm`

- Config: o bloco `nodulo_maior_1cm` do §4, `on_met: gate_relevance`, `anchor: {finding: nodulo}`.
- Comportamento esperado: em US de tireoide com nódulo, o LLM extrai a maior dimensão; código aplica `>1cm`; nódulos ≤1cm deixam de promover (comportamento V2). Nódulos negados (já resolvidos pela negação) nem chegam à âncora.
- Homologação: CSV de negócio ganha coluna `medida_extraida` + `criterio_atendido` + `evidencia` → revisor valida o número.
- **Reversível:** V1 = remover o bloco (ou `on_met: annotate_only`); V2 = `gate_relevance`.

## 9. Fora de escopo (não-objetivos)

- Construir biblioteca de regex de medidas por especialidade (evitado por decisão de design).
- Deixar o LLM avaliar limiar/`>`/lógica (fica em código).
- LLM "blanket" sobre todos os laudos (só critérios configurados + gated).
- Associação espacial fina (qual lobo) — o `description` guia; refino fica para depois se o negócio pedir.

## 10. Plano incremental

1. **Backend:** `extract_quantitative_llm` (prompt+schema estruturado) reusando `call_openai_compatible_chat`. Testes com laudos sintéticos (mm/cm, "A x B x C", "maior que", faixa).
2. **Avaliador em código:** parsing de `op`/`value`, normalização de unidade (cm↔mm, %), `all_of`/`any_of`.
3. **Integração no `engine.process`:** gating (âncora/tipo de exame), `on_met`, audit fields, fail-safe.
4. **Config piloto** (tireoide) + `temperature=0` (sem cache).
5. **Validação HML** no runner E2E; homologação manual do negócio.
6. Segundo critério (**PMAP+RVP**) p/ provar composto cross-sentença.

## 11. Riscos e mitigações


| Risco                     | Mitigação                                                  |
| ------------------------- | ---------------------------------------------------------- |
| Não-determinismo do LLM   | `temperature=0` (sem cache); homologar o valor + evidência  |
| Custo/latência            | gating por âncora+tipo de exame; só critérios configurados |
| Alucinação de número      | evidência (span) obrigatória na saída; revisor confere     |
| LGPD (texto ao modelo)    | serving in-tenant Databricks; span curto no audit          |
| Falha do LLM              | fail-safe: `met=null`, não altera decisão determinística   |
| Regressão em quem não usa | opt-in; ausência do bloco = no-op byte-compatível          |


