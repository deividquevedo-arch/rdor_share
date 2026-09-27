# Proposta de ajuste — SPEC 27 (`27-config-especialidade.md`)

**Data:** 2026-08-24 · **Para:** Diego, João, Gabriel · **De:** DS / motor NLP
**Para:** avaliação, validação e ajuste. Nada aqui é alteração feita — é proposta.

> **A documentação é de vocês; o defeito é nosso.** Mudamos o formato canônico de `findings` na
> `nlp_engine 0.6.0`, mantivemos os dois aceitos, e **não atualizamos a doc que vocês leem**.
> Compatível respondia *"vai quebrar?"*. Não respondia *"como eu escrevo agora?"* — e é essa a
> pergunta que a SPEC 27 responde.
>
> Em 20/08 isso cobrou: a doc descreve um formato, a régua do TI-RADS usa o outro, e a proposta
> de correção que nasceu daí teria desmontado a V2 que está em `hml`.

---

## Ajuste 1 · `findings` tem dois formatos, e a SPEC documenta um

**Onde:** §4.1, tabela do núcleo.

**Está escrito:** `findings` · `dict[str, list[str]]` · *categoria → termos*

**O problema:** esse é o formato **v1**. Desde a `0.6.0` o canônico é **por entidade**, e é o que
tirads e ca-estômago usam em produção e em hml:

```python
'findings': {
    'neoplasia': {
        'label':  'Neoplasia',                  # nome de negócio exibido na saída
        'terms':  ['neoplasia', 'carcinoma'],
        'regex':  ['\bneoplasia[s]?\b'],
        'exclude': [...],                       # variante que não conta
        'unless':  [...],                       # exceção à exclusão
        'skip_organ_gate': True,                # achado que não exige o órgão perto
    },
}
```

Os dois continuam aceitos — a detecção é estrutural, não há data de corte. Mas quem lê a SPEC hoje
escreve o antigo e **não tem como descobrir o outro**.

**Proposta:** documentar os dois, marcar o por-entidade como **canônico desde a `0.6.0`**, e trocar
o exemplo — a SPEC cita o TI-RADS como exemplo do formato plano, e o TI-RADS usa o outro.

---

## Ajuste 2 · Três blocos de §4.3 mudaram de lugar e a SPEC não diz

**Onde:** §4.3, tabela de blocos opcionais.

**Está escrito:** `findings_exclusion_terms`, `findings_skip_organ_gate`, `findings_ignore_sections`
como blocos **top-level**, com o tirads de exemplo.

**O problema:** no formato por entidade essas três viram **chaves dentro de cada achado**
(`exclude`, `skip_organ_gate`) ou dentro de `findings_policy`. E existe uma **precedência não
documentada**: `findings_policy.ignore_sections` **sobrescreve** o top-level
`findings_ignore_sections`. O sintoma de errar isso é E2E **byte-idêntico** entre duas versões —
nenhum erro, nenhuma diferença, e a impressão de que a mudança não fez nada.

**Proposta:** documentar as duas formas de expressar cada uma e **declarar a precedência**.

---

## Ajuste 3 · 🔴 §6.1 diz que `runtime` não é lido, e ele é

**Onde:** §6.1, primeira linha.

**Está escrito:** 🚫 *"Nada neste pipeline lê `runtime`."*

**O problema:** o runner **mescla `runtime.llm_router` sobre `nlp.llm_router`** em
`ntb_ia_loader.py:105-113`. Não é detalhe: **hepatologia e transplante_pulmao não declaram
`enabled` no `nlp.llm_router`**, e a lib assume `False`. As duas só têm juiz ligado **por causa
desse merge**.

Consequência direta: quem limpar o bloco `runtime` confiando nesta linha **desliga o juiz LLM em
produção — sem erro, sem log e sem mudança de resultado visível no dia**.

**Proposta:** decidir com o Gabriel e documentar a decisão, das duas uma:
- o merge **fica** → a SPEC descreve a precedência (`runtime` sobre `nlp`); ou
- o merge **sai** → as duas configs declaram `enabled: True` **antes** de ele sair.

⚠️ **Não inverter a ordem.** Remover o merge antes de declarar derruba o juiz nas duas.
Card `283644`, em execução do nosso lado.

---

## Ajuste 4 · §4.2 diz que não existe corte, e existe

**Onde:** §4.2, "Como a relevância é decidida".

**Está escrito:** *"O `confidence_score` é informativo — não é um corte. Não existe threshold no motor."*

**O problema:** é verdade para `fl_relevante` diretamente, e **enganoso** para quem tem juiz LLM.
A `llm_router.uncertainty_band` é um par de cortes sobre exatamente esse score: ela decide **quem
vai ao juiz**, e o juiz muda `fl_relevante`. No ca-estômago a régua inteira depende disso — a banda
`[0.60, 0.97]` é o que implementa a cascata regra → expansão → juiz, e o corte inferior vem do teto
**analítico** de um laudo sem achado (0,597).

Quem calibrar seguindo esta seção não entende por que mexer em peso mudou resultado.

**Proposta:** manter a frase para o caminho determinístico e acrescentar que, **com `llm_router`,
o score passa a ter função de corte** através da banda.

---

## Ajuste 5 · 🔴 O requisito de versão do motor não existe como campo

**Onde:** não existe seção. É o buraco.

**O que encontramos hoje, nas quatro configs:**

| especialidade | onde o requisito está declarado | estado |
|---|---|---|
| hepatologia | **em lugar nenhum** | 🔴 em produção, com juiz LLM, sem declarar motor |
| cancer_estomago | comentário solto, linha 171: *"Requer nlp_engine >= 0.6.3"* | 🔴 **errado** — a régua `0.6.2` depende da `0.9.4` |
| tirads | espalhado em 3 linhas de changelog (`0.8.5`, `0.9.2`) | ⚠️ nenhuma canônica |
| transplante_pulmao | dentro de um comentário de parâmetro (`0.7.3`) | ⚠️ |

Quatro especialidades, quatro convenções, nenhuma legível por máquina, **uma ausente e uma errada**.

No incidente de 20/08 a pergunta era exatamente *"qual motor esta config precisa?"*. Com um campo,
o diagnóstico teria sido imediato. E o fluxo novo — **versão fixada por especialidade** — torna isso
obrigatório: a fixação sem declaração é combinado verbal.

**Proposta:** campo `engine_min_version` no bloco de identificação, e o runner **falha na carga**
se a wheel instalada for menor. Falhar alto na etapa `install` custa um run; falhar em silêncio
custou uma queda em produção.

---

## O que nós fazemos do nosso lado

**A mensagem de erro que induziu a correção destrutiva.** Hoje:

```
nlp.findings values must be list[str]
```

Ela afirma que só existe um formato. Proposta:

```
nlp.findings['<chave>']: recebido <tipo>.
Formatos aceitos:
  (a) list[str]  — lista de termos            (formato v1, aceito)
  (b) dict       — {'terms': [...], 'regex': [...], 'exclude': [...], 'label': ...}
                                              (canônico desde nlp_engine 0.6.0)
Recebido: <repr truncado>
```

Sai na `0.10.0`, junto com o log estruturado que vocês pediram.

---

## O que pedimos

1. **Validar o Ajuste 3** — só vocês sabem se o merge do `runtime` fica ou sai. É o único aqui que
   pode derrubar produção, nos dois sentidos.
2. **Aceitar ou recusar `engine_min_version`** (Ajuste 5). Se aceitarem, a validação é no runner.
3. Dizer se preferem que a gente **abra PR na SPEC 27** com os ajustes 1, 2 e 4 escritos, ou se
   preferem escrever com esta proposta como insumo.

---

## O que esta proposta NÃO faz

- **Não propõe achatar `findings` para `list[str]`.** Destruiria `regex`, `exclude`, `unless`,
  `label` e `skip_organ_gate` — a régua do TI-RADS V2 em hml e a do ca-estômago dependem deles.
- **Não muda comportamento do motor.** Os cinco ajustes são documentação e um campo novo.
temos - **Não trata dos 28 cards de engenharia da lib**, que estão no documento do Gabriel.
    