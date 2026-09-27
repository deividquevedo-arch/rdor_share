# DII — o que ajustar antes de abrir o PR

> Doenças inflamatórias intestinais · 16/09/2026 · branch
> `doenca_inflamatoria_intestinal/feature/migracao-config-motor`, config
> `0.2.1-doenca_inflamatoria_intestinal`

A medição está sólida e a sequência pedida em 14/09 foi executada inteira. O `document_vet`
resolveu o caso da colonoscopia por config pura, e a feature na lib deixou de ser necessária —
F1 **0,754 → 0,958** sobre 18.480 laudos, com o `gold_filter` medido nos dois sentidos e a
plataforma separada da bancada (0,02% de divergência em 17.494).

**Ok para abrir o PR**, com os ajustes abaixo.

---

# 1. Ligar a camada semântica e o juiz

**É o item principal, e é o que falta para a linha sair do estágio de desenvolvimento.**

A config declara `decision_mode: 'hybrid'` com `use_embeddings: False` e
`llm_router.enabled: False`. O bloco fica inerte e quem lê conclui o contrário. **A saída é ativar,
não remover.**

A régua de entrega ao negócio: o alvo de toda especialidade é o fluxo completo — regra → híbrido
calibrado → juiz. `rule_only` é estágio de desenvolvimento, e nenhuma lista vai ao negócio a partir
de perfil parcial.

## 1.1 São três chaves, em dois arquivos, e as três andam juntas

```diff
  'embeddings': {
-     'use_embeddings': False,
+     'use_embeddings': True,
      'decision_mode': 'hybrid',
```

```diff
  'llm_router': {
-     'enabled': False,
+     'enabled': True,
```

```diff
  # jobs/definicoes/doenca-inflamatoria-intestinal-batch.json
- "embedding_enable": "false"
+ "embedding_enable": "true"
```

🔴 **O widget é o passo que passa despercebido.** Ele decide se a wheel instala o extra
`[embeddings]`. Sem ele, `use_embeddings: True` **não instala a biblioteca**, e a semântica cai em
`token_overlap` sem erro e sem log — o perfil medido não seria híbrido.

**Não é hipótese:** em dev, TI-RADS, últimos 30 dias — **4.543 de 17.896 laudos (25,4%) com
`FALLBACK:ModuleNotFoundError`**.

## 1.2 A branch não traz definição de job

São 4 arquivos: as três navegações e a config. Sem `jobs/definicoes/` e `jobs/clusters/` não existe
onde declarar o widget, e a linha não roda pelo runner. As outras sete linhas têm os dois, e a
ateromatose levou-os no mesmo PR da config.

## 1.3 Dois números a calibrar antes de fixar

⚠️ **`similarity_threshold: 0.80` é frouxo para ligar assim.** Em `hybrid` a promoção é direta:
`fl == 0` com `semantic_score >= threshold` vira `fl = 1`. O `cancer_rim` usa **0.92**, declarado
como conservador justamente para nenhum candidato ser promovido sem validação humana. O PR já
carrega 17 falsos positivos contra 4 do legado — a 0,80 esse número pode crescer - vale testar/calibrar.

⚠️ **`ambiguity_band: [0.3, 0.7]` é inerte.** A chave só é lida em `decision_mode: 'fallback'`
(`decision_pipeline.py:558`). Em `hybrid` quem decide é o `similarity_threshold`.

## 1.4 O bloco do juiz está incompleto

```python
'llm_router': {'enabled': False, 'mode': 'llm', 'provider': 'openai_compatible',
               'api_key_env': 'DATABRICKS_TOKEN', 'fallback_policy': 'keep_current',
               'json_response_format': False}
```

Faltam **`model`**, **`uncertainty_band`**, **`max_input_chars`**, **`prompt_system`** e
**`specialty_context`**. Trocar `enabled` para `True` assim roda o juiz sem prompt de especialidade
e na banda default `(0.35, 0.65)`, que é o que a lib assume quando a chave falta.

⚠️ **A banda default provavelmente não serve.** O modo `hybrid` **recalcula o score** (peso default
`0.7` régua / `0.3` semântica) e desloca a escala. No `cancer_rim` a banda teve de ir para
`[0.75, 0.95]` por causa disso. Banda errada dá `llm_called = 0` — juiz habilitado e nunca acionado.

**Referência de formato:** `ntb_ia_cancer_rim_config.py` na `hml` — traz os dois blocos completos,
com o porquê de cada número comentado ao lado. **Formato**, não conteúdo: o prompt do ca-rim é a
régua de oncologia renal e não se copia para cá.

## 1.5 🔴 O prompt do juiz se escreve a partir dos deltas, não antes deles

**Prompt genérico não entra.** O juiz existe para resolver a ambiguidade que sobra e para vetar o
que seria errado sinalizar como relevante — e o que é ambíguo nesta linha é fato que os dados já
mostram, não algo a supor.

**Duas invariantes antes de escrever qualquer frase:**

1. **O juiz só FILTRA.** A lib não o deixa promover sem evidência de regra. Toda inclusão de escopo
   vira **achado na régua**, nunca instrução no prompt.
2. **O prompt descreve o que sobra de ambíguo depois que a régua já decidiu** — não a régua inteira
   reescrita em prosa.

### A matéria-prima já está medida, e a maior parte dela não é do juiz

Os 17 falsos positivos já estão classificados na avaliação. Separando por onde cada um se resolve:

| categoria do FP | n | onde se resolve |
|---|---|---|
| negação a distância | **8** | **régua** — `direction_default` (§4) e a janela; não é prompt |
| anatomia fora do trato | 3 | **régua** — organ gate / `exclude`; só vira prompt se vier na mesma frase do achado |
| referência ao passado | **4** | **candidato a prompt** — temporalidade é o que a régua expressa pior |
| recomendação de RM | 2 | **régua** — `soft_findings` + `document_vet`, o mesmo mecanismo já em uso |

🔴 **Levar os 17 ao prompt seria trocar defeito de config por chamada de LLM.** Custa token, não
deixa rastro auditável e esconde a causa. Corrija a régua primeiro; o juiz fica com o resíduo.

### E o resíduo de verdade só aparece depois de ligar a semântica

Em `hybrid`, todo laudo com `fl = 0` e `semantic_score >= similarity_threshold` **vira `fl = 1`**.
Essa é uma população que **não existe** em `rule_only` — e é exatamente onde a ambiguidade mora.
É dela que sai a maior parte do prompt.

### Procedimento — cinco passos, cada um produzindo um insumo do prompt

**1. Corrigir a régua** nos itens da tabela acima e remedir na mesma janela. A pergunta que fecha o
passo: **quantos dos 17 sobram?**

**2. Rodar o híbrido SEM o juiz**, mesma janela dos 18.480. Isolar a população nova pelo **delta
contra a corrida `rule_only`**, por `id_exame` — não por flag. ⚠️ `decision_source` sai como
`hybrid` em **todos** os laudos que passam pela camada, promovidos ou não; ele não isola nada.

```sql
SELECT h.id_exame,
       get_json_object(h.exm_laudo_resultado,'$.semantic_score')        semantic_score,
       get_json_object(h.exm_laudo_resultado,'$.semantic_matched_term') termo,
       get_json_object(h.exm_laudo_resultado,'$.semantic_evidence')     frase
FROM   <saida_hibrido> h
JOIN   <saida_rule_only> r USING (id_exame)
WHERE  r.fl_relevante = 0 AND h.fl_relevante = 1
ORDER BY semantic_score
```

**3. Ler os casos** — os FP residuais do passo 1 e as promoções semânticas do passo 2. **A frase que
torna cada um errado é o que entra no prompt.** Registrar, para cada condição, quantos laudos ela
explica: condição que aparece uma vez não é régua, é ruído.

**4. Escrever o prompt com essas condições**, em forma de **critério de exclusão**, e nada além
delas. `prompt_system` no formato de uma chave JSON (`{"relevante": boolean}`), como nas outras
linhas; `specialty_context` com a tarefa, o que navega e — a parte que vem dos passos 1 a 3 — o que
**não** navega, com as palavras que os laudos realmente usam.

**5. Ligar o juiz e medir o que ele remove.** O delta agora é `1 → 0` contra a corrida do passo 2,
cruzado com `llm_called = true`:

- quantos ele remove, e **quantos dos removidos eram FP de verdade**;
- se derrubar verdadeiro positivo, **o que se ajusta é o prompt**, não o `enabled`;
- se `llm_called` vier zero, o que se ajusta é a `uncertainty_band`.

### O precedente que sustenta o método

No **câncer de estômago** a régua sozinha entregava 75 laudos com 55 errados — precisão **0,267**. O
que levou a **0,929** não foi tirar o juiz: foi **alinhar o prompt à regra de negócio**, em **seis
versões medidas, três revertidas**. O prompt é parâmetro calibrado, e calibra-se contra caso real.

---

# 2. Remover o bloco `runtime` — no MESMO commit do item 1

```diff
- 'runtime': {'profile': 'rule_only',
-             'llm_router': {'enabled': False, ...}},
```

O bloco existia para sobreposição via widget durante testes. Nenhum dos 15 widgets do runner o
alimenta hoje — perdeu a função, e sai de todas as configs.

🔴 **E aqui remover deixou de ser opcional.** O `runtime` **sobrepõe** o `nlp` no carregamento
(`ntb_ia_loader.py:104-113`). Ligar `nlp.llm_router.enabled: True` e deixar o `runtime` com `False`
mantém o juiz desligado — sem erro e sem log, com o run passando normalmente.

---

# 3. Cabeçalho: explicar o `document_vet`

```python
'document_vet': {'enabled': True,
                 'soft_findings': ['recomendacao_colonoscopia'],
                 'normality_phrases': [... 13 frases ...]}
```

Funciona, e é a solução certa. Mas é **uso não óbvio da chave**: `normality_phrases` nomeia
*enunciado de normalidade* e aqui carrega **boilerplate de laudo de colonoscopia**. O cabeçalho tem
96 linhas e não menciona `document_vet` nenhuma vez.

**Um parágrafo** dizendo que o termo `colonoscopia` é achado legítimo em laudo de imagem e ruído em
laudo de colonoscopia, e que o `document_vet` é o mecanismo que separa os dois pelo conteúdo do
próprio laudo. É condição que acompanha a proposta desde que ela foi feita.

---

# 4. Verificar: `negation.direction_default: None`

A chave está **presente com valor `None`**. O carregador faz:

```python
if "direction_default" in negation:
    direction["_default"] = negation["direction_default"]
```

Como a chave existe, `_default` é setado **para `None`** — não cai no default da lib, que desde a
`0.11.1` é `left`.

🔴 **E há um sintoma que pode ser isto:** dos 17 falsos positivos, **8 são negações a distância**.
Se a direção default não está valendo, o negador pode estar sendo procurado no lado errado.

**Confirmar se `None` é deliberado.** Se não for, remover a chave ou declarar `'left'`, e remedir
os 8.

---

# 5. Remedir — na mesma janela, e levar só o delta

Ligar a semântica e o juiz **muda o perfil**: o híbrido tende a subir recall e o juiz tende a
derrubar. As métricas atuais (F1 0,958, MCC 0,958, 17 FP) foram medidas em `rule_only` e **deixam de
valer**.

**Como remedir, sem refazer a homologação inteira — sempre na janela dos 18.480 laudos já medida:**

| # | corrida | o que ela responde |
|---|---|---|
| 0 | `rule_only` atual | é a baseline; já existe |
| 1 | `rule_only` **com a régua corrigida** (§1.5 passo 1 e §4) | quantos dos 17 FP sobram sem LLM nenhum |
| 2 | **híbrido, sem juiz** | o que a semântica acrescenta — e é a população que alimenta o prompt |
| 3 | **híbrido + juiz** | o que o juiz remove, e se ele remove o certo |

⚠️ **Uma variável por corrida.** Ligar semântica e juiz juntos e comparar contra a baseline mistura
dois efeitos opostos — o híbrido sobe recall, o juiz derruba — e o número resultante não atribui
nada.

Ao negócio vai apenas o conjunto de **discordâncias** entre a corrida 0 e a corrida 3. Re-homologa-se
o **delta**, não o todo.

No cabeçalho, junto das métricas: `semantic_score` não nulo, zero `FALLBACK`, `llm_called` > 0, o F1
do perfil novo, e a **versão do prompt** que o produziu.

**O aceite da run em dev**, sobre a tabela de saída:

```sql
SELECT count(*) laudos,
       sum(CASE WHEN get_json_object(exm_laudo_resultado,'$.semantic_score') IS NOT NULL
                THEN 1 ELSE 0 END) camada_rodou,
       sum(CASE WHEN exm_laudo_resultado LIKE '%FALLBACK%' THEN 1 ELSE 0 END) degradou,
       sum(CASE WHEN get_json_object(exm_laudo_resultado,'$.llm_called')='true'
                THEN 1 ELSE 0 END) juiz_chamado
FROM diamond_fabrica_ia_dev.doenca_inflamatoria_intestinal.tb_mod_diamond_doenca_inflamatoria_intestinal_saida_v0
WHERE dt_execucao_modelo >= current_date()-1
```

`camada_rodou` = `laudos` · `degradou` = 0 · `juiz_chamado` > 0. Se `juiz_chamado` vier zero, o que
se calibra é a `uncertainty_band`, não o `enabled`.

---

# 6. O que não bloqueia o PR

- **`embedding_model` em produção** — a config já aponta para `gold_fabrica_ia_hml`, que é o Volume
  do workspace novo e o certo. Em prd o caminho ainda não existe; está com a plataforma no card
  `305810` — *[Plataforma NLP] Modelo de embeddings sem caminho válido em produção*. Valide em dev,
  suba, e acompanhe. ⚠️ Mas saiba que validar em dev **não diz nada sobre prd** neste ponto: são
  dois defeitos diferentes, `ModuleNotFoundError` em dev e `FileNotFoundError` em prd.
- **O grant `USE CATALOG security`** — é a mesma pendência que aparece no PR 7275 da ateromatose e,
  provavelmente, na view do TI-RADS que entrega base64. **É um item, não três**, e vai à plataforma
  como item único.
- **O gabarito de 206 é derivado da própria comparação** entre as duas réguas. Não invalida, mas o
  F1 precisa aparecer com essa ressalva onde o número for publicado, para ninguém o ler como acerto
  clínico. Os 96 laudos que a régua nova marca e o legado não são o que vai à revisão do negócio.
- **Eixo de data** — plataforma por `dt_dia_exame`, legado por `dt_liberacao_laudo`. É nota de
  plataforma, vale para todas as linhas, e ainda não tem card.
