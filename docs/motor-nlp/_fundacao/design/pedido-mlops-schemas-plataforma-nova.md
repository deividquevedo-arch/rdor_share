# Pedido ao MLOps — schemas nos catálogos `diamond_fabrica_ia*`

**Data:** 2026-08-07 · **Para:** MLOps · **Assunto:** provisionamento de schema para 3 especialidades

---

## 1. O pedido

Com a migração de catálogo (`diamond_ia_*` → `diamond_fabrica_ia_*`), três especialidades ficaram
sem schema e não conseguem rodar.

| catálogo | schemas a criar |
|---|---|
| `diamond_fabrica_ia_dev` | `tirads` · `tireoide` |
| `diamond_fabrica_ia_hml` | `tirads` · `tireoide` · `transplante_pulmao` |
| `diamond_fabrica_ia` (prd) | `tirads` · `tireoide` · `transplante_pulmao` |

**Por que não se resolve sozinho:** o runner monta as tabelas como
`{catalog}.{specialty_id}.tb_mod_diamond_{specialty_id}_*` e executa `CREATE TABLE IF NOT EXISTS`.
Ele **cria a tabela, mas não cria o schema** — sem o schema, o estágio de setup falha.

O nome do schema **é** o `specialty_id` e não é configurável.

### Situação conferida em 2026-08-07

```
diamond_fabrica_ia_dev : hepatologia ✓ · transplante_pulmao ✓ · tirads ✗ · tireoide ✗
diamond_fabrica_ia_hml : hepatologia ✓ · transplante_pulmao ✗ · tirads ✗ · tireoide ✗
diamond_fabrica_ia     : hepatologia ✓ · transplante_pulmao ✗ · tirads ✗ · tireoide ✗
```

---

## 2. Impacto por especialidade

**TI-RADS** — validado (recall 1,000 · MCC 0,983 contra base ouro), pronto para entrega. Não roda
em nenhum ambiente.

**Transplante de Pulmão** — já entregue e validado 1:1 (126 TP · 0 FP · 0 FN · 1.726 TN). Roda em
`dev`, mas falha ao ser promovido para HML/PRD.

**Tireoide (V3)** — é a novidade deste pedido, e a justificativa está abaixo.

---

## 3. Por que o Tireoide precisa migrar para a plataforma nova

Não é preferência de ambiente: **no runner legado a régua de exame de sangue não tem como
funcionar.**

### O que a linha de cuidado precisa

O Tireoide V3 passou a rastrear **hipertireoidismo e doença de Graves por exame de sangue** (TSH,
T4 livre, TRAb, Anti-TPO), além dos achados de imagem. Regra de negócio definida em 2026-08-03:
sangue e imagem valem **isoladamente** — um TSH suprimido captura o paciente por si só.

### O bloqueio técnico no runner legado

O resultado laboratorial vem em `proced_lista_exames`, que é um **array de componentes rotulados**:

```
item[0] → nme_exame = "METODO"      laudo = "Método..: ELETROQUIMIOLUMINESCÊNCIA"
item[1] → nme_exame = "Resultado"   laudo = "4.25"     <- o valor
```

O `extract_laudo` do runner legado (`fabrica_ia/nlp_platform/batch/entrada.py:21-45`) lê **apenas o
primeiro item do array**:

```python
item = lista[0]
return (item.get("laudo_original") or item.get("laudo_transformado") or "").strip()
```

Quando o primeiro item é o método ou o material, **o valor nunca chega ao motor**.

**Medido no run de 21/06/2026 (1.576 laudos, 329 de sangue):**

| | |
|---|---|
| valor lido com sucesso | **63** |
| valor ausente do texto recebido | **264** |

Os 264 chegaram ao motor como `"Método..: ELETROQUIMIOLUMINESCÊNCIA"` ou `"MATERIAL: SORO"` —
sem número algum. Não é erro de régua nem da lib: o dado foi truncado antes.

### Por que a plataforma nova resolve

O pipeline de entrada de vocês concatena o **array inteiro** antes de mapear
(`plataform/pipeline_e2e/nlp_ia_02_input.py:395`):

```python
F.array_join(F.transform("proced_lista_exames", lambda x: x["laudo_original"]), "\n")
```

Com isso o texto chega completo (`"Método..: ELETROQUIMIOLUMINESCÊNCIA\n4.25"`), e o motor extrai
o valor corretamente — o `nlp_engine 0.8.0` descarta os segmentos de método/material e lê o
número, de forma determinística e **sem chamar LLM**.

### A alternativa que descartamos

Corrigir o `extract_laudo` na `fabrica-ia-lib` seriam poucas linhas, mas: (a) alterar aquela lib
está fora do escopo definido para esta frente; (b) o repositório será descontinuado. Preferimos
migrar do que remendar.

---

## 4. O que muda para vocês

**Nada além do schema.** A config do Tireoide segue o mesmo padrão das outras três já portadas
(`plataform/config/speciality/`), e a saída ganha as mesmas colunas de achados
(`findings` / `findings_spans` / `findings_match`), criadas automaticamente pelo `mergeSchema`.

Os critérios de sangue **não chamam LLM** — são leitura determinística de valor. Cerca de 56 mil
exames laboratoriais/mês entram no volume processado sem custo proporcional de inferência.

Requer `nlp_engine >= 0.8.0` (já publicado, tag `v0.8.0`).

---

## 5. Duas perguntas

1. **A validação deve continuar em `dev`?** A hepatologia já tem tabelas nos três catálogos novos,
   o que sugere promoção em andamento. Se devemos acompanhar, nos digam para ajustar.
2. **Há processo formal de provisionamento de schema?** Se sim, seguimos por ele.

---

## 6. Pendência anterior, ainda aberta

O modelo de embeddings (`st_models/`) continua em `/Volumes/diamond_ia_hml/...`, enquanto a lib
passou para `gold_fabrica_ia_hml`. TI-RADS e hepatologia apontam para o Volume antigo.

Hoje funciona — conferido: `sentence_transformers` em 2.711 de 2.711 laudos, zero fallback. Mas no
dia em que o Volume antigo for limpo, os dois **degradam para `token_overlap` sem erro, sem log e
sem alerta**: a métrica cai e nada aponta a causa.

Com a migração de catálogo em curso, o risco deixou de ser hipotético. Pedimos a cópia de
`st_models/` para o Volume novo antes de qualquer limpeza.
