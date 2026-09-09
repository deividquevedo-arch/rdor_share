# Run de validação da `0.12.2` em dev — TI-RADS

Objetivo: provar **no ambiente** o que o A/B local mediu. O A/B isola o delta de decisão; ele não
toca instalação, caminho do runner, nem a camada de LLM da etapa quantitativa. Este run fecha isso.

---

## 0. Pré-condição — sem isto o run falha na instalação

🔴 **A `0.12.2` só é instalável em dev depois que o PR para a `hml` for mergeado.** A esteira
publica no feed `fabrica-ai-hml` a partir da `hml`, e é dele que o `%pip` do runner instala em dev.

Com a versão ausente, a etapa `install` falha assim:

```
ERROR: Could not find a version that satisfies the requirement nlp-engine==0.12.2
       (from versions: 0.9.4, 0.10.0, 0.10.1, 0.11.2)
```

**Conferir antes de rodar:** a lista entre parênteses do erro é o próprio inventário do feed. Se a
`0.12.2` não constar, o merge ainda não propagou.

---

## 1. Coorte — 18/08 em dev

Escolhida por três razões, todas medidas:

| critério | valor |
|---|---|
| baseline com **uma só versão de motor** | `engine_version = 0.11.2` em 100% do dia |
| baseline com **uma só versão de config** | `config_version` única |
| **exercita o caminho corrigido** | **9** entregas `TR4` pelado, contra 4 no melhor dos outros dias |
| volume | 1.000 laudos na saída gravada · 82 entregues |

⚠️ Os 9 são **cota superior**: as quatro condições de `_coordinated_gate_demotes` podem proteger
parte deles. O que o run precisa provar é que **algum** rebaixamento acontece e que **nenhuma**
entrega nova aparece.

---

## 2. Widgets do `ntb_ia_motor_e2e`

| widget | valor | por quê |
|---|---|---|
| `specialty` | `tirads` | |
| `environment` | `dev` | |
| `nlp_engine_version` | **`0.12.2`** | pin explícito; `latest` não serve para validar versão |
| `date_range_enable` | **`true`** | só assim `start_date`/`end_date` valem |
| `start_date` | `2026-08-18` | |
| `end_date` | `2026-08-18` | |
| `limit_rows` | **vazio** | 🔴 ver §2.1 |
| `reprocess_enable` | **`true`** | 🔴 ver §2.2 |
| `model_execution_date` | vazio (= hoje) | separa as linhas novas das gravadas |
| `persist` | `true` | a comparação lê a tabela de saída |
| `persist_input` | `true` | igual ao job de produção |
| `embedding_enable` | `true` | igual ao job; mudar isto trocaria duas variáveis |
| `mlflow_enable` | `true` | |
| `logger_level` | `INFO` | |

### 2.1 `limit_rows` tem de ficar VAZIO

O teto é aplicado **depois** da união da fila, e a ordem é inéditos → pendentes → **reprocessados
por último**. Com um teto, a coorte a remedir fica fora dele e o run **fecha com sucesso sem tocar
nela**. Medido em 02/09: dos 1.000 primeiros gravados, **zero** ids em comum com a coorte.

### 2.2 `reprocess_enable` tem de ficar `true`

A dedup da entrada usa `id_exame` **puro**, sem `config_version` nem `engine_version`: uma janela
já processada fica bloqueada e o run termina em segundos, com sucesso, processando quase nada.
O widget só funciona em `dev` — que é exatamente onde este run acontece.

ℹ️ **Não trocar o `model_version` da config.** O contorno de sufixo de bancada está obsoleto desde
que o `reprocess_enable` existe, e mexer nele muda o nome das tabelas e da view.

### 2.3 Horário

⚠️ **Não iniciar entre 21:00 e 00:00 (BRT).** `dt_execucao_modelo` é gravado em **UTC** e a view de
exportação filtra pela data **local**: nessa faixa a view sai **vazia, sem erro**.

---

## 3. O que conferir depois — três perguntas, nessa ordem

### 3.1 A versão instalada é a que se pediu

```sql
select engine_version, config_version, count(*) laudos
from diamond_fabrica_ia_dev.tirads.tb_mod_diamond_tirads_saida_v0
where date(dt_execucao_modelo) = current_date()
group by 1, 2
```

🔴 Se vier `0.11.2`, o pin não pegou e **nada mais nesta página vale**. É a checagem que o backtest
do ca-cólon faz com `assert`, e é a que ontem teria poupado um run inteiro.

### 3.2 A direção da mudança — o aceite

```sql
with novo as (
  select id_exame, fl_relevante, findings, exm_laudo_resultado
  from diamond_fabrica_ia_dev.tirads.tb_mod_diamond_tirads_saida_v0
  where date(dt_execucao_modelo) = current_date() and engine_version = '0.12.2'
), velho as (
  select id_exame, fl_relevante, findings
  from diamond_fabrica_ia_dev.tirads.tb_mod_diamond_tirads_saida_v0
  where engine_version = '0.11.2' and date(dt_exame) = '2026-08-18'
)
select v.fl_relevante antes, n.fl_relevante depois, count(*) laudos
from velho v join novo n on v.id_exame = n.id_exame
group by 1, 2 order by 1, 2
```

**Aceite:** só existem `1 → 0` e as diagonais. **Qualquer `0 → 1` reprova.**

### 3.3 Cada rebaixamento carrega a chave que o explica

```sql
with novo as (
  select id_exame, fl_relevante, exm_laudo_resultado
  from diamond_fabrica_ia_dev.tirads.tb_mod_diamond_tirads_saida_v0
  where date(dt_execucao_modelo) = current_date() and engine_version = '0.12.2'
), velho as (
  select id_exame, fl_relevante
  from diamond_fabrica_ia_dev.tirads.tb_mod_diamond_tirads_saida_v0
  where engine_version = '0.11.2' and date(dt_exame) = '2026-08-18'
)
select count(*) rebaixados,
       sum(case when instr(n.exm_laudo_resultado, 'require_measure_no_anchor') > 0
                then 1 else 0 end) com_a_chave
from velho v join novo n on v.id_exame = n.id_exame
where v.fl_relevante = '1' and n.fl_relevante = '0'
```

**Aceite:** `com_a_chave` = `rebaixados`. Rebaixamento sem a chave é achado, não confirmação.

⚠️ **`require_measure_no_anchor` é campo POR CRITÉRIO**, dentro de `quantitative.<criterio>`. Não
confundir com chaves de topo — foi assim que uma comparação inteira se perdeu em 08/09. O `instr`
acima acha os dois; para separar, ler o JSON em vez de casar texto.

### 3.4 Pré-condição, se o resultado der zero

```sql
select count(*) exercitam_o_caminho
from diamond_fabrica_ia_dev.tirads.tb_mod_diamond_tirads_saida_v0
where date(dt_execucao_modelo) = current_date() and engine_version = '0.12.2'
  and instr(exm_laudo_resultado, 'require_measure_no_anchor') > 0
```

🔴 **Zero divergência com zero laudos nesta condição não é aprovação — é medição vazia.**

---

## 4. O que este run NÃO decide

- **Não homologa.** A `0.12.2` remove entrega do laudo de **punção aspirativa**, que é o caso de
  maior suspeição clínica. Se o negócio quiser esses laudos de volta, o caminho é a exceção de
  PAAF, não desfazer esta correção.
- **Não mede taxa absoluta.** Dev tem corpus e volume próprios.
