# Checkpoint — 2026-08-07

Retomada prevista: **segunda-feira**. Este documento é o ponto de partida.

---

## 1. O que fechou nesta semana

**Transplante de pulmão — ENTREGUE ao MLOps** (card 246669). Validação 1:1 na plataforma nova:
1.852 laudos da coorte, TP 126 · FP 0 · FN 0 · TN 1.726, recall e MCC 1,000.

**TI-RADS V2 — VALIDADO, aguardando subida.** Run completo com `nlp_engine 0.8.0`: 2.709 laudos,
TP 121 · FP 3 · FN 0, precisão 0,9758 · recall 1,000 · MCC 0,9832 — **acima da referência
homologada**. Labels clínicos confirmados na saída (`Nódulo`, `Cisto`, `Linfonodomegalia`).

**`nlp_engine` 0.7.2 → 0.8.0**, cinco versões com tag e mutante morto em cada uma:

| versão | o que resolveu |
|---|---|
| 0.7.3 | critério multi-medida perdia a medida na coluna |
| 0.7.4 | `label` da config não chegava ao audit |
| 0.7.5 | limiar de medida única sumia da coluna |
| 0.7.6 | `label` também para achado léxico |
| **0.8.0** | **medida determinística do texto** (`measure.source.kind: value_text`) para laboratorial |

---

## 2. Aguardando terceiros

| # | pendência | com quem | destrava |
|---|---|---|---|
| 1 | régua do ca_estômago — lesão pré-maligna entra? | PO + negócio (Carol) | toda a frente de ca_estômago |
| 2 | subida das configs para `hml` | João (em andamento) | branch V3 do tirads |
| 3 | schemas `tirads`/`tireoide` nos catálogos novos | MLOps | rodar tirads na plataforma nova |
| 4 | janela da hepatologia | time | validação da hepatologia |
| 5 | limiares de sangue (T3 fora desta versão) | Lucas | refino, **não bloqueia** |

---

## 3. Frente por frente

### 3.1 Câncer de estômago — PARADO por decisão de negócio

O retorno de negócio da `0.1.8` (37 laudos, 15 positivos) deu **recall 0,53** — TP 8 · FP 3 ·
FN 7 · TN 18. É o primeiro gabarito que inclui laudos que o motor **não** marcou; os anteriores
vinham dos próprios positivos do motor, então falso-negativo nunca aparecia.

**Diagnóstico dos 7 FN:** nenhum tem neoplasia/tumor/massa/linfoma. São pólipo, gastrite atrófica,
metaplasia e úlcera de Sakita — **lesões pré-malignas, que a régua V1 excluiu deliberadamente**.
O motor fez o que foi especificado.

**Por que não corrigimos:** dos 3.189 negativos do corpus limpo, **1.575 (49%) mencionam esses
termos**. Incluir dobraria o encaminhamento — é dimensionamento de linha de cuidado, não config.

**Prontos para postar:**
- `docs/motor-nlp/cancer_estomago/justificativa-lote3-revisao-regua.md`
- `Downloads/homologacao_Cancer_Estomago_lote3_v0.1.8.xlsx` — 348 laudos
- mapeamento dos estratos: `docs/motor-nlp/cancer_estomago/dados/lote3-estratos-2026-08-06.csv`

⚠️ A planilha vai **embaralhada e sem indicar o estrato** — mostrar enviesaria a resposta. O cruzamento
é pelo CSV acima, do nosso lado.

**Estratos:** 18 positivos do motor (precisão) · 250 negativos aleatórios (recall, primeira medição
real) · 80 enriquecidos com termo pré-maligno (**decide a régua**).

**Versão homologada continua a `0.1.8`.** A `0.1.13` foi avaliada e é pior — introduz FN sem
eliminar FP. Não promover.

**Corpus disponível:** 3.207 laudos limpos (maio 1.729 + junho 1.478), de 10.037 processados. Só
32% dos laudos têm conteúdo diagnóstico — limitação de origem, não de algoritmo.

### 3.2 Tireoide V3 + sangue — pronto, esperando a hml

**Config no legado:** `feature/v3-tireoide`, commit `9942748`, `0.4.1-tireoide-v3-sangue`.
4 critérios `value_text` (TSH < 0,4 · T4L > 1,8 ng/dL · TRAb > 1,5 · Anti-TPO > 34), todos
`promote`, sem LLM.

**⚠️ NÃO roda no legado, e não é problema de config.** O `extract_laudo` da `fabrica_ia`
(`nlp_platform/batch/entrada.py:21-45`) lê **apenas `lista[0]`** do `proced_lista_exames`; quando o
primeiro item é `METODO` ou `MATERIAL`, o valor nunca chega ao motor. Medido no run de 21/06:
**63 valores lidos, 264 truncados**.

A plataforma nova resolve porque concatena o array inteiro (`nlp_ia_02_input.py:395`).

**Próximo passo:** após a subida do João, criar `tirads/feature/v3-sangue` a partir de `hml` e
portar. `specialty_id` continua `tirads` — sem projeto novo. `config_version` sugerido:
**`0.2.0-tirads`**, com *Impacto na métrica: NÃO comparável*.

**Adaptação necessária:** na plataforma nova `gold_query` não é lido; a seleção é
`gold_filter.keywords` sobre o **nome do exame** — o que facilita, porque as keywords de sangue
funcionam ali sem o problema do `\b` nem do filtro por texto.

### 3.3 Plataforma nova — stand-by

Branch `feature/validacao-plataforma` em `faf8761` (remoto = backup). O João está levando para a
`hml`; nosso PR criaria conflito.

Backup local do merge da `hml`: branch `backup/merge-hml-2026-08-07` (`3919015`), caso a mudança de
catálogo precise ser reaproveitada.

⚠️ **O catálogo mudou:** `diamond_ia_*` → `diamond_fabrica_ia_*`. Toda a validação atual está no
catálogo **antigo**. Consultas de comparação precisam apontar para o certo.

---

## 4. Achados que valem para além da frente onde apareceram

**`\b` não funciona em `gold_query`.** Vira BACKSPACE no literal de string do Spark SQL. Medido:
`\btsh\b` → 0 matches, `tsh` → 391. **O mesmo defeito existe na config de ca_estômago**
(`'endoscop.a digestiva alta|\x08EDA\x08'`) — o termo `EDA` nunca filtrou nada. Vale auditar as
demais configs.

**`gold_filter` tem semânticas OPOSTAS nas duas plataformas.** No legado é substring no **texto do
laudo**, e descarta a linha (`entrada.py:92`). Na nova é `rlike` no **nome do exame**. Mesmo nome,
critério diferente.

**A view de export não filtra execução.** `vw_mod_diamond_<schema>_export_v0` lê a tabela de saída
sem `WHERE` — no tirads são **10.843 linhas para ~2.700 laudos**, o mesmo exame repetido por run.
Vale para as três especialidades e **já acontece hoje**.

**`engine_version` não é confiável.** Lê metadata em disco; cluster quente mantém o módulo antigo.
No runner legado a coluna é da `fabrica_ia` (0.5.8), não do `nlp_engine`. Único identificador
confiável é o `config_version`.

**Embeddings degradam em silêncio.** `st_models/` continua em `diamond_ia_hml` enquanto a lib foi
para `gold_fabrica_ia_hml`. Hoje funciona (2.711/2.711 com `sentence_transformers`), mas na limpeza
do Volume antigo TI-RADS e hepatologia caem para `token_overlap` **sem erro nem log**. Débito
aberto em `nlp-engine-lib/docs/REFERENCIA-PARAMETROS.md`.

---

## 5. Documentos prontos para postar

| documento | para |
|---|---|
| `cancer_estomago/justificativa-lote3-revisao-regua.md` | PO + negócio |
| `_fundacao/pedido-mlops-schemas-plataforma-nova.md` | MLOps |
| `_fundacao/proposta-padrao-versionamento-config.md` | time da Fábrica de IA |
| `_fundacao/notas-plataforma-nlp-mlops.md` | MLOps (já postado) |
| `tireoide/entrega-mlops-tirads-v1.md` | MLOps — **atualizar com a validação 0.8.0** |

⚠️ O pedido de schemas pode ter perdido parte da validade: o João indicou que a infra provisiona.
Confirmar antes de enviar. O `CREATE SCHEMA IF NOT EXISTS` existe no código, mas só no estágio de
**export**, que roda **depois** das escritas de entrada e saída.

---

## 6. Débito que continua aberto

**Documentação.** O repo `Projects/` tem dois remotos (`fabrica-ia-lib` legado e o GitHub
`rdor_share` da A3) e **9 CSVs com texto de laudo** nos commits locais. Nenhum foi pushado.
Decisão registrada: adiar a reorganização. **Não pushar os docs até resolver.**
Ver `debito-organizacao-docs-e-laudo-no-git.md`.
