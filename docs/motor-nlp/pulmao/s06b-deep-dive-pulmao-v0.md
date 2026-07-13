# S06b — Deep-dive motor vs legado Pulmão (Grupo 1b)

**Backlog:** S06b T06.6 / T06.7; gaps F2 vs **S09** em [`anexo03-historias-e-tasks-v0.md`](../anexo03-historias-e-tasks-v0.md).

## 1. Classificação no legado (fonte do produto)

Pulmão está no **Grupo 1b** (*Embeddings + regras*) na taxonomia consolidada — ver secção 6 de [`07-relatorio-final-v0.4-plataforma-nlp-clinica.md`](../07-relatorio-final-v0.4-plataforma-nlp-clinica.md). O relatório cita **MiniLM** como exemplo; o notebook de predição referenciado abaixo usa **`paraphrase-multilingual-mpnet-base-v2`** (SentenceTransformers).

## 2. RPI resumido — notebook legado (`algoritmos/pulmao/pulmao/model/ntb_ia_predicao.ipynb`)

| Elemento | Observação |
|----------|------------|
| Modelo de frase | `SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')` (células com import e classe interna). |
| Similaridade | `cosine_similarity` entre embeddings de frases (trechos do laudo vs referências). |
| Coluna de decisão final amostrada | Uma célula próxima do fim define `exm_encaminhamento_nlp` com `np.where(mask_exclusao, 'N', 'S')` onde `mask_exclusao` testa se `exm_justificativa_nlp` (mapeada de `categorizacao` no rename) casa com `^(EXCLUSAO\|CASO ACOMPANHADO)\b` — ou seja, **regra pós-processo** sobre texto de justificativa/categorização. |
| Pipeline completo | O notebook é extenso: a `exm_justificativa_nlp` / categorização **vem** de etapas anteriores (incl. lógica com embeddings e classificações). Para paridade reprodutível, o time deve documentar **ordem** completa: extração de frase → regras locais → similaridade → agregação → `exm_encaminhamento_nlp`. |

**Implicação para o motor Fase 1:** o `ClinicalNlpEngine` actual (rule-based + YAML) **não** incorpora o `SentenceTransformer` nem a mesma cadeia; comparação 1:1 com o export exige **S09** (embeddings opcionais) ou critério de escopo explícito (abaixo).

## 3. Decisão de paridade (S08) — opções A / B / C

Registar escolha no PR / acta; sem isto, métricas de match não têm interpretação única.

| ID | Descrição |
|----|-----------|
| **A** | Paridade com o **export** actual (incl. linhas cujo `S` reflete trechos extra-torácicos ou lógica semântica do legado). |
| **B** | Paridade apenas em **subcoorte** (ex.: laudos com estudo de tórax / escopo pulmonar) — ver agregado `escopo` em `stratify_pulmao_mismatches.py`. |
| **C** | Legado como **referência fraca**; definir **novo gold** (processo clínico separado). |

## 4. Ferramentas no repo (sem PHI agregada por defeito)

| Script | Uso |
|--------|-----|
| [`stratify_pulmao_mismatches.py`](../../../plataform/nlp_engine/scripts/stratify_pulmao_mismatches.py) | JSON: FN/FP por `legacy_class`, por bucket, por heurística de escopo (torax / misto / abdome). |
| [`export_pulmao_qualitative_sample.py`](../../../plataform/nlp_engine/scripts/export_pulmao_qualitative_sample.py) | CSV com `id_exame` + colunas vazias `root_cause` / `notes_reviewer` para revisão offline (7+8+2 por defeito). |

**Classificação T06.7 (preencher na revisão):** por linha, etiquetar `root_cause` ∈ {`requer_regra`, `requer_gate_orgao`, `requer_negacao`, `fora_escopo_pulmao`, `requer_embedding`, `outro`}.

## 5. Próxima fase (embeddings) — S09

Encapsular componente de embeddings, flag YAML e calibração: ver **S09 — Componente de embeddings (MiniLM)** (nota: alinhar modelo a **mpnet** do notebook, se for o alvo) em [`anexo03` § Fase 2](../anexo03-historias-e-tasks-v0.md). Não tratar gap semântico como *bug* silencioso do MVP rule-based.

## 6. Ligar à evidência numérica

Ver matriz e buckets em [`s06b-amostra-padrao-pulmao-v0.md`](s06b-amostra-padrao-pulmao-v0.md) (secção "Rerodada local").
