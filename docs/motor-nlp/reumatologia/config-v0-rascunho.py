# Databricks notebook source
# MAGIC %md
# MAGIC # Config da especialidade `reumatologia` — RASCUNHO v0
# MAGIC
# MAGIC ⚠️ **RASCUNHO.** Nao e a config do repositorio da plataforma. Traducao literal da regua
# MAGIC legada, gerada programaticamente a partir do `CONFIG` de
# MAGIC `apps/databricks/reumatologia/model/ntb_ia_reumatologia_algoritmo.ipynb` em 2026-09-04.
# MAGIC
# MAGIC **O que a linha decide:** laudo de imagem musculoesqueletica entra na captacao quando
# MAGIC descreve achado inflamatorio de espondiloartrite ou artrite. A decisao do legado e
# MAGIC puramente lexica — `fl_relevante = 1 if achados != [] else 0`. Sem juiz, sem limiar,
# MAGIC sem score.
# MAGIC
# MAGIC **Criterio de aceitacao: paridade comportamental.** Nao existe gabarito clinico para
# MAGIC esta linha. A migracao nao pode afirmar acerto, so equivalencia. SPEC em
# MAGIC `docs/motor-nlp/reumatologia/spec-migracao-reumatologia-v0.md`.
# MAGIC
# MAGIC ## Baseline do legado, medido em 2026-09-04
# MAGIC
# MAGIC | medida | valor |
# MAGIC |---|---|
# MAGIC | entrada acumulada | 914.286 laudos (29/12/2025 a 02/09/2026) |
# MAGIC | volumetria diaria | 3.200 a 4.500 |
# MAGIC | saida | 792.660 linhas, 2.971 relevantes, taxa 0,37% |
# MAGIC
# MAGIC Distribuicao dos relevantes: `artrite reumatoide` 1.757, `sacroileite` 898,
# MAGIC `artrite psoriasica` 98, `espondilite` 95, mais 8 combinacoes somando 108.
# MAGIC
# MAGIC ## O que foi adaptado
# MAGIC
# MAGIC - **19 orgaos estrangeiros removidos.** O `CONFIG` legado declara figado, vias biliares,
# MAGIC   pancreas, baco, rins, ureteres, bexiga, adrenais, utero, ovarios, prostata, vasos,
# MAGIC   peritonio, retroperitonio, pulmao, musculoesqueletico, conclusao e colon_reto — mais os
# MAGIC   4 achados do colon. `TARGET_ORGAN = "reumatologia"` os neutraliza em runtime.
# MAGIC   ⚠️ **Isto e hipotese.** Se a paridade falhar, e a primeira suspeita.
# MAGIC - **4 seeds duplicadas removidas** (97 declaradas, 93 distintas).
# MAGIC - **Direcao da negacao:** o legado declara `window_tokens: 7` e NENHUMA direcao. Aqui vai
# MAGIC   `left`, o default fixado na 0.11.1. Divergencia, se houver, aparece na paridade.
# MAGIC - **Juiz desligado**, declarado explicitamente: o legado nao tem juiz, e ligar mudaria o
# MAGIC   comportamento antes de haver paridade. Com 3.700 laudos/dia o custo e decisao de produto.
# MAGIC - **`segmentation: full_doc`**, espelhando `FORCE_FULL_DOC_FOR = {"reumatologia"}`.
# MAGIC
# MAGIC ## Changelog
# MAGIC
# MAGIC ### `0.1.0-reumatologia` — traducao literal (rascunho, 2026-09-04)
# MAGIC Sem medicao de paridade ainda. Nao subir sem `match_rate`.

# COMMAND ----------

CONFIG = {
    'specialty_id': 'reumatologia',
    'config_version': '0.1.0-reumatologia',
    'model_version': 'v0',
    'nlp': {
        'target_organs': ['reumatologia'],
        'organs': {
            'reumatologia': {
                'seeds': [
            'coluna', 'coluna vertebral', 'coluna espinal', 'espinha dorsal', 'vertebra',
            'vértebra', 'vertebras', 'vértebras', 'cervical', 'torácica', 'toracica', 'lombar',
            'sacral', 'coluna cervical', 'coluna toracica', 'coluna torácica', 'coluna lombar',
            'coluna sacral', 'região cervical', 'região toracica', 'região torácica',
            'região lombar', 'região sacral', 'corpo vertebral', 'corpos vertebrais',
            'disco intervertebral', 'discos intervertebrais', 'faceta articular',
            'facetas articulares', 'processo espinhoso', 'processos espinhosos',
            'apófise espinhosa', 'apófises espinhosas', 'espondilo', 'espondilite',
            'espondilartrite', 'ligamento longitudinal', 'ligamento amarelo',
            'ligamento nucal', 'articulação', 'articulacoes', 'articulações', 'junta',
            'juntas', 'artrite', 'artrites', 'artralgias', 'artralgia',
            'articulação sacroilíaca', 'articulacao sacroiliaca', 'sacroilíaca', 'sacroiliaca',
            'sacroilíacas', 'sacroiliacas', 'articulação do quadril', 'articulacao do quadril',
            'quadril', 'quadris', 'articulação do joelho', 'articulacao do joelho', 'joelho',
            'joelhos', 'articulação do tornozelo', 'articulacao do tornozelo', 'tornozelo',
            'tornozelos', 'articulação do ombro', 'articulacao do ombro', 'ombro', 'ombros',
            'articulação do punho', 'articulacao do punho', 'punho', 'punhos',
            'articulações das mãos', 'articulacoes das maos', 'articulações dos pés',
            'articulacoes dos pes', 'articulações interfalangeanas',
            'articulacoes interfalangeanas', 'articulações metacarpofalangeanas',
            'articulacoes metacarpofalangeanas', 'sinovial', 'sinóvia', 'cartilagem articular',
            'cartilagem hialina', 'membrana sinovial', 'cápsula articular',
            'capsula articular', 'espaço articular', 'espaco articular',
            'espaço intra-articular', 'espaco intra-articular',
                ],
                'regex': [
                r'\\bcoluna( vertebral| espinal)?\\b',
                r'\\bvertebr(a|as)\\b',
                r'\\b(cervical|toracica|lombar|sacral)\\b',
                r'\\bespondil\\w*\\b',
                r'\\bdisco(s)? intervertebral(is)?\\b',
                r'\\bcorpo(s)? vertebral(is)?\\b',
                r'\\bfaceta(s)? articular(es)?\\b',
                r'\\bapofise(s)? espinhosa(s)?\\b',
                r'\\barticulacao(oes)?\\b',
                r'\\barticulacoes?\\b',
                r'\\bjunta(s)?\\b',
                r'\\bartrite(s)?\\b',
                r'\\bartralgia(s)?\\b',
                r'\\bsacroiliac(a|as)\\b',
                r'\\b(quadril|joelho|tornozelo|ombro|punho)\\b',
                r'\\b(sinovial|sinovia|cartilagem articular)\\b',
                r'\\bcapsula articular\\b',
                r'\\bmembrana sinovial\\b',
                r'\\bespaco(s)? (intra-)?articular(es)?\\b',
                ],
            },
        },
        'segmentation': {'mode': 'full_doc'},
        'findings': {
            'espondilite': [
                'espondilite anquilosante', 'sindesmofito marginal', 'anquilose vertebral',
                'romanus',
            ],
            'sacroileite': [
                'sacroileite', 'sacroilite', 'sacroiliite', 'sacroileite bilateral',
                'erosoes sacroiliacas', 'edema osseo sacroiliaco', 'anquilose sacroiliaca',
                'sinovite sacroiliaca',
            ],
            'artrite_reumatoide': [
                'artrite reumatoide', 'reumatoide', 'artrite reumatoide ativa',
                'pannus sinovial inflamatorio', 'sinovite proliferativa',
                'erosoes osseas marginais',
            ],
            'artrite_psoriasica': [
                'artrite psoriasica', 'artropatia psoriasica', 'psoriasica', 'dactilite',
                'entesite inflamatoria', 'periostite inflamatoria',
                'sindesmofito nao marginal', 'acroosteolise',
            ],
            'nodulo_reumatoide': [
                'nodulo reumatoide', 'nodulos reumatoides', 'lesao nodular reumatoide',
            ],
        },
        'negation': {
            'phrases': [
                'nao', 'sem', 'ausencia de', 'negado', 'nega', 'descarta', 'livre de',
                'sem evidencias de', 'sem sinais de', 'sem achados de', 'sem alteraçoes',
                'sem alteracoes', 'sem sinais', 'sem achados', 'sem alteracao',
                'sem alteraçao', 'sem polipos', 'sem diverticulos', 'nao sugerindo',
                'não sugerindo', 'questionase', 'questiona-se', 'questiona se',
            ],
            'window': 7,
            'direction_default': 'left',
        },
        'embeddings': {
            'use_embeddings': False,
        },
        'llm_router': {
            'enabled': False,
        },
    },
    'runtime': {
        'llm_router': {
            'enabled': False,
        },
    },
}
