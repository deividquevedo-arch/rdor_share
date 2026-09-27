# Nível 2 — Embedding via Databricks Model Serving (substitui download HF por endpoint cloud)

**Problema:** o `sentence_transformers` baixa `paraphrase-multilingual-MiniLM-L12-v2` do HF Hub a cada
restart do cluster (cache efêmero) → trava ~36 min. **Solução:** servir o MESMO modelo multilingual
(PT) como endpoint no Databricks e chamar via HTTP (como já fazemos com o LLM). **Aditivo:** o backend
`sentence_transformers` original permanece; o HTTP é uma opção nova, ligada só quando validada.

> NÃO usar embeddings Foundation Model em inglês (`databricks-gte-large-en`/`bge-en`) — corpus é PT.

---

## Passo 1 — Registrar o modelo no Unity Catalog (célula p/ rodar 1x, durante o E2E)

Baixa o modelo do HF **uma única vez** (para logar) e registra no UC. Depois disso, o endpoint serve
sem HF.

```python
# %pip install -U mlflow "sentence-transformers>=3.0"  (se necessário; DBR ML já traz mlflow)
import mlflow
from sentence_transformers import SentenceTransformer

mlflow.set_registry_uri("databricks-uc")

MODEL_HF = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
UC_MODEL = "diamond_ia_hml.nlp_engine.st_paraphrase_multilingual_minilm"  # catalog.schema.nome

model = SentenceTransformer(MODEL_HF)                      # download 1x (one-time)
example = ["Tireoide com nodulo solido no lobo direito."]
signature = mlflow.models.infer_signature(example, model.encode(example))

# nested=True: o notebook do E2E deixa run(s) MLflow ativa(s) -> aninhar evita
# "Run ... is already active" sem precisar encerrar a run do E2E.
with mlflow.start_run(run_name="st-multilingual-minilm", nested=True):
    info = mlflow.sentence_transformers.log_model(
        model=model,
        artifact_path="model",
        signature=signature,
        input_example=example,
        registered_model_name=UC_MODEL,
    )
print("registrado:", info.model_uri)
```

## Passo 2 — Criar o endpoint de serving (scale-to-zero p/ custo)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()
ENDPOINT = "nlp-embed-multilingual-minilm"
UC_MODEL = "diamond_ia_hml.nlp_engine.st_paraphrase_multilingual_minilm"
VERSION = "1"   # ver a versao registrada no passo 1 (UC Models)

w.serving_endpoints.create(
    name=ENDPOINT,
    config=EndpointCoreConfigInput(
        served_entities=[ServedEntityInput(
            entity_name=UC_MODEL,
            entity_version=VERSION,
            workload_size="Small",
            scale_to_zero_enabled=True,     # dorme quando ocioso; acorda no 1o request
        )],
    ),
)
# aguardar READY em Serving > nlp-embed-multilingual-minilm (1a subida ~alguns min)
```

## Passo 3 — Esperar READY e testar (formato da resposta importa p/ o backend)

A criação do endpoint é ASSÍNCRONA (~min). Rodar o predict antes → 404
`RESOURCE_DOES_NOT_EXIST`. Esperar ficar pronto primeiro:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
NAME = "nlp-embed-multilingual-minilm"
w.serving_endpoints.wait_get_serving_endpoint_not_updating(NAME)   # bloqueia até provisionar
print("state:", w.serving_endpoints.get(NAME).state)

import mlflow.deployments
client = mlflow.deployments.get_deploy_client("databricks")
resp = client.predict(endpoint=NAME,
                      inputs={"inputs": ["Tireoide com nodulo.", "Linfonodomegalias: nao ha."]})
print(type(resp), resp)   # esperado: {"predictions": [[...384 floats...], [...]]}
```

Se o `inputs=[str]` falhar (warning do input_example na hora de logar), tentar
`{"dataframe_split": {"columns": ["text"], "data": [["..."], ["..."]]}}` ou
`{"dataframe_records": [{"text": "..."}]}` — anotar qual funciona (o backend usa esse formato).

Anotar: URL = `https://<host>/serving-endpoints/nlp-embed-multilingual-minilm/invocations`, dim do vetor,
e o shape exato de `predictions` (lista de vetores) — o backend HTTP consome isso.

---

## Desenho do backend HTTP na lib (0.3.16, ADITIVO — não remove o sentence_transformers)

`semantic_expand.py`:
- **Nova função** `_evidence_with_serving_endpoint(text, *, model_name, terms, endpoint_url, api_key_env)`
  — espelha `_evidence_with_sentence_transformers`, mas obtém os vetores (chunks + termos) via POST ao
  endpoint (reusa o padrão do `llm_router`: header `Authorization: Bearer <token de api_key_env>`),
  e calcula cosseno igual. Cache dos vetores de termos por `(endpoint, terms_key)`.
- **`embedding_backend`** ganha o valor `"databricks_serving"` (ou `"serving_http"`). Seleção:
  - `"sentence_transformers"` → método ATUAL (inalterado).
  - `"databricks_serving"` → HTTP endpoint.
  - `"auto"` (default) → **continua preferindo `sentence_transformers`** (byte-compat); só usa HTTP se
    `embeddings.serving_endpoint` estiver configurado. Assim o método original NUNCA é removido; o HTTP
    entra por opt-in e só depois de validado.
- **Config novo (opt-in):**
  ```python
  'embeddings': {..., 'embedding_backend': 'databricks_serving',
                 'serving_endpoint': 'nlp-embed-multilingual-minilm',
                 'serving_base_url': 'https://{host}/serving-endpoints',   # runner injeta host
                 'api_key_env': 'DATABRICKS_TOKEN'}
  ```
- **Fallback seguro:** erro HTTP/timeout → cai em `token_overlap` (como o backend atual já faz quando
  ST ausente), nunca quebra o run.
- **Validação:** rodar E2E com `embedding_backend='databricks_serving'` e comparar contra a base ouro —
  score semântico e fl devem bater com o `sentence_transformers` (mesmo modelo). Só então tornar padrão.

**Ganhos:** sem torch/HF no cluster de batch, sem download, run mais rápido, governança OK (endpoint
interno, sem fetch externo não autenticado).
