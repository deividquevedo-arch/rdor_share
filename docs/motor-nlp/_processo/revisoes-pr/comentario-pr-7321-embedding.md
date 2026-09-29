@<João Marcelo da Silva Ferreira> o desenho está muito bom — quero registrar isso antes de qualquer coisa.

Dois pontos que você acertou e que não são óbvios:

O motor roda no **driver** (`queue_df.toLocalIterator`), então o `/tmp` e o `HF_HUB_OFFLINE` ficam no processo que de fato carrega o modelo. Numa execução distribuída por UDF nada disso valeria, e é o tipo de coisa que só aparece quando quebra em produção.

E **falhar alto em vez de degradar** é a decisão certa. Hoje um `embedding_model` inválido cai em `token_overlap` sem erro e sem log, e a config diz que roda híbrido enquanto executa outra coisa. Você fechou essa porta.

Sobre o teste em dev: **funcionou**. No run de 16/09 no `cancer_rim`, `[sentence_transformers]` em **10.000 de 10.000**, zero fallback. É a primeira vez no projeto que a camada semântica roda com modelo real numa linha inteira.

Sobre o *"não estava retornando"*: a camada rodou, o que não houve foi promoção — o `cancer_rim` exige `0.92`, o máximo foi **0,9654** e só **2 laudos em 10.000** passam. Não promover é o que aquele número foi calibrado para fazer.

Antes de aprovar te peço para fechar **duas evidências**. As duas são para proteger produção, nenhuma é reparo no seu código.

---

## 1. O principal dos jobs consegue ler `mlops_fabrica_ia`?

Rodando `SHOW CATALOGS` no workspace da plataforma aparecem 35 catálogos e esse não está entre eles. Pode ser só diferença de permissão entre o meu usuário e o do job — mas precisa ser confirmado, porque o loader levanta `RuntimeError` quando não resolve, e hoje quatro linhas declaram `use_embeddings: True`.

Atualmente as que rodam em PRD degradam calados e o job fecha verde. Com a mudança, a mesma condição **aborta o run**. A troca é correta — só precisamos que o grant esteja lá antes, senão viram quatro jobs parados de uma vez.

**Fecha com:** um `download_artifacts` rodado com a identidade do job em prd, ou o grant registrado.

---

## 2. O modelo no catálogo novo é o mesmo do antigo?

Essa apareceu quando vi o código da cópia, e acho que vale a pena confirmar:

```python
model = mlflow.sentence_transformers.load_model(local_path)
mlflow.sentence_transformers.log_model(model=model, ...)
```

Isso não copia o artefato — **desserializa e serializa de novo**, com a `sentence-transformers` que o `%pip install` (sem pin) trouxe naquele dia. Os pesos provavelmente sobrevivem, mas o layout é reescrito pela versão nova: pode trocar `pytorch_model.bin` por `model.safetensors`, reordenar o `modules.json`, e o `MLmodel`/`requirements.txt` passam a pinar a versão usada na cópia em vez da que o motor usa.

Como os limiares foram calibrados contra o modelo antigo — `0.92` no ca-rim, `0.78` na hepatologia —, se o modelo mudou os números mudam de significado.

**Fecha com** um teste direto, que responde melhor que comparar bytes:

```python
from sentence_transformers import SentenceTransformer
import mlflow, numpy as np

frases = [
    "Nodulo solido em rim direito medindo 3,2 cm.",
    "Lesao cistica com septos espessados.",
    "Exame dentro dos limites da normalidade.",
]

antigo = SentenceTransformer(
    mlflow.artifacts.download_artifacts(
        "models:/diamond_ia_hml.nlp_engine.st_paraphrase_multilingual_minilm/1"
    )
)
novo = SentenceTransformer(
    mlflow.artifacts.download_artifacts(
        "models:/mlops_fabrica_ia.default.st_paraphrase_multilingual_minilm/1"
    )
)

a = antigo.encode(frases, normalize_embeddings=True)
b = novo.encode(frases, normalize_embeddings=True)

print("diferenca maxima:", np.abs(a - b).max())
```

**Aceite:** zero, ou na ordem de `1e-7`.

Se der diferença, o caminho é copiar o artefato sem passar por `load_model`/`log_model` — aí a pergunta *"é a mesma versão?"* fica respondida por construção.

---

## Detalhe menor, sem pressa

- `HF_HUB_OFFLINE` e `TRANSFORMERS_OFFLINE` são efeito global de processo e persistem para o notebook inteiro, inclusive para etapas sem relação com embeddings.

E vale a descrição do PR contar o que muda — principalmente que o modo de falha das quatro linhas passa de silencioso para explícito. Quem for ler isso daqui a três meses vai precisar dessa frase.
