# Diretriz -- Desenvolvimento das 3 Libs

**Regra:** todo codigo do motor NLP segue estas regras. Sem excecao.

---

## 1. Fronteiras entre libs

| Regra | Exemplo correto | Exemplo proibido |
|-------|----------------|-----------------|
| Nenhuma lib importa outra | `from nlp_engine.engine import process` | `from nlp_engine import data_manage` |
| Comunicacao por dict/DataFrame | `engine.process(df, config_dict)` | `engine.process(df, "config.yaml")` |
| Cada lib tem seu config_loader | `nlp_engine.config_loader.load(dict)` | `nlp_engine.config_loader.load("file.yaml")` |
| Notebook e o unico orquestrador | Notebook le YAML, extrai secoes, injeta | Lib le YAML do disco |

---

## 2. Estrutura de cada lib

```
{lib_name}/
  {lib_name}/
    __init__.py
    config_loader.py       # recebe dict, valida, retorna config tipada
    ...modulos de negocio...
  tests/
    conftest.py            # fixtures compartilhados
    test_{modulo}.py       # 1 arquivo de teste por modulo
  pyproject.toml           # ou setup.cfg -- semver, deps pinadas
  README.md
```

### Modulos por lib

**nlp_engine:**
- `text_pipeline.py` -- limpeza, normalizacao, segmentacao, negacao
- `engine.py` -- interface `ClinicalNlpEngine` + `RuleBasedEngine`
- `config_loader.py` -- merge shared organs + specialty config
- `scoring.py` -- score continuo 0.0-1.0
- `semantic_expand.py` -- embeddings MiniLM (Fase 2, opcional via flag)

**data_manage:**
- `loader.py` -- leitura de Delta/Spark DataFrame
- `saver.py` -- escrita em Delta
- `contracts.py` -- validacao de schema entrada/saida

**monitoring:**
- `metrics.py` -- precision, recall, F1, taxa de relevancia
- `quality_guard.py` -- validacao estrutural do output

---

## 3. Regras de codigo

### Fazer

- Funcoes pequenas (< 30 linhas). Se passou, dividir.
- Nomes descritivos: `detect_negation()`, nao `proc2()`.
- Type hints em toda funcao publica.
- Docstring em funcoes publicas (1-2 linhas, o que faz + o que retorna).
- Retornar tipos explicitos (dict, DataFrame, dataclass). Sem retornos ambiguos.
- Feature flags via config dict para comportamento condicional entre especialidades.

### Nao fazer

- Comentarios obvios (`# incrementa o contador`).
- Duplicar logica entre especialidades (se precisa variar, varia por YAML).
- Hardcodar listas clinicas, thresholds, orgaos no codigo (vai para YAML).
- Usar `eval()`, `exec()`, ou `str()` para parse de estruturas.
- Importar lib que nao usa (imports mortos).
- `nltk.download('all')` em producao (apenas recursos necessarios).
- PHI (dados de paciente) em testes, logs ou repositorio.

---

## 4. Testes

| Regra | Detalhe |
|-------|---------|
| Framework | pytest |
| Dados | Frases sinteticas (sem PHI). Ex: "Figado com sinais de cirrose e hipertensao portal" |
| Cobertura | 1 arquivo de teste por modulo de negocio |
| Cenarios minimos | input valido, input invalido, com negacao, sem negacao, YAML valido, YAML invalido |
| Fixtures | `conftest.py` com configs de exemplo e laudos sinteticos |
| Execucao | Rodar antes de todo PR. CI bloqueia merge se falhar. |

Exemplo de teste:

```python
def test_negation_detects_absence(text_pipeline):
    result = text_pipeline.process("Ausencia de lesoes hepaticas focais")
    assert result.negations[0].expression == "ausencia de"
    assert result.negations[0].scope == "lesoes hepaticas focais"
```

---

## 5. Versionamento

| Regra | Detalhe |
|-------|---------|
| Esquema | Semver: MAJOR.MINOR.PATCH |
| MAJOR | Quebra de backward compatibility (todas as especialidades migram juntas) |
| MINOR | Nova funcionalidade, backward compatible (feature flag via YAML) |
| PATCH | Bug fix, sem mudanca de comportamento |
| Registro | `engine_version` obrigatorio em todo output do motor |
| Backward compat. | Comportamento diferente entre especialidades = YAML (config), nao versao da lib |

---

## 6. Design patterns aplicados

| Pattern | Onde | Por que |
|---------|------|---------|
| **Composition Root** | Notebook orquestrador | Unico ponto que conhece e conecta as 3 libs |
| **Hexagonal (Ports & Adapters)** | Libs sao nucleo agnostico | Databricks, Delta, Excel sao adaptadores externos |
| **Template Method** | Notebook | Fluxo fixo (load -> process -> emit -> save); conteudo varia por YAML |
| **Strategy** | `engine.py` | `RuleBasedEngine` hoje, `EncoderEngine` futuro -- mesma interface |

---

*Detalhamento: `anexo02-arquitetura-motor-nlp-v0.md`, `05-roadmap-entregas-sprint-v0.md` (A1, A5), `doc-gestao-dependencias-libs-v0.md`*
