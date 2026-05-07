# Checklist de implementacao -- Motor NLP Fase 1 (MVP rule-based)

**Uso:** por modulo ou por PR, marcar `[x]` conforme avanca. Copiar linhas para descricao de PR se necessario.

**Metodologia:** RPI + SPEC + progressive disclosure + review estrutural. Ver `diretriz-tech-lead-refatoracao-v0.md` (mesmo diretorio `docs/motor-nlp/`).

**Decisoes atuais (time):** Python **3.12**; codigo da lib em **`.py`**; orquestracao em **`.ipynb`**; dependencias **`requirements.txt`** centralizado; publicacao de wheels (**T00.5/T00.6**) pode seguir alinhamento **MLOps** (desenvolvimento local com `pip install -e .` ate la).

---

## 1. Fluxo por unidade de trabalho (modulo ou funcao publica)

| # | Etapa | Check | Criterio de pronto |
|---|--------|-------|-------------------|
| 1 | **Research** | [ ] | Notebook legado identificado; dependencias mapeadas; 3-5 bullets no PR ou SPEC |
| 2 | **SPEC** | [ ] | Template preenchido (secao 3); fronteiras e edge cases explicitos |
| 3 | **Camada 1 -- Extrair** | [ ] | Paridade com legado nos mesmos inputs sinteticos |
| 4 | **Camada 2 -- Tipar** | [ ] | Type hints + docstrings em API publica; CI/lint OK |
| 5 | **Camada 3 -- Refinar** | [ ] | Melhorias so com SPEC atualizado + novos testes |
| 6 | **Camada 4 -- Otimizar** | [ ] | So se houver criterio (volume/SLO); evidencia no PR |
| 7 | **Testes** | [ ] | pytest; sem PHI; cenarios minimo: valido, invalido, negacao, YAML se aplicavel |
| 8 | **Validacao** | [ ] | AC da historia (anexo03) + checklist review estrutural (diretriz Tech Lead) |

Ao fechar a etapa **Validacao** (linha 8), preencher criterios de paridade, timing de smoke no Databricks e referencia legado na matriz sugestiva: [`doc-validacao-paridade-databricks-v0.md`](../doc-validacao-paridade-databricks-v0.md).

**Pular camadas:** nao. **Funcao publica pequena:** use linhas F.1-F.4 na secao 4.

---

## 2. Template SPEC (copiar por modulo)

```
SPEC: {nome_modulo}
----------------------------------------------
Responsabilidade: (1 frase)
O que NAO faz:    (fronteiras)

Input:  tipo, campos obrigatorios/opcionais
Output: tipo, campos, invariantes

Edge cases: (lista)
Dependencias: (libs, config dict)
Testes: arquivo + cenarios
```

---

## 3. Matriz historia / modulo -- Fase 1 (S00-S06)

Marque por coluna quando cada fase estiver concluida para o modulo.

| Story | Modulo / artefacto | Research | SPEC | Impl | Testes | Validacao (AC) |
|-------|-------------------|----------|------|------|--------|----------------|
| S00 | Repos `nlp_engine`, `data_manage`, `monitoring`, `plataforma-nlp` + CI lint | [ ] | N/A | [ ] | [ ] CI | [ ] 4 repos; CI; T00.5/T00.6 conforme MLOps |
| S01 | `text_pipeline` (`to_plain`, segmentacao, negacao, boilerplate; T01.6 encadeamento) | [ ] | [ ] | [ ] | [ ] T01.5 | [ ] AC S01 |
| S02 | `ClinicalNlpEngine` + `RuleBasedEngine` (`engine.py`) | [ ] | [ ] | [ ] | [ ] T02.5 | [ ] AC S02 |
| S02 | `scoring.py` (`nlp_engine`) | [ ] | [ ] | [ ] | [ ] | [ ] AC S02 |
| S02 | T02.4a invariantes (`nlp_engine` / `output_invariants`) + T02.4b schema (`monitoring`) | [ ] | [ ] | [ ] | [ ] | [ ] AC S02 |
| S03 | `config_loader.py` + merge organs + YAMLs | [ ] | [ ] | [ ] | [ ] T03.4 | [ ] AC S03 |
| S04 | `metrics.py` + metadados + tabela metricas | [ ] | [ ] | [ ] | [ ] | [ ] AC S04 |
| S05 | `data_manage` loader/saver + notebook `.ipynb` orquestrador | [ ] | [ ] | [ ] | [ ] E2E dev | [ ] AC S05 |
| S06 | Relatorio validacao hepato vs baseline | [ ] | N/A | [ ] | [ ] | [ ] AC S06 |

**Paralelos (fora da trilha critica S01-S06):** S07 inventario dados; S08 governanca clinica -- ver `anexo03-historias-e-tasks-v0.md`.

**Referencias:** `anexo03-historias-e-tasks-v0.md` (tasks Txx.x e AC); `anexo02-arquitetura-motor-nlp-v0.md` (contrato I/O).

---

## 4. Checklist por funcao publica (opcional, modulos grandes)

| # | Etapa | Check |
|---|--------|-------|
| F.1 | SPEC da funcao (pre/pos-condicoes, erros) | [ ] |
| F.2 | Implementacao (uma responsabilidade; < 30 linhas ou justificativa) | [ ] |
| F.3 | Teste dedicado + edge cases do SPEC | [ ] |
| F.4 | Paridade legado (se aplicavel) | [ ] |

---

## 5. Review estrutural (antes de merge)

- [ ] Acoplamento OK (nenhuma lib importa outra)
- [ ] Coesao OK (modulo com responsabilidade unica)
- [ ] Contrato I/O tipado e validado onde aplicavel
- [ ] Nomeacao clara
- [ ] Paridade com notebook se extracao legado
- [ ] YAGNI respeitado

---

*Indice geral da documentacao: `../README.md`*
