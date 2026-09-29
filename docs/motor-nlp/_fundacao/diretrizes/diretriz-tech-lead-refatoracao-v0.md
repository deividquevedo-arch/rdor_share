# Diretriz -- Tech Lead: Refatoracao e Evolucao de Legado

**Regra:** nenhuma refatoracao sem Research, nenhuma implementacao sem SPEC, nenhuma decisao implicita.

---

## 1. RPI obrigatorio (Research -> Plan -> Implement)

Toda task de refatoracao segue esta sequencia. Sem atalhos.

### Research

Antes de mexer em qualquer modulo:

- Definir se o modulo e **especifico de uma especialidade** ou **compartilhado** entre varias (ex.: TextPipeline, trechos comuns a varios `ntb_ia_*`).
- **Especifico:** ler o notebook legado **correspondente** (ex: `ntb_ia_hepatologia_algoritmo.py`).
- **Compartilhado:** fazer **inventario** em `algoritmos/` (e paths equivalentes no repo) dos notebooks que implementam a **mesma responsabilidade**; comparar **semelhancas e diferencas** (assinatura, ordem dos passos, regex, dependencias).
- Se houver **divergencia** entre legados: documentar no SPEC o **comportamento adotado**, a **referencia canonica** acordada com o time e o que fica em **config/YAML** vs codigo fixo (sem escolha implicita).
- Entender o que faz (fluxo de dados, transformacoes, regras aplicadas).
- Mapear dependencias (quais libs usa, quais tabelas le/escreve, quais configs hardcoded).
- Identificar o que muda (extrai para lib) e o que permanece (serving layer, DDL).
- Documentar achados em **3-5 bullet points** no PR ou no SPEC; em modulo **compartilhado**, o SPEC deve estar **consolidado** antes da implementacao.

### Plan

Definir SPEC do modulo antes de codar (ver secao 2).

### Implement

So entao codar. Teste acompanha o codigo (nao depois).

**Anti-pattern:** abrir o editor e comecar a escrever funcoes. Se nao passou pelo R e pelo P, pare.

---

## 2. Spec Driven Development (SDD)

Antes de implementar qualquer modulo, preencher este template:

```
SPEC: {nome_do_modulo}
----------------------------------------------
Responsabilidade: (1 frase -- o que faz)
O que NAO faz:    (fronteiras explicitas)

Input:
  - tipo: dict | DataFrame | str
  - campos obrigatorios: [...]
  - campos opcionais: [...]

Output:
  - tipo: dict | DataFrame | dataclass
  - campos: [...]
  - invariantes: (ex: confidence_score entre 0.0 e 1.0)

Edge cases:
  - input vazio
  - input com encoding quebrado
  - laudo sem texto (campo nulo)
  - negacao no inicio vs meio vs fim da sentenca

Dependencias:
  - spaCy pt_core_news_lg
  - config dict (recebido, nao lido do disco)

Testes:
  - test_{modulo}.py
  - cenarios: [input valido, invalido, com negacao, sem negacao]
```

### Exemplo real: `text_pipeline.py`

```
SPEC: text_pipeline
----------------------------------------------
Responsabilidade: receber texto bruto de laudo (HTML/RTF/plain)
                  e retornar texto limpo com negacoes detectadas

O que NAO faz: matching de keywords, scoring, leitura de Delta

Input:
  - tipo: str (texto bruto do laudo)
  - pode ser: HTML, RTF, plain text, encoding variado

Output:
  - tipo: dataclass ProcessedText
  - campos: clean_text, sentences, negations, metadata
  - invariantes: clean_text nunca e vazio se input nao e vazio

Edge cases:
  - laudo RTF com tags mal formadas
  - laudo HTML com boilerplate OCR
  - texto so com rodape/assinatura (deve retornar vazio)
  - acentos misturados (NFKC vs NFD)

Dependencias:
  - striprtf, html2text, ftfy (limpeza)
  - spaCy pt_core_news_lg (sentencizer)

Testes:
  - test_text_pipeline.py
  - cenarios: plain, RTF, HTML, encoding quebrado, vazio,
              com negacao, sem negacao, boilerplate only
```

O SPEC e escrito ANTES do codigo. Vive como docstring no topo do modulo ou como comentario no PR.

---

## 3. Progressive Disclosure na refatoracao

Refatorar em camadas. Cada camada e um PR revisavel. Nao pular.

| Camada | O que faz | Criterio de pronto |
|--------|----------|-------------------|
| **1 -- Extrair** | Mover funcao pura do notebook para a lib. Sem mudar comportamento. | Funcao roda identica ao notebook original |
| **2 -- Tipar** | Adicionar type hints, docstring, testes unitarios | Testes passando, tipos corretos |
| **3 -- Refinar** | Incorporar melhorias (ex: negacao avancada, edge cases) | Testes novos passando, comportamento documentado |
| **4 -- Otimizar** | Performance (batch, vectorized ops) se necessario | Benchmark mostra ganho sem regressao |

**Regra:** a Camada 1 deve produzir output identico ao notebook original. Se divergir, e bug -- corrigir antes de avancar.

**Exemplo concreto:**

```
Camada 1: extrair to_plain() do biliar para text_pipeline.py
          -> teste: mesmo output para os mesmos 5 laudos sinteticos
Camada 2: type hints (str -> ProcessedText), docstring, test_text_pipeline.py
Camada 3: adicionar deteccao de boilerplate OCR (hoje so biliar tem)
Camada 4: batch processing via nlp.pipe (se volume justificar)
```

---

## 4. Anti-vibe-coding

| Regra | O que significa |
|-------|----------------|
| **Decisao explicita** | Toda escolha de design documentada no PR ou SPEC. "Por que dict e nao dataclass?" -> resposta escrita. |
| **Sem copia cega** | Nao copiar funcao de outro notebook sem entender. Se copiou, o Research deve explicar o que faz. |
| **Sem "depois melhoro"** | O PR deve estar correto. Se sabe que tem divida tecnica, abrir issue explicita. |
| **Sem premissa implicita** | Se assume que input sempre vem limpo, documentar. Se assume spaCy carregado, documentar. |
| **Pesquisar antes** | Se nao sabe como funciona (ex: `_snippet_mentions_other_organs`), ler o codigo antes de refatorar. R do RPI. |

---

## 5. Code Review estrutural (checklist)

Antes de aprovar qualquer PR de refatoracao:

- [ ] **Acoplamento:** o modulo depende de algo que nao deveria? (lib importando outra lib? dependencia de Spark em modulo puro?)
- [ ] **Coesao:** o modulo faz uma coisa so? (se a docstring precisa de "e" ou "tambem", dividir)
- [ ] **Simplicidade:** pode remover algo sem perder funcionalidade? (imports mortos, variaveis nao usadas, branches inalcancaveis)
- [ ] **Contrato:** input/output esta tipado e validado? (type hints + validacao no config_loader)
- [ ] **Nomeacao:** o nome diz o que faz sem ler o corpo? (`detect_negation` sim, `proc2` nao)
- [ ] **Paridade:** se extraiu de notebook, o comportamento e identico? (teste de paridade com dados sinteticos)
- [ ] **SPEC:** existe SPEC do modulo? Codigo corresponde ao SPEC?
- [ ] **YAGNI:** implementou algo que nao e necessario agora? (se nao esta na task, nao entra)

---

*Complementar a: `diretriz-arquitetura-pre-codigo-v0.md`, `diretriz-desenvolvimento-libs-v0.md`, `diretriz-config-e-governanca-v0.md`*
