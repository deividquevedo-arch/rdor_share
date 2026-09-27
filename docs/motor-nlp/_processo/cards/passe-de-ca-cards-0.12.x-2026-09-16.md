# Passe de CA nos cards em *Pronto para QA* — 16/09/2026

> Conferência dos critérios de aceite **um a um**, contra a árvore de trabalho
> (`nlp-engine-lib`, branch `feat/0.13.0-estrutura`). Nada postado no board.

---

# 0. Duas correções à triagem de 15/09

A triagem classificou estes cards por **amostragem do primeiro critério**. Conferidos os oito de
cada um, dois registros estavam errados:

| card | o que a triagem dizia | o que é |
|---|---|---|
| `253594` [P3-28] | *"CA1 exige `rev:` fixado; nenhum encontrado"* | **CA1 atendido** — `rev: v0.15.17`, e há teste comparando com o `ruff` do projeto. Os gaps reais eram CA3, CA7 e CA8 |
| `253587` [P2-21] | *"44 de 58 parametrize sem `ids=`"* | **57 de 60 têm ids descritivos** — a contagem só procurava a chave `ids=` e ignorava `pytest.param(..., id=...)`, que é a forma usada na maioria |

ℹ️ Os 3 restantes do `253587` parametrizam **strings**, e o id que o pytest gera é a própria
string (`[ordinal_category]`) — descritivo, não índice numérico. Conferido por `--collect-only`.
**O CA2 estava atendido.**

🔴 **E uma terceira, achada depois de commitar:** o CA7 do `253589` **já estava atendido**. O
`site/` era publicado como artefato `docs` desde antes, por `PublishPipelineArtifact`. O passo que
entrou como "correção" publicava a mesma pasta com outro nome — duplicata pura, upload dobrado.
Removido em `db695da`.

⚠️ **As três têm a mesma causa, e ela vale mais que os três achados:** procurei um **mecanismo
específico** (`rev:`, `ids=`, `PublishBuildArtifacts`) e concluí ausência, em vez de checar se o
**resultado** que o critério pede já existia. Critério de aceite descreve efeito; auditá-lo por
string é auditar a implementação que eu esperava encontrar.

---

# 1. `253574` [P2-08] — eliminar `type: ignore`

**CA4 pedia zero em `src/`. Havia cinco.** Agora são **zero**, e nenhum virou `noqa`.

| arquivo | era | virou |
|---|---|---|
| `ordinal_category.py:118` | `# type: ignore[union-attr]` no `.match().group(0)` | walrus estreitando o `Match \| None` |
| `ordinal_extraction.py:326` | `# type: ignore[no-any-return]` | variável local tipada `list[OrdinalMention]` |
| `output_invariants.py:267` | `# type: ignore[arg-type]` no `float(conf_raw)` | `isinstance` antes de converter |
| `to_plain.py:107` e `:118` | dois `# type: ignore[no-untyped-call]` | **um** alias tipado `Callable[[str], str]`, declarado uma vez |

⚠️ **Nenhuma das quatro muda comportamento**, e a de `output_invariants` foi escrita para preservar
a tabela de erros exatamente: `None`, lista e dict continuam caindo em `confidence_not_numeric`, e
`bool` continua sendo aceito como numérico — era o que o `except TypeError` fazia.

ℹ️ **O gate pegou um efeito colateral e estava certo:** nomear o alias sem `_` tornou
`striprtf_to_text` superfície pública do módulo, e o `test_all_bate_com_o_que_o_modulo_define`
reprovou. Renomeado para `_striprtf_to_text`.

✅ `mypy strict`: **38 arquivos, zero problemas.**

# 2. `253592` [P3-26] — documentação de humanos × de agentes

| CA | estado |
|---|---|
| CA1 decisão A/B registrada com justificativa | ✅ **escrita agora** no cabeçalho do índice: índice, não movimentação, porque os caminhos de `docs/` são citados de fora do repositório. Com gatilho de reavaliação: acima de ~25 arquivos, reconsiderar |
| CA4 todo arquivo classificado | ✅ **era o gap** — `plano-acao-backlog-lib-2026-09.md` estava sem seção e as seis SPECs só apareciam por padrão de nome. Agora há tabela nominal |
| CA5 nenhum link interno quebrado | ✅ **eram três** — `docs/agents/README.md` apontava para `agents/<ficha>.md` de dentro de `docs/agents/`, resolvendo para `docs/agents/agents/`. Resíduo de movimentação, que é exatamente o que o CA existe para pegar |
| CA7 processo de alteração dos arquivos de agente | ✅ **escrito** no `CONTRIBUTING.md`: PR próprio, aprovação do papel *Dono do NLP Engine*, regra alterada declarada em uma frase, e entrada nos dois índices |
| CA8 o site não inclui a meta-documentação | ✅ o `pdoc` parte do código; `docs/*.md` não entra. Registrado, com a ressalva de reconferir se a ferramenta mudar |

✅ **E o CA4 ganhou gate:** `test_indice_de_docs_cobre_tudo` reprova quando um `.md` novo de
`docs/` não é citado no índice. Sem isso a classificação era verdadeira no dia e falsa no mês
seguinte.

# 3. `253589` [P2-23] — referência de API dos docstrings

CA1 a CA6, CA8 e CA9 ✅ — `make docs-build` com `pdoc`, regra `D` do ruff ativa com convenção
Google, `doctest` no gate e no hook.

🔴 **CA7 NÃO era gap — eu errei.** O `site/` já saía do agente como artefato `docs`, publicado
por `PublishPipelineArtifact` logo depois do `make build`. **O card estava fechado neste ponto
antes de eu tocar em qualquer coisa.** O passo que acrescentei foi removido em `db695da`, e o
passo que existia ganhou o comentário apontando qual CA ele atende.

# 4. `253594` [P3-28] — hooks de pre-commit

| CA | estado |
|---|---|
| CA1 config com versões fixadas | ✅ já estava |
| CA3 `detect-private-key` | ✅ **adicionado** — e com ele o guarda local, porque **`detect-private-key` não pega token do Databricks**: ele procura bloco PEM, e o token é string opaca. São dois formatos, e só os dois juntos cobrem |
| CA6 mesmas regras do `make check` | ✅ já estava, e o teste do espelho **foi corrigido**: ele pegava o primeiro `rev:` do arquivo e passou a comparar o hook errado quando outro repositório entrou antes do ruff. Falha do teste, exposta pela mudança |
| CA7 `make hooks-install` + doc | ✅ **criados** `hooks-install` e `hooks`, e o `CONTRIBUTING` passou a citá-los |
| CA8 job de CI rodando os hooks | ✅ **adicionado** `make hooks` depois do `make check` |
| CA2 violação trivial é corrigida ou bloqueia | ✅ **demonstrado** — arquivo com `import sys`/`import os` fora de ordem e espaço no fim da linha: o hook **falhou o commit** e corrigiu os dois (`Found 2 errors (2 fixed, 0 remaining)`) |
| CA5 `pre-commit run --all-files` passa | ✅ **os 9 hooks verdes**, exit 0, sobre a base inteira |
| CA4 hooks ≤ 5s em commit típico | 🔴 **NÃO ATENDIDO — 9,7s.** Medido, não estimado |

🔴 **Achado que o card não previa: `pre-commit` não era dependência declarada.** O `CONTRIBUTING`
mandava rodar `uv run pre-commit install` — comando que não funciona. Adicionado ao grupo `dev`.
**Isso também derruba o CA2 do `253590`** (*"todos os comandos citados no CONTRIBUTING funcionam
literalmente"*), que estava marcado como atendido.

⚠️ **`end-of-file-fixer` e `trailing-whitespace` seguem de fora**, agora por escolha e não por
impossibilidade: o `ruff-format` já normaliza os `.py`, e os dois acrescentariam custo ao estágio
que justamente está acima do alvo.

## 4.1 🔴 O CA4: 42s → 9,4s, e o alvo de 5s colide com o CA3 do próprio card

Medido pelo **caminho real do hook** — `.venv/Scripts/pre-commit.exe`, que é o que o
`.git/hooks/pre-commit` invoca —, commit de 5 arquivos:

| configuração | tempo |
|---|---|
| tudo no commit (como estava) | **~42 s** |
| com os checks de projeto no push | **~9,7 s** (mediana de 5 rodadas, 8,8 a 10,5) |
| alvo do CA4 | ≤ 5 s |

**O que saiu para o `pre-push`:** `doctest` (~12,5 s), `api-ref-check` (~7,7 s) e
`api-surface-check` (~7,4 s) — os três varrem `src/` inteiro e **não olham para o arquivo que
mudou**. Somam 27,6 s dos 42 s. É o mesmo critério que já tinha posto a cobertura no push, e
agora está pinado por teste (`test_checks_de_projeto_inteiro_rodam_no_push`), com o contrapeso
que impede esvaziar o commit (`test_gates_por_arquivo_ficam_no_commit`).

### O orçamento decomposto — mediana de 3 rodadas

| item | custo | acumulado |
|---|---|---|
| **piso** — `pre-commit` subindo, com todos os hooks pulados | **2,71 s** | 2,71 s |
| `detect-private-key` — **exigido pelo CA3** | +1,13 s | 3,84 s |
| `ruff` | +0,56 s | 4,40 s |
| `ruff-format` | +~0,5 s | ~4,90 s |
| guarda de segredo | +0,53 s | **~5,4 s** |
| `check-added-large-files` | +1,4 s | 6,8 s |
| `mypy` | +3,65 s | **9,4 s** ← estado atual |

🔴 **O conjunto mínimo que atende o CA2 e o CA3 já custa ~5,4 s**, antes de qualquer gate de
qualidade entrar. **O CA4 colide com o CA3 do mesmo card**, e isso não aparece lendo os critérios —
só medindo.

Configurações testadas, todas acima do alvo: sem `mypy` **6,85 s**; sem `mypy` e sem
`check-added-large-files` **5,20 s**. Remover o `detect-private-key` economizaria 1,13 s e feriria o
CA3 — trocar um critério por outro não é atender.

### O que foi decidido

✅ **O `mypy` FICA no estágio de commit.** Chegou a ser proposto movê-lo para o `pre-push`, e a
proposta não se sustentou em duas frentes:

- **não fecha o CA4** — 6,85 s continua acima de 5 s;
- **e o custo é real:** no commit, o erro de tipo aparece no commit que o introduziu; no push, pode
  aparecer com vários commits empilhados, e corrigir vira `amend` ou rebase. Isso o diferencia dos
  três que foram para o push — `doctest`, `api-ref-check` e `api-surface-check` verificam
  **artefato de consistência**, enquanto o `mypy` verifica **o código recém-escrito**.

ℹ️ O argumento estrutural a favor de mover existe e fica registrado: o `mypy` é o **único hook do
estágio de commit com `pass_filenames: false`** — varre o projeto inteiro independente do que
mudou, que é a propriedade dos quatro do push. E não dá para torná-lo por arquivo: `strict` precisa
do grafo de imports completo.

⚠️ **Três atribuições minhas caíram na própria medição**, e ficam aqui porque duas quase viraram
mudança inútil:

1. os ~4,3 s que atribuí ao `uv run` eram da invocação **externa** (`uv run pre-commit`), que o
   commit real não paga — o caminho real é `.venv/Scripts/pre-commit.exe`;
2. trocar o interpretador do guarda, feito com base nisso, **não economizou nada**;
3. medir o custo **marginal** dos hooks (4,14 s, "dentro do alvo") era **redefinir a métrica até
   ela caber** — quem faz `git commit` espera o total.

### O que se leva como contraposição

O alvo de 5 s é **exemplo** no card, não requisito medido. A contraposição é a tabela acima: o piso
não é gate, e o mínimo exigido por outros CAs já o estoura. A intenção declarada — *"hook de commit
que custa isso é hook que se desliga"* — foi de **42 s para 9,4 s**.

⚠️ **A medição é n = 1 máquina.** O que a enfraquece não é o sistema operacional — hook de
pre-commit **só roda na máquina de quem programa**, então Windows corporativo é o ambiente real, não
uma extrapolação. O que falta é amostra. **Decisão: não medir em outra máquina**; a evidência atual
basta para a contraposição.

ℹ️ **O primeiro build dará o número de Linux**, porque o estágio `CI` roda em PR de qualquer branch
em `ubuntu-latest` e agora executa `make hooks`. ⚠️ **Mas não é a medição do CA4** — é
`--all-files` sobre o repositório inteiro, carga diferente de um commit de 5 arquivos.

## 4.2 ✅ E o hook pagou o próprio custo no primeiro uso

Ao rodar o estágio de push, o `pytest` reprovou com
`ModuleNotFoundError: No module named 'scripts'` em `tests/test_guarda_segredos.py`.

**Era defeito real, e da classe exata que o P3-28 existe para pegar:** o teste passava com
`python -m pytest` (o `-m` põe o diretório corrente no `sys.path`) e falhava com `uv run pytest`,
que é o que o CI executa. **Verde local, vermelho na esteira.**

Corrigido em `pyproject.toml`: `pythonpath = ["src", "."]`. O `.` entra porque os testes do guarda
importam de `scripts/`.

⚠️ **Sem o hook de push, isso teria ido para o CI.** Os dois estágios rodados e verdes:
6 hooks no commit, 10 no push, incluindo a suíte com cobertura.

# 5. `253587` [P2-21] — `parametrize` na suíte

CA1 ✅ zero `test_*_case_N`. CA2 ✅ (ver §0). CA7 ✅ `pytest-randomly` e `pytest-xdist` declarados.

**CA5 era o gap real** — pedia caso no limite exato de cada banda de incerteza, entrada vazia,
`None` e `NaN`, e **nada disso estava pinado**.

✅ **`test_bordas_de_banda_e_degenerados.py`, 31 casos.** A banda é fechada nos dois extremos
(`lo <= v <= hi`), o limite exato **consulta o juiz**, `NaN` vira `0.0` e fica fora, e texto vazio
não levanta.

🔴 **E isto não é caixinha marcada.** É o mesmo parâmetro que a medição do P0-29 de hoje
responsabilizou: a linha com `uncertainty_band: [0.35, 0.65]` entregou 36 laudos relevantes sem
nenhum span positivo de regra, com score de 0,367 a 0,566, porque o piso está **abaixo** do teto
analítico de 0,597. O teste pina onde a decisão muda de lado.

✅ **Dois mutantes mortos, verificados:** abrir a banda (`lo < v < hi`) derruba **5** casos; remover
o guarda de `NaN` do `_clip01` derruba **6**. Arquivo restaurado do git depois de cada mutação, e a
árvore reconferida limpa.

---

# 6. Estado do gate

`ruff check` ✅ · `ruff format --check` ✅ 123 arquivos · `mypy strict` ✅ 38 arquivos ·
`api-ref-check` ✅ · `api-surface-check` ✅ · `doctest` ✅ 67 passam, 13 pulam ·
cobertura **87,77%** por ramo, piso 85% — subiu de 87,59%.

**Suíte: 1.155 → 1.202 testes, todos passando**, em 93s. Os **47 novos** são 31 de borda de banda
e valores degenerados, 13 do guarda de segredos e 3 do índice de docs.

# 7. O que fica aberto

- 🔴 **`253594` CA4 — 9,4 s contra o alvo de 5 s.** Fica **aberto**, com a decomposição como
  contraposição: o piso de 2,71 s não é gate, e o mínimo exigido pelo CA2 e pelo CA3 já custa
  ~5,4 s. **CA1, CA2, CA3, CA5, CA6, CA7 e CA8 verificados e atendidos.**
- 🟡 **Três commits locais, sem push:** `f4a8994` (fecho dos CA), `ef524c2` (casos de borda),
  `db695da` (remoção do artefato duplicado). Árvore limpa, gate verde nos dois estágios.
- ℹ️ **Nada tocado no board** — os cinco cards seguem em *Pronto para QA*, sem comentário de
  evidência.
- 🟡 **`253590` [P2-24] CA8** — *"validação por terceiro: pessoa que não escreveu o documento
  consegue montar o ambiente e abrir um PR seguindo só o CONTRIBUTING"*. Não é verificável por
  quem escreveu, por construção.
- 🟡 **`253586` [P2-20]** segue fora deste passe — 13 arquivos de teste com builder local e um
  `conftest` já em 358 linhas. É refinamento, não fecho.
