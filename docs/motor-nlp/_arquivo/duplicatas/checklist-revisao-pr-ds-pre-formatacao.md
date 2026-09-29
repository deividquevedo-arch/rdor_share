# Checklist de revisão — PR do time de DS

> **Todo item aqui já pegou alguma coisa, ou já produziu apontamento errado.** Não é boa
> prática genérica.
> Ordem importa: os blocos 1 e 2 evitam apontamento errado, que custa mais caro que apontamento
> esquecido.


## Como usar

Percorrer na ordem. Os blocos **1** e **2** existem para evitar apontamento errado, que custa mais
caro que apontamento esquecido: um apontamento errado obriga o autor a provar que está certo e
desloca a atenção do que importa.

Fechar a revisão separando em três listas: **bloqueia o merge** · **bloqueia a promoção a
produção** · **item do repositório, não deste PR**. Um item que não cabe em nenhuma das três
provavelmente não é da revisão.

---

## 1. Antes de abrir o diff

- [ ] **O que o merge arrisca?** Responder isso ANTES de listar bloqueio. Separar em três:
      *bloqueia o merge* · *bloqueia o despause/promoção* · *item do repositório, não deste PR*.
- [ ] **Diff contra o `merge-base`**, nunca contra a ponta do alvo 
      — senão o que a branch **não tem** aparece como mudança (já virou 44 arquivos onde havia 6).
- [ ] **`mergeStatus`** antes de exigir sync. `succeeded` = não há conflito a resolver.

## 2. Antes de apontar qualquer coisa

- [ ] **É desvio real ou padrão da plataforma?**
      Cobrar de um autor o que 6 das 7 configs fazem igual é ruído — e desloca a atenção do que
      importa.
- [ ] **É alçada dele?** POP-IA-08: config da especialidade e `jobs/definicoes` são do cientista;
      a lib e o `ORGANS_SHARED` são do dono do NLP Engine; cluster, catálogo, schema, grant e
      Volume são do administrador Databricks.
- [ ] 🔴 **Conferir a afirmação DELE no código.** Se ele justifica uma escolha citando um arquivo,
      abrir o arquivo. *Três apontamentos caíram assim num único PR.*

## 3. Config de especialidade

- [ ] **Sem bloco morto:** `catalog`, `monitoring`, `distribution` não são lidos e costumam apontar
      para o ambiente errado.
- [ ] **Chave que decide sempre declarada**, mesmo quando coincide com o default.
      `nlp.llm_router.enabled` é o caso canônico: ausente a lib assume `False`, mas um bloco com
      `mode`, `model` e `prompt_system` faz quem lê concluir o contrário.
- [ ] **Cabeçalho e changelog NO ARQUIVO** — histórico que só existe no `git log` não chega a quem
      abre a config.
- [ ] **Perfil de entrega:** `rule_only` é estágio de desenvolvimento. **Nenhuma lista vai ao
      negócio a partir de perfil parcial** — salvo exceções previamente acordadas (migração)
- [ ] **Incoerências que anulam camadas:** `decision_mode: hybrid` com `use_embeddings: False`;
      `llm_router` completo sem `enabled`; `uncertainty_band` que não alcança a faixa onde o juiz
      deveria rodar.

## 4. A evidência que ele apresenta

- [ ] **A medição tocou a população?** Coorte sem os casos afetados dá delta zero e não mede nada.
      Exigir a **pré-condição impressa junto com o resultado**.
- [ ] 🔴 **Zero e 100% são gatilho.** Quase nunca são ausência: são filtro que não casou, acento em
      SQL, campo que não existe naquela versão, caminho que nunca rodou.
- [ ] **Denominador declarado** em todo percentual.
- [ ] **Quem avaliou os casos?** Se foi quem fez a migração, **não é homologação** — e isso precisa
      estar escrito, não descoberto depois.
- [ ] 🔴 **A avaliação clínica cobriu os DOIS lados?** Só o que a versão nova deixou de entregar
      mede **recall**. Sem olhar o que ela passou a entregar **não há precisão**, e o PR não pode
      afirmar que melhorou.
- [ ] **O run fechou em sucesso** não prova que gravou a coorte. Conferir o que foi escrito.

## 5. Higiene do PR

- [ ] **Work item vinculado**, com número **e** título.
- [ ] **Citações apontam para arquivos que existem** na árvore do commit.
- [ ] **Zero PHI** em notebook, log ou fixture.
- [ ] **Descrição com no máximo 4.000 caracteres** — o que não couber vai para a SPEC.
- [ ] Exchange: **os três ambientes no mesmo PR** (dev, hml, prd). Prd ficar para trás já entregou
      coluna de achado vazia em 100% das linhas.

---

## 6. 🔴 Só para PR de linha NOVA

Os blocos acima valem para qualquer PR. Estes só aparecem quando a especialidade está **entrando**,
e cada um custou um bloqueio real.

### Antes de olhar o diff: o PR foi precedido de run?

- [ ] 🔴 **Houve execução ponta a ponta em dev, com envio?** Validação local prova a **régua** e não
      toca runner, `gold_filter`, `column_map`, view nem envio. Na reumatologia, **seis bloqueios só
      apareceram rodando**. PR de linha nova sem run é PR sem evidência.
- [ ] **A paridade foi medida contra a saída GRAVADA do legado**, não contra a régua lida.
- [ ] ⚠️ **A fonte do legado é a branch `hml` do repositório legado**, nunca a cópia local — a cópia
      costuma estar meses atrás, e a primeira tentativa da reumatologia usou a errada.

### Filtro de entrada

- [ ] 🔴 **O `gold_filter` foi medido nos DOIS sentidos, com custo?** `match_rate` só mede o que
      **chega** ao motor: filtro que perde exame não aparece em métrica de paridade nenhuma.
- [ ] ⚠️ **Ele lê `proced_descricao`, não o laudo.** Palavra-chave escrita supondo o texto do laudo
      perde exame em silêncio — no ca-estômago deixava de fora **124 legíveis/dia** contra 106 que trazia.

### Definição de job

- [ ] 🔴 **`nlp_engine_version` é o LITERAL, não `${nlp_engine_version}`.** Com a variável a linha
      troca de versão sozinha — o `cancer_colon` variou três vezes em três dias.
- [ ] 🔴 **A task de `api_*` declara `disabled`.** Sem isso, a execução em homologação **posta
      inferência no sistema real de Navegação**. Verificável em um comando: comparar com as outras
      definições, que declaram.
- [ ] **`pause_status`** coerente com as demais — é padrão, não escolha do autor.

### Arquivos de navegação e exchange

- [ ] **Os três ambientes no mesmo PR.** Prd ficar para trás já entregou a coluna de achado **vazia
      em 100% das linhas**.
- [ ] **`id_linha_navegacao`** confirmado com quem opera o destino, não inferido do nome.
- [ ] ⚠️ **`descriptografia` é bloco morto** — a view já entrega em claro. Cobrar sua presença é
      apontamento errado; encontrá-lo numa linha antiga é item do repositório, não deste PR.

### Régua e configuração da linha nova

- [ ] 🔴 **`segmentation.mode` não se clona de outra linha sem medir.** `auto` descarta
      IMPRESSÃO/CONCLUSÃO: na hepatologia são **86% dos laudos** com cobertura abaixo de 1,0.
- [ ] 🔴 **Vocabulário estrangeiro no dicionário de órgãos compartilhado.** Régua migrada costuma
      carregar termos de outra especialidade — no notebook do biliar, `colon` aparece **77 vezes**.
      Remover muda resultado: **medir, não limpar no olho**.
- [ ] **Perfil de entrega** — ver bloco 3. `rule_only` não vai ao negócio (SALVO EXCEÇÕES PREVIAMENTE ACORDADAS).

### O que depende de terceiro e **não** entra no PR

- [ ] **Schema provisionado** em dev, hml e prd — é do time da Fábrica, por fluxo próprio.
      **sinalizar isso na descrição do PR é o lugar errado**.
- [ ] **Grants** (`USE CATALOG`, `EXECUTE` em função de decriptação) — mesma coisa.

---

## 7. 🔴 PR que corrige filtro de entrada ou régua

- [ ] 🔴 **Escape de regex em literal SQL foi VERIFICADO no Spark, não deduzido.** O predicado do
      `gold_filter` passa por `F.expr`, então o literal é lido pelo parser SQL: `\b` ali dentro é o
      caractere **BACKSPACE**, não borda de palavra, e o termo casa **zero**. Para chegar ao motor de
      regex é preciso `\\b` no valor. A verificação cabe numa consulta:

      ```sql
      rlike '(?i)\bp[e]s?\b'    -- casou 0
      rlike '(?i)\\bp[e]s?\\b'  -- casou 1.123
      ```

      Mesma classe já encontrada em `cancer_estomago` (`'\beda\b'`, keyword inerte desde sempre).

- [ ] **Cada termo foi medido ISOLADO, não o pacote junto.** Na reumatologia o conjunto deu +47% de
      entrada; decomposto, um único termo (`pelve`) respondia por +43% e foi descartado. Pacote
      medido junto esconde o termo que estraga.

- [ ] 🔴 **O PR distingue "corrige defeito" de "muda régua"?** São regimes diferentes: defeito
      entra com medição; **mudança de régua exige evidência de qual achado real ela traz** e rodada
      com o negócio. Misturar os dois no mesmo PR atrasa o que já estava provado.

- [ ] **O que o PR acrescenta REPRODUZ o legado ou inventa critério?** Ler o filtro e a régua do
      legado antes de aprovar **ou** de cobrar. Exclusões de `paaf`, `punção` e `biópsia` na
      reumatologia pareciam critério novo e estavam no legado desde sempre.

- [ ] **Escopo é o que o negócio declarou, não julgamento clínico do revisor.** A fonte é o Data
      Card mais as alterações registradas em PR. Termo que não consta de nenhum dos dois **não é
      redução de escopo** quando fica de fora — é o escopo.

- [ ] ⚠️ **Ampliar o filtro invalida a comparação com a homologação anterior.** O corpus muda, e
      taxa de relevância medida antes deixa de ser comparável. Dizer isso na descrição.

---

## O que NÃO entra numa revisão de PR

| item | onde vai |
|---|---|
| criar schema, liberar grant, provisionar Volume | fluxo próprio do time, **nunca** no PR |
| defeito de código compartilhado achado de passagem | card próprio, ligado |
| mudança de régua clínica | dono clínico ou de negócio |
| dimensionamento para produção num PR que tem alvo `hml` | confunde duas réguas |

---

## Lembrete de tom

Revisão que só lista o que falta desmotiva e esconde o que importa. **Dizer o que está bom é
informação**, não cortesia: separa "o autor não sabia" de "o autor decidiu e documentou".

E quando um apontamento cai, **assumir explicitamente** — inclusive orientação dada antes pelo
próprio revisor e que o dado depois desmentiu.
