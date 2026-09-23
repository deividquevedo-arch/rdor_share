# Checklist de revisão — PR do time de DS

> **Todo item aqui já pegou alguma coisa, ou já me fez errar.** Não é boa prática genérica.
> Ordem importa: os blocos 1 e 2 evitam apontamento errado, que custa mais caro que apontamento
> esquecido.

---

## 1. Antes de abrir o diff

- [ ] **O que o merge arrisca?** Responder isso ANTES de listar bloqueio. Separar em três:
      *bloqueia o merge* · *bloqueia o despause/promoção* · *item do repositório, não deste PR*.
- [ ] **Diff contra o `merge-base`**, nunca contra a ponta do alvo — senão o que a branch **não
      tem** aparece como mudança (já virou 44 arquivos onde havia 6).
- [ ] **`mergeStatus`** antes de exigir sync. `succeeded` = não há conflito a resolver.

## 2. Antes de apontar qualquer coisa

- [ ] **É desvio dele ou padrão da plataforma?** `python .claude/scripts/e-desvio-ou-padrao.py <chave>`
      Cobrar de um autor o que 6 das 7 configs fazem igual é ruído — e desloca a atenção do que
      importa.
- [ ] **É alçada dele?** POP-IA-08: config da especialidade e `jobs/definicoes` são do cientista;
      a lib e o `ORGANS_SHARED` são do dono do NLP Engine; cluster, catálogo, schema, grant e
      Volume são do administrador Databricks.
- [ ] 🔴 **Conferir a afirmação DELE no código.** Se ele justifica uma escolha citando um arquivo,
      abrir o arquivo. *Três apontamentos meus caíram assim num único PR.*

## 3. Config de especialidade

- [ ] **Sem bloco morto:** `catalog`, `monitoring`, `distribution` não são lidos e costumam apontar
      para o ambiente errado.
- [ ] **Chave que decide sempre declarada**, mesmo quando coincide com o default.
      `nlp.llm_router.enabled` é o caso canônico: ausente a lib assume `False`, mas um bloco com
      `mode`, `model` e `prompt_system` faz quem lê concluir o contrário.
- [ ] **Cabeçalho e changelog NO ARQUIVO** — histórico que só existe no `git log` não chega a quem
      abre a config.
- [ ] **Perfil de entrega:** `rule_only` é estágio de desenvolvimento. **Nenhuma lista vai ao
      negócio a partir de perfil parcial** — trava a promoção a prd, não o merge para hml.
- [ ] **Incoerências que anulam camadas:** `decision_mode: hybrid` com `use_embeddings: False`;
      `llm_router` completo sem `enabled`; `uncertainty_band` que não alcança a faixa onde o juiz
      deveria rodar.

## 4. A evidência que ele apresenta

- [ ] **A medição tocou a população?** Coorte sem os casos afetados dá delta zero e não mede nada.
      Exigir a **pré-condição impressa junto com o resultado**.
- [ ] 🔴 **Zero e 100% são gatilho.** Quase nunca são ausência: são filtro que não casou, acento em
      SQL, campo que não existe naquela versão, caminho que nunca rodou.
- [ ] **Denominador declarado** em todo percentual.
- [ ] **Quem adjudicou?** Se foi quem fez a migração, **não é homologação** — e isso precisa estar
      escrito, não descoberto depois.
- [ ] **O run fechou em sucesso** não prova que gravou a coorte. Conferir o que foi escrito.

## 5. Higiene do PR

- [ ] **Work item vinculado**, com número **e** título.
- [ ] **Citações apontam para arquivos que existem** na árvore do commit.
- [ ] **Zero PHI** em notebook, log ou fixture.
- [ ] **Descrição com no máximo 4.000 caracteres** — o que não couber vai para a SPEC.
- [ ] Exchange: **os três ambientes no mesmo PR** (dev, hml, prd). Prd ficar para trás já entregou
      coluna de achado vazia em 100% das linhas.

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

E quando um apontamento meu cai, **assumir explicitamente** — inclusive orientação que eu mesmo
dei antes e que o dado depois desmentiu.
