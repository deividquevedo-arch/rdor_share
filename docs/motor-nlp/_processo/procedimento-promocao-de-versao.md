# PROPOSTA — Promoção de versão da lib

> ⚠️ **Isto é uma proposta, não uma norma.** Nada aqui vale como regra até o time discutir e
> aprovar. **Não está publicado na wiki.**
>
> Se aprovado, o destino é a wiki `IA.wiki`, seção `/Fábrica de IA`, ao lado de *Checklist de
> aprovação do PR* e *Gitflow e ambientes* — e passa a ser a fonte, com o repositório apontando
> para lá. Está escrito no formato daquelas páginas justamente para que a aprovação seja a única
> coisa que falte.

---

## 0. O que se pede ao time

**O problema.** A pinagem por linha e o fluxo acordado invocam a palavra *"validado"* — *nenhuma
versão nova entra sem validação prévia*. **Essa palavra não está definida em lugar nenhum.** Sem
definição, cada bump reabre a mesma discussão, e a validação vira o que cada um entende por ela.

**O que NÃO resolve.** O playbook de paridade existente trata de evoluir **config e régua** contra
gabarito. Aqui a config fica congelada, só a engine muda, e **não existe gabarito** — precisão e
recall não são calculáveis. São problemas diferentes.

**A decisão pedida:**

1. O time adota esta definição de *"validado"* para troca de versão em linha de produção?
2. Os **quatro números** da seção 3 e o **critério de aceite** da seção 6 são o mínimo aceitável?
3. Aprovado, a página vai para `/Fábrica de IA` e os cards passam a **citá-la por link**, em vez de
   reescrever a regra.

**O que muda na prática, se adotado.** Quem promove versão entrega quatro números em vez de
*"rodou sem erro"*. Quem revisa tem critério objetivo para aceitar ou devolver. Nenhum passo novo é
criado — o que existe hoje passa a ter nome e piso.

---

## O texto proposto

O que você precisa medir para trocar a versão da `nlp_engine` de uma linha em produção — e o que
não conta como validação. Todos os itens vêm de medições reais deste projeto; nenhum é hipotético.

> **A régua em uma frase.** Trocar a versão não é validado por *"rodou sem erro"*. É validado por
> **quantos laudos mudaram de decisão, em que direção, e por qual chave** — com a prova de que a
> coorte continha os casos afetados.

---

## 1. Quando se aplica

| situação | aplica? |
|---|---|
| trocar o pin de uma linha em produção | ✅ sim |
| linha nova entrando em produção | ✅ sim — entra na versão pinada vigente |
| mudar config sem mudar a engine | ❌ não — é o *Checklist de aprovação do PR* |
| publicar a lib sem entrar em nenhuma linha | ❌ não — é o gate da esteira |

---

## 2. Por que não dá para usar a régua de config

Quando você muda a config, existe gabarito: alguém anotou o que era certo, e você mede precisão e
recall contra isso.

**Quando você muda só a versão da lib, não existe gabarito.** Ninguém anotou a resposta certa para
os laudos da coorte. Então **precisão e recall não são calculáveis** — qualquer número que os alegue
está comparando saída de máquina contra saída de máquina.

O que se mede é o **delta de decisão**: quantos laudos mudam, para que lado, e por qual chave.

---

## 3. Os quatro números — toda validação entrega os quatro

| número | o que é |
|---|---|
| **rebaixados `1 → 0`** | deixaram de ser entregues |
| **promovidos `0 → 1`** | passaram a ser entregues |
| **exercitam o caminho** | quantos laudos passaram pelo código que mudou |
| **denominador** | total de entregas na coorte |

🔴 **Zero sem "exercitam o caminho" não vale nada.** Um run de dev deu zero divergência porque a
coorte **não continha a população** — os laudos afetados não estavam ali. *"Está seguro"* e *"a
coorte não tinha o caso"* aparecem exatamente iguais na tela.

⚠️ Percentual sozinho não é aceito. **Número absoluto e denominador**, sempre.

---

## 4. Como medir

1. **Dois motores, mesma execução, mesma entrada.** As duas versões rodam sobre os mesmos laudos, no
   mesmo processo. Rodar em momentos diferentes mistura versão com mudança de corpus.
2. **LLM e embeddings desligados dos dois lados.** Ligados, o delta mistura versão com
   não-determinismo do modelo.
3. **Versão pinada nos dois braços**, nunca `latest`.
4. **Escolha a coorte pela população, não pela data.** Antes de rodar, responda: *quantos laudos
   deste dia exercitam o caminho que mudou?* Se for zero, a coorte não serve.
5. **Cada laudo que mudou tem a chave que o explica** — o campo da trilha que justifica a mudança.
   Mudança sem chave é mudança não atribuída.
6. **Declare a direção esperada ANTES de rodar.** Correção que só remove não pode promover. Se
   promover, o resultado é investigação, não aceite.

---

## 5. As armadilhas — cada uma já invalidou uma medição aqui

| armadilha | como ela aparece |
|---|---|
| **Coorte sem a população** | zero divergência que parece segurança |
| **Comparar contra produção** | produção chama o LLM e o braço "depois" não — o delta vira ruído |
| **`limit_rows` para isolar coorte** | o teto corta **depois** da união da fila, e os reprocessados ficam por último: o run fecha **com sucesso sem tocar a coorte** |
| **Ler o campo de topo achando que é o aninhado** | `llm_called` existe no topo (juiz) **e** por critério — são coisas diferentes |
| **Erro de rede tratado como fim-de-dados** | `if not linhas: break` faz run parcial sair como completo. Compare processados × total da coorte |
| **Validar a injeção em vez do artefato** | o braço "depois" tem de ler o arquivo que **vai ser entregue**, e abortar se divergir |
| **`latest` em qualquer dos braços** | a versão muda no meio da medição, sem sinal nenhum no resultado |

---

## 6. Critério de aceite

A promoção passa quando os três valem:

- [ ] **A pré-condição está impressa** — número de laudos que exercitaram o caminho, com alvo
      declarado antes. Referência usada até aqui: **≥ 30**.
- [ ] **Todo laudo que mudou tem chave que o explica**, e a direção bate com a declarada.
- [ ] **Nenhuma mudança na direção não prevista** — ou, havendo, cada caso investigado e registrado.

Falhando qualquer um, **não promove**. E o que precisa de conserto é a coorte ou o harness, não o
critério.

---

## 7. Onde cada coisa fica registrada

| o quê | onde |
|---|---|
| **esta regra** | na **wiki**, se aprovada — é política do time, não artefato de projeto |
| a **evidência** de uma promoção (os quatro números, coorte, data) | no **card** daquela versão |
| em que versão cada linha está agora | no `ESTADO.md` do repositório de documentação |

⚠️ **Regra e convenção não moram em card.** Card registra o que foi medido numa aplicação
específica; a política vive na wiki e é citada por link. Card que reescreve a regra cria uma segunda
fonte, e as duas divergem na primeira mudança.

---

## 8. 🔴 Bloqueio conhecido — hoje isto não é executável para versão de `hml`

O cluster de **dev** resolve o `pip` contra o feed **`fabrica-ai`**, que é o de **produção**. Versão
publicada só pela `hml` **não instala em dev**, e o Volume deixou de ser rota de instalação.

Na prática: hoje só se valida em dev uma versão que **já está em produção**. Enquanto isso não muda,
ou se promove sem validar, ou não se valida.

✅ **A correção é pequena e é de MLOps:** declarar `fabrica-ai-hml` como índice extra do `pip` no
cluster de dev — e só de dev.
