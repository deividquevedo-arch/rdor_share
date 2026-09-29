# Migração de doenças biliares e neuroimunologia — inventário do legado

> Etapa de **Research**. Levantado em **14/09/2026**, direto do notebook legado.
> Alvo: as duas linhas em produção até 18/09.

---

## 1. Fonte — confirmada, e desta vez a cópia local serve

| verificação | resultado |
|---|---|
| repositório | `fabrica-ia-plataforma` — é o único; nem `fabrica-ia-lib` nem `fabrica-ia-nlp-platform` têm as linhas |
| cópia local × `origin/hml` × `origin/main` | **blob idêntico nos três**, para os dois notebooks |
| `origin/hml` | `5c18fae`, 04/09/2026 · `origin/main` `c898fba`, 11/08/2026 |

ℹ️ Diferente da reumatologia, onde a cópia local era anterior ao PR que mudou a régua e custou uma
primeira tentativa inteira. Aqui não há divergência a contornar.

---

## 2. Os dois legados são o MESMO motor

**45 linhas diferem em 5.210.** O que varia é `TARGET_ORGAN`, os nomes de tabela de entrada e saída,
a query da janela de datas, e dois pares de termos.

**Consequência para o plano:** uma extração serve as duas — e também birads, endometriose e nódulo
pulmonar, que vivem no mesmo formato.

### 2.1 O `CONFIG` já está no formato que o `nlp_engine` espera

Chaves de topo: `negation`, `organs`, `findings`, `semantic`. Mesma situação do ca-cólon —
**migração é tradução de config, não reescrita.**

---

## 3. As duas réguas, medidas

### 3.1 `doencas_biliares` — régua pequena, portão pequeno

**6 categorias, 41 termos** · portão de órgão com **18 seeds + 5 regex**.

| categoria | termos | exemplos |
|---|---|---|
| colecistite cronica | 12 | *calculo no ducto cistico*, *fibrose da vesicula* |
| diagnosticos | 8 | *colecistite cronica*, *coledocolitiase*, *ileo biliar* |
| neoplasias | 8 | *polipo sugestivo de malignidade*, *massa sugestiva de neoplasia* |
| sinais inflamatorios | 7 | *espessamento de parede*, *colecao pericolecistica* |
| colelitiase | 3 | *calculo*, *sinal da parede-Eco-sombra*, *sinal WES* |
| coledocolitiase | 3 | *dilacao*, *calculo no coledoco*, *coledoco dilatado* |

### 3.2 `neuroimunologia` — régua mínima, portão grande

**2 categorias, 16 termos** · portão de órgão com **72 seeds + 37 regex**.

| categoria | termos | exemplos |
|---|---|---|
| `disgnosticos` | 9 | *Esclerose Multipla*, *Neuromielite Optica*, *NMO*, *NMOSD* |
| sinais | 7 | *Desmielinizante*, *Neuroinflamatorio*, *Neuroinflamacao* |

⚠️ O peso está **invertido** entre as duas: no biliar a régua decide e o portão é estreito; no neuro
a régua tem 16 termos e o portão tem 109 entradas. A migração do neuro é sobre o **portão**.

### 3.3 Parâmetros comuns

- **Negação:** 23 tokens, janela de **7**.
- **Semântica:** `min_sim_seed_vocab` 0,65 · `top_k_per_seed` 24 · `min_token_len` 3.

---

## 4. 🔴 Quatro riscos identificados antes de começar

### 4.1 O legado expande com embeddings, e os nossos não funcionam em produção

O bloco `semantic` do legado gera termos **por laudo**, com embeddings. É a mesma divergência que a
auditoria do DII classificou como *"expansão semântica por documento"* — e as três linhas em
produção que declaram embeddings rodam em `token_overlap` por `FileNotFoundError`.

**Consequência:** a paridade vai divergir por essa via, e a diferença **não é defeito da migração**.
Tem de ser medida e nomeada separadamente, senão vira ruído no `match_rate`.

### 4.2 Vocabulário estrangeiro no dicionário compartilhado

O `organs` carrega **20 órgãos**, e os dois maiores não são das linhas-alvo:

| órgão | peso | pertence a |
|---|---|---|
| `reumatologia` | 116 (97 seeds + 19 regex) | outra linha |
| `neuroimunologia` | 109 (72 + 37) | alvo |
| `colon_reto` | 58 (44 + 14) | outra linha |
| `doencas_biliares` | 23 (18 + 5) | alvo |

E `findings` tem quatro chaves — `colon_reto` (53 termos) e `reumatologia` (29) entram carregados nas
duas linhas. **Remover muda resultado: medir, não limpar no olho.** Mesmo padrão do ca-cólon.

### 4.3 A segmentação difere entre as duas

`FORCE_FULL_DOC_FOR = {"neuroimunologia"}` contra conjunto **vazio** no biliar. Mesma classe do
`mode: auto` da hepatologia, que descarta 86% dos laudos na segmentação. **Não clonar uma na outra.**

### 4.4 Defeitos no próprio legado, a não reproduzir

| defeito | onde | efeito |
|---|---|---|
| 🔴 **vírgula faltando** entre literais adjacentes | notebook do **neuro** | `'massa heterogenea com distensao vascular'` + `'paredes irregulares'` viraram **um** termo concatenado; idem `'ducto bilar comum'` + `'vesicula'` |
| 🔴 **erro de grafia em seed** | `ducto bilar comum` | *bilar* por *biliar* — âncora provavelmente morta |
| ⚠️ grafia da chave | `disgnosticos` no neuro | só nome de categoria, sem efeito — mas mede a deriva do copia-e-cola |

ℹ️ Os dois primeiros estão nas entradas de `doencas_biliares`, e o notebook em que aparecem é o do
**neuro** — que não as usa. Efeito prático provável: zero. **Valor real:** provam que as configs
derivaram por cópia, e é a classe de defeito que a migração deve **contar**, não herdar.

---

## 5. O que falta levantar

- [ ] **Volumetria** das duas linhas — laudos/dia e relevantes/dia em produção.
- [ ] **Gabarito:** existe conferência clínica, ou só a saída gravada?
- [ ] **Saída gravada** de uma janela de ≥ 3 dias, para medir paridade.
- [ ] **`gold_filter`** de cada linha — o filtro de entrada é invisível à paridade e só se mede rodando.
- [ ] 🔴 **Schemas `doencas_biliares` e `neuroimunologia`** em dev **e prd** — criar schema é do time
      da Fábrica, e na reumatologia a ausência em prd virou bloqueio na hora de promover.
