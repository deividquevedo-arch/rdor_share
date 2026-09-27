# Plataforma NLP — observações de uso e um pedido

**Data:** 2026-08-04 · **Para:** MLOps · **Origem:** validação de `transplante_pulmao` e `tirads` em `dev`

> Vale para qualquer especialidade. As entregas específicas de **transplante de pulmão** e
> **TI-RADS** estão anexas ao mesmo card.

---

## 1. Widgets com default que entrega o lote errado

| widget | default | efeito |
|---|---|---|
| `date_range_enable` | `false` | **`start_date`/`end_date` são ignorados** (`ntb_ia_motor_e2e.py:301`) |
| `limit_rows` | `100` | corta o lote em 100 linhas |

Os dois executam **sem erro**. Quem reproduzir uma validação com os defaults obtém números diferentes e conclui, erradamente, que não bate. Preenchimento completo dos 15 widgets está nas docs de entrega.

Sugestão: `date_range_enable` default `true`, ou falhar quando `start_date` for preenchido e o flag estiver `false`.

---

## 2. Schema = `specialty_id`, e precisa existir antes

O runner monta `{catalog}.{specialty_id}.tb_mod_diamond_{specialty_id}_...` — o schema **é** o `specialty_id`, não é configurável. As tabelas a plataforma cria sozinha; o schema não.

Em `diamond_ia_dev` existiam só `hepatologia`; `transplante_pulmao` e `tirads` foram criados pelo Diego. **`cancer_estomago` ainda não existe.**

Como não havia convenção estabelecida (confirmado com o João), adotamos **schema = `specialty_id`**, que é o que o código já impõe.

---

## 3. Laudo vazio entra no lote

O runner legado descartava laudo sem texto na entrada (`entrada.py:92`); a plataforma nova não faz essa checagem, porque o `gold_filter` virou predicado SQL e não há filtro de texto.

No pulmão: 2.085 linhas, **233 sem texto** — todas classificadas como não-relevante, corretamente. Não altera métrica, mas infla o denominador de negativos. Quem comparar totais sem saber disso vai achar que há divergência.

---

## 4. O pedido: um filtro por texto do laudo

**É o item de maior impacto desta lista.**

Hoje `gold_filter.keywords` vira predicado SQL sobre `proced_descricao` — o **nome do exame** (`plataform/data/ntb_ia_gold_filters.py:25,48`). No runner legado, a mesma chave era substring sobre o **texto do laudo**. Mesmo nome, critério diferente, troca silenciosa.

Custo medido no TI-RADS: com keywords só de tireoide, **25 dos 127 positivos da base ouro não chegaram ao motor** — 19,7% do recall. Ampliar as keywords recuperou 19, ao custo de +25% de volume. Recuperar os 6 últimos exigiria 2,7× o lote.

Com um estágio de filtro por texto **após o fetch**, a seleção poderia ser larga no nome do exame e precisa no conteúdo — recall completo sem multiplicar o volume que vai ao LLM.

Sugestão de forma: chave opcional em `data.filters`, ex. `text_filter: {keywords: [...], mode: any}`, aplicada depois do `array_join` do laudo e antes do `to_rows`.

---

## 5. Wheel nova ignorada em cluster quente — e `engine_version` mente

**É o mais perigoso da lista, porque o indicador de versão diz que deu certo.**

No `ntb_ia_motor_e2e` a instalação e o import ficam em sequência, sem reiniciar o Python:

```
226:  wheel = installer.bootstrap()      # instala a wheel resolvida
231:  from nlp_engine.nlp_engine...      # importa em seguida
```

Se o cluster já tinha o `nlp_engine` importado de uma execução anterior, o import **reaproveita o módulo em memória** e a wheel nova não passa a valer. Só que `engine_version` vem de `_package_version()`, que lê o **metadata em disco** — já atualizado. Resultado: a coluna mostra a versão nova e o código executado é o antigo.

Aconteceu conosco na validação da `0.7.2`: `engine_version = 0.7.2` na tabela, colunas novas ausentes. Resolveu reiniciando o cluster.

Duas correções possíveis, ambas simples:

- chamar `dbutils.library.restartPython()` logo após o `bootstrap()`, antes dos imports — é o que a bancada de validação (`plataform/tests/ntb_ia_validacao_lib.py:69`) já faz;
- ou, após o import, comparar `nlp_engine.__version__` com a wheel resolvida e **falhar** se divergir.

Sem isso, qualquer atualização de lib pode ser silenciosamente ignorada em cluster reaproveitado — e a evidência aponta para o lado errado.

---

## 6. Três coisas que falham em silêncio

**`gold_query` é ignorado** — zero ocorrências em `.py`. A doc de vocês (`boas-praticas/04`) já marca com 🔴 que o config de `transplante_pulmao` caía nisso e rodava sem filtro nenhum. Já convertemos o nosso, mas valeria o builder emitir warning ao encontrar a chave.

**`mode` é ignorado** — a combinação é sempre OR. Um `mode: all` na config passa despercebido.

**`parse_failed` fica enterrado no blob.** No pulmão, 4 laudos (0,19%) tiveram resposta do LLM em JSON malformado na extração quantitativa. Sem impacto lá — os 4 são negativos no gabarito —, mas quando a extração falha a medida vira `None` e o critério não promove: um caso com VEF1 < 40 real viraria falso negativo **sem emitir erro**. Hoje isso só existe dentro de `exm_laudo_resultado`, sem coluna própria, igual ao `llm_error`.

Duas mitigações: ligar `json_response_format` na config, e expor `parse_failed` no monitoramento.

---

## 7. O modelo de embeddings ficou no Volume antigo

A lib passou a ser publicada em `gold_fabrica_ia_hml/nlp_engine/nlp_engine_lib`, mas a pasta
`st_models/` **não** acompanhou: as configs de TI-RADS e hepatologia seguem apontando para
`/Volumes/diamond_ia_hml/nlp_engine/nlp_engine_lib/st_models/paraphrase-multilingual-MiniLM-L12-v2`.

**Hoje funciona** — o Volume antigo ainda existe e está legível. Conferido no run de TI-RADS:
`semantic_backend = "sentence_transformers"` em **2.711 de 2.711** laudos, zero fallback.

O risco é o dia em que `diamond_ia_hml` for limpo. Aí o modelo não carrega e a decisão híbrida
degrada para `token_overlap` — **sem erro, sem log, sem alerta**. A métrica cai e nada aponta a
causa. Não afeta o pulmão, que não usa embeddings.

Pedido: **copiar `st_models/` para o Volume novo** antes de qualquer limpeza do antigo. Ajustamos
as configs em seguida.

Do nosso lado, abrimos débito na lib para tornar esse fallback observável — hoje só dá para
detectar inspecionando `semantic_backend` dentro do blob, achado por achado.

---

## 8. O que funcionou sem ressalva

- **Credencial e `base_url` injetados pelo runner** — nenhuma ocorrência de `missing_api_key`, `missing_base_url_or_model`, `retry_exhausted` ou erro HTTP em 4.796 laudos processados nas duas especialidades.
- **Merge de `runtime.llm_router` sobre `nlp.llm_router`** (`config/ntb_ia_loader.py:106-109`) — é o que liga o juiz e o extrator quantitativo. Funcionou nas duas configs.
- **Wheel `0.7.1`** resolvida por `latest`, com resultado idêntico ao homologado.
