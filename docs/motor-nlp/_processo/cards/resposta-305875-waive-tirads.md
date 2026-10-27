# Resposta ao card 305875 — [NLP Engine] Avalizar a chave `waive` e os dois campos de saida para habilitar a config 0.9.0-tirads

> Rascunho de comentário, 26/10/2026. Responde às perguntas do comentário de detalhamento, na ordem.

**1. Em produção hoje (TI-RADS):** lib **`0.12.3`** (literal na definição do job `tirads-batch`) e config
**`0.8.0-tirads`** — conferido na tabela de saída de prd em 21, 22 e 26/10. A `0.12.3` **já contém** a capacidade
`waive`; ela fica inerte porque nenhuma config a declara.

**2. O que muda na 0.9.0-tirads:** só três coisas — `config_version` `0.8.0` → `0.9.0`, a chave `waive` nos **dois
critérios de tamanho** (nódulo e cisto, os que condicionam o TR4) e a linha de changelog no cabeçalho.

**3. De/Para**
```python
# 0.8.0-tirads (prd)                         # 0.9.0-tirads
'measure': {'name': 'nodulo_max', 'unit': 'cm'},   (igual)
'threshold': {'op': '>=', 'value': 1.0},           (igual)
'require_measure': True,                           (igual)
                                                   'waive': {'text': r'(paaf|pun[cç][aã]o|aspirativa)'},
'gates_ordinal_promotion': ['TR4'],                (igual)
# o mesmo no critério cisto_max
```

**4. Formato e comportamento**
- `waive: {'text': <regex>}` — aplicada ao texto tratado do laudo, sem diferenciar maiúscula/minúscula; ou
  `waive: {'finding': <chave de achado>}` — dispensa quando a régua encontrou aquele achado.
- **Ausente:** comportamento atual (a `0.8.0` contra a `0.12.3` deu **zero divergência** em 1.052 laudos).
- **Regex inválida:** a dispensa **não** é aplicada e o motivo vai em `gate_waived_error` — o gate segue normal.
- O padrão foi medido **sem `\b`** e com cedilha (`punção` em 29 laudos, `puncao` em zero); mudar o padrão invalida o A/B.

**5. Onde é interpretado:** na camada quantitativa, no gate do critério com `require_measure` que condiciona o TR4
(`gates_ordinal_promotion`), **antes** de o gate decidir: com evidência de punção, o critério não exige medida.

**6. Campos `gate_waived_by` e `gate_waived_error`:** **informativos**, dentro do JSON de `exm_laudo_resultado`
(bloco `quantitative.<criterio>`), e **condicionais** — só aparecem quando a dispensa foi aplicada ou falhou.
`gate_waived_by` traz o padrão que casou. Nenhum consumidor atual os lê; a view e os envios não os extraem.

**7. Schema:** **nenhuma** mudança na tabela de saída nem na view; só conteúdo do JSON.

**8. Quem usa:** o job `tirads-batch` (dev, hml, prd), runner `ntb_ia_motor_e2e`, config lida pelo `ConfigLoader`.
Nenhuma outra linha declara `waive`.

**9. O que o MLOps precisa fazer:** **nada de biblioteca** — a `0.12.3` já está publicada nos dois feeds e pinada em
prd. Só o fluxo normal de config: revisar o PR (um arquivo) → merge na `hml` → validar → promover à `main`.

**10. 🔴 O PR NÃO pode ser só aberto.** A branch `tirads/feature/waive-paaf` (`f8a16d7`) saiu de uma `main` antiga.
Comparada à `main` de hoje, ela **devolve o `embedding_model` para o caminho de Volume antigo**
(`/Volumes/diamond_ia_hml/...`) — o mesmo que deixou os embeddings sem funcionar em produção até 13/10 (card
`305810`). Antes do PR: **rebasear sobre a `hml` atual** e conferir que o diff final é só `config_version`, os dois
`waive` e o changelog.

**11. Validação em DEV/HML**
- run com a `0.9.0` sobre a mesma janela de um run com a `0.8.0` (referência: 02/07, 1.052 laudos; ou janela recente);
- esperado: **só promoções `0 → 1`, todas com `gate_waived_by`** e em laudo de punção (no A/B: 3 de 3), **zero
  rebaixamentos** e **zero `gate_waived_error`**;
- conferir a camada semântica com modelo real (`sentence_transformers`, não `token_overlap`) — pega a regressão do item 10;
- view e envio de navegação rodam sem erro.

**12. Critério objetivo de rollback** (reverter a config para `0.8.0-tirads`, um arquivo; a lib não muda):
- qualquer rebaixamento `1 → 0` atribuível à troca;
- promoção sem `gate_waived_by`;
- `gate_waived_error` presente;
- entregas TR4 acima do esperado — no A/B a dispensa mudou **3 em 1.052 laudos (~0,3%)**; mais de 1% de promoções
  novas pede revisão;
- camada semântica caindo para `token_overlap`.

**Aval pedido:** a chave `waive` (config) e os campos `gate_waived_by` / `gate_waived_error` (saída, informativos).
