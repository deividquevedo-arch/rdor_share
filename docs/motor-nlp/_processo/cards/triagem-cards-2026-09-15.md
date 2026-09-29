# Triagem dos cards abertos — 15/09/2026

> **38 cards abertos**, lidos do board (descrição e critério de aceite), não da nossa anotação.
>
> A coluna **critério de aceite** traz o do board quando existe; onde o card está **sem CA**, vem uma
> proposta marcada como *(proposto)* — é o que fecharia o card, e precisa ser escrito nele.
>
> Escopo a partir de 15/09: as **migrações saem da nossa fila** (Lucas e Leandro); aqui fica o fecho
> do ciclo de bumps da lib e as implementações mapeadas.

---

# A. Parados por Ops ou por alinhamento — **atacar primeiro**

| card | título | motivo / necessidade | o que trava | critério de aceite |
|---|---|---|---|---|
| `283647` | [NLP Engine] Declarar contrato de entrada e saída entre a lib e a plataforma | Consenso das duas agendas de 21/08: o problema entre os times é **contrato e comunicação**, não arquitetura. Hoje a plataforma supõe implicitamente o que o motor devolve | **Aval de três mudanças de contrato**: `waive` + `gate_waived_by`/`gate_waived_error`; tokens na extração quantitativa; `applies_to_exam_type` em findings | *(proposto)* Documento de contrato publicado e **os três itens avalizados numa rodada só**; PR `7228` mergeado; nenhuma chave nova entra na lib sem passar por aqui |
| `298600` | [NLP Engine] embedding_model aponta para volume de HML do workspace antigo nos três ambientes | Caminho **literal e idêntico nos 3 ambientes** para o volume do workspace antigo. Produção roda perfil não homologado: 98,8% · 100% · 86,0% em `token_overlap` | **Schema `nlp_engine` não existe em `gold_fabrica_ia`** — criar schema não é nossa alçada | *(proposto)* Caminho resolvido **por ambiente** (classe do `base_url`, PR 7135) **e** zero `FileNotFoundError` num run de produção das 3 linhas |
| `283646` | [Fabrica IA] Validação de compliance/DPO do envio de laudo clínico ao LLM | Três especialidades enviam excerto de laudo ao LLM; **duas já em produção**. A SPEC 27 §7 exige validação antes de habilitar em prd, e não há anonimização no caminho | **15 dias parado, sem canal nem dono definido** — não é decisão técnica | *(proposto)* Parecer registrado de compliance/DPO, com a decisão sobre anonimização; ou, se negado, o plano de anonimização especificado |

---

# B. Mudou de dono

| card | título | motivo / necessidade | o que trava | critério de aceite |
|---|---|---|---|---|
| `303791` | Plano de Migração algoritmos final | Todos os algoritmos rodando no NLP Engine. Em andamento: ateromatose (Lucas), DII (Leandro); ca-cólon com PR aberto | **Atribuído a nós e não é mais nossa frente** | *(proposto)* Reatribuir a quem executa; manter o card como o guarda-chuva das migrações, com uma linha por algoritmo e o estado de cada uma |

---

# C. Entregue ou desatualizado no board

| card | título | motivo / necessidade | o que trava | critério de aceite |
|---|---|---|---|---|
| `300200` | [NLP Engine] Âncora ausente sai do gate: TR4 entregue sem conferir o tamanho | Defeito P1 ativo em produção: 36 de 1.032 entregas como `TR4` pelado | **Nada — está em *Desenvolvido* e a `0.12.2` foi entregue, tagueada e publicada** | A/B fechado: 4 rebaixados `1→0`, zero promovidos, 4 de 4 com `require_measure_no_anchor`. **Atendido** — falta mover |
| `285305` | [NLP Engine] TI-RADS entrega TR falso: legenda ACR não filtrada e medida associada ao nódulo errado | **Dois defeitos num card só.** O 1 (legenda ACR) foi corrigido na `0.10.1` e medido: 156 → 133 entregas, 23 removidas, zero acrescentadas. O 2 (medida do nódulo errado) é a `0.15.0` | **Card com dois escopos não fecha** — o aceite do 1 não fecha o 2 | *(proposto)* **Partir em dois.** O atual fecha com a evidência do defeito 1; abre-se um novo para o vínculo lesão↔medida, alocado na `0.15.0` |
| `302198` | [Tireoide] Adicionar novos campos nos envios | Navegação RDSL quer `Convênio`, `Plano`, `Médico Solicitante`, `CRM` e `UF CRM` para agilizar a avaliação de elegibilidade | **Nada nosso** — PR aberto, com o time de Ops | Envios de Tireoide com os cinco campos. **Validado em 15/09**, 55 registros; aguarda o merge |

---

# D. Trabalho nosso, em curso

| card | título | motivo / necessidade | o que trava | critério de aceite |
|---|---|---|---|---|
| `283648` | [P0-29] Impedir que o juiz LLM promova sem evidência de regra | **Quebra de invariante da arquitetura.** Nada impede o juiz de virar `fl 0 → 1` num laudo sem nenhum span positivo | **Exige medição por linha antes de subir** — só aumenta ou reduz entrega, e sem medir não se arbitra | *(proposto)* Promoção sem span positivo impossível por construção, com teste que mata o mutante; **impacto medido por linha** em janela com a população |
| `283644` | [NLP Engine] Juiz LLM ligado por contorno não documentado em hepatologia e transplante_pulmao | Duas linhas **em produção** com o juiz ligado por contorno: `nlp.llm_router` não declara `enabled`, e a lib assume `False` na ausência | **P1 aberto há ~23 dias sem medição registrada** | *(proposto)* `enabled` declarado explicitamente nas duas configs **e** medição do delta de decisão ao tornar explícito |
| `298598` | [NLP Engine] Plano de bumps do backlog técnico — 0.11.1 a 0.15.0 | Consolidar os 28 cards `[P0-01]`–`[P3-28]` em versões, com critério de agrupamento explícito, para a fila ser previsível | **Card de acompanhamento** — fecha quando o ciclo fechar | *(proposto)* `0.13.0` a `0.15.0` entregues e tagueadas, com os cards do backlog movidos |
| `280008` | [Motor NLP] Estudo e planejamento — contexto do paciente na decisão | Entender se e como o motor deve avaliar no escopo do **paciente**, não só do laudo isolado | **Falta a SPEC da fase 0** — exclusão/refutação no escopo do laudo. Não depende das 6 decisões em aberto | Viabilidade medida sobre execução real *(concluído)*; componentes e fronteiras definidos com dono. **Falta a SPEC da fase 0** |

---

# E. Higiene da lib — 21 cards

Fecham com o ciclo de bumps. **Nenhum está parado por terceiro.**

## E1. Entregues, aguardando avaliação (9)

`253573` [P2-07] extra `databricks` vazio · `253575` [P2-09] TypedDict no caminho principal · `253576` [P2-10] hierarquia de exceções · `253577` [P2-11] `except Exception` silencioso · `253578` [P2-12] regras do ruff e mypy · `253583` [P2-17] SQL por f-string · `253585` [P2-19] `__all__` explícito · `253588` [P2-22] cobertura com gate de PR · `300200` (ver bloco C)

**O que trava:** nada nosso — estão em *Desenvolvido*, aguardando o time avaliar.
**Critério de aceite:** o de cada card, já comentado com evidência medida contra a árvore mergeada.

## E2. 🔴 CA comprovadamente **não atendido** (3)

| card | título | o que falta, medido na `origin/hml` |
|---|---|---|
| `253574` | [P2-08] Uniformizar a convenção de tipos de config e eliminar `type: ignore` | CA4 exige **zero** `type: ignore` em `src/`; há **6** |
| `253586` | [P2-20] Popular `tests/conftest.py` com fixtures compartilhadas | CA2 exige zero builders locais nos testes; há **10** |
| `253594` | [P3-28] Adotar hooks de pre-commit espelhando o `make check` | CA1 exige `rev:` fixado; **nenhum** encontrado |

**Estes exigem trabalho, não movimentação.**

## E3. Candidatos a fechar — falta passe completo de CA (4)

`253587` [P2-21] `parametrize` · `253589` [P2-23] referência de API · `253590` [P2-24] `CONTRIBUTING.md` e política de versionamento · `253592` [P3-26] documentação humanos × agentes

**O que trava:** conferi **um CA de cada** e passou; os demais critérios não foram verificados.
**Critério de aceite:** passe completo, e mover o que passar. ~30 minutos.

## E4. `0.13.0` — em aberto (5)

| card | título | o que trava | critério de aceite |
|---|---|---|---|
| `253579` | [P2-13] Consolidar o singleton do spaCy e torná-lo thread-safe | **Feito na branch `feat/0.13.0-estrutura`, não mergeado** | double-checked locking + publicação atômica; 5 mutantes mortos. **Atendido, falta merge** |
| `253581` | [P2-15] Dividir `rads_extraction.py` por responsabilidade | idem | CA5 provado: 300 laudos, zero divergentes. ⚠️ **CA2 não fecha ao pé da letra** — pede nenhum arquivo > 300 linhas e dez excedem |
| `253580` | [P2-14] Quebrar `ClinicalNlpEngine.process()` em métodos coesos | não iniciado — **sem** streaming, que vira bump próprio | *(proposto)* `process()` decomposto, saída byte-idêntica em corpus de referência |
| `253582` | [P2-16] Eliminar duplicação de helpers utilitários entre módulos | não iniciado | *(proposto)* helpers num módulo só, sem alterar valor; se alterar, sai para release própria |
| `253591` | [P3-25] Avaliar e (se aprovado) achatar a estrutura de pacote duplamente aninhada | ⚠️ **não é mandato de achatar** — etapa 1 é investigação, e "manter, documentado" é entrega válida | CA1 ADR com origem, razão de design e custo; CA2 decisão aprovada. **Recomendação: manter** — o pacote de topo abriga `monitoring/`, que é uma das 3 libs da arquitetura |
| `253593` | [P3-27] Avaliar Protocol/dataclass nas fronteiras de API pública | mesma natureza — avaliação | *(proposto)* ADR com a decisão; se for adotar, verificar se muda assinatura pública |

---

# F. Herdados — decidir o destino (6)

Nenhum tocado há 13 a 26 dias. **Não afirmo o destino sem ler com quem abriu.**

| card | título | motivo / necessidade | o que trava | critério de aceite |
|---|---|---|---|---|
| `171773` | NLP Engine V0 pronto | **sem descrição e sem CA** | não dá para saber o que fecha | *(proposto)* encerrar — a V0 foi superada pelos bumps `0.9.x`–`0.12.x` |
| `174859` | Documentação para utilizar NLP Engine V1 | Card de 27/05 sem descrição; o escopo atual é leitura de quem assumiu | escopo não confirmado por quem abriu | *(proposto)* confirmar o escopo, ou absorver no `283647`, que já é o contrato para o consumidor |
| `179295` | Fábrica IA V1 — Catálogo e Framework de Dados Homologados | Dados distribuídos em fontes, formatos e níveis de qualidade diferentes | **É a dívida da base ouro**, cujo destino foi decidido em 02/09: o **lake**, não o repositório | Dados homologados carregados automaticamente · catálogo acessível · metadados documentados · histórico de versões · rastreabilidade da origem |
| `180226` | Fábrica IA V1 | Validar escalabilidade da arquitetura implementando algoritmos no padrão criado | *Aguardando Produção* — **card guarda-chuva** de uma fase já ultrapassada | Funções RADS implementadas · reuso comprovado · tempo de construção reduzido · assertividade > 90% |
| `184596` | [NLP Engine][Fígado] Levar o hepato para o fluxo de exceção | Monitorar 3 dias 1:1, validar relatórios, desligar o legado e passar os envios | *Aguardando Produção* há 26 dias — **a hepatologia já está em produção na plataforma nova** | Validação com stakeholder para subida em prd · ajuste da esteira MLOps. *(proposto: verificar se já foi atendido de fato e encerrar)* |
| `211273` | [Fabrica IA] Fluxo de documentação de uma linha de cuidado | Encadeamento acordado de documentos entre o pedido do negócio e a entrega, para rastreabilidade | **Piloto do Caminho A não executado** | Cenário 1: com especialidade de legado confiável, o fluxo Motor Legado é concluído e as divergências registradas |

---

# G. 🔴 Pendências de Ops **sem card** — criar

Vivem só na pauta mínima. **Enquanto forem item de pauta, dependem de uma reunião acontecer; como card, entram no backlog do Ops com dono e fila.**

| # | título proposto | motivo / necessidade | critério de aceite proposto |
|---|---|---|---|
| 1 | [Plataforma NLP] Declarar `fabrica-ai-hml` como índice extra do `pip` no cluster de dev | Dev resolve o `pip` contra o feed de **produção**. Erro de 09/09: `Could not find a version that satisfies nlp-engine==0.12.2 (from versions: 0.9.4, 0.10.0, 0.10.1, 0.11.2)`. Só se valida em dev o que já está em produção | Um cluster de dev instala versão que existe apenas no feed da `hml`, comprovado por run |
| 2 | [Plataforma NLP] Monitoria sem nenhuma métrica de LLM | As colunas são `total`, `relevantes`, `relevance_rate`, `confidence_*`, `exames_distintos`. No TI-RADS a taxa ficou 3,17% → 3,21% **enquanto 4.703 chamadas falhavam** | Chamadas, erros e tokens por linha e por dia na tabela de monitoria, com alerta que dispare em falha de LLM |
| 3 | [Plataforma NLP] `load_validation_rules` falha aberto e a view sai sem filtro | Arquivo ausente → aviso → `{}` → **view sem filtro nenhum**, em silêncio. São 18 arquivos com regra de escopo **e regra clínica** | Arquivo ausente **falha fechado**; o filtro aplicado fica explícito no contrato de saída |
| 4 | [Plataforma NLP] Entrada recebe o documento RTF cru | `exm_laudo_texto` vem de `laudo_original`. No TI-RADS, **116 de 4.321 laudos (2,7%) são o RTF inteiro e ocupam 63,4 dos 68,6 MB do dia** | `coalesce(laudo_transformado, laudo_original)` avaliado com custo medido, e a decisão registrada |
| 5 | [Plataforma NLP] Provisionar o schema `nlp_engine` em `gold_fabrica_ia` | Destino do modelo de embeddings em produção. O catálogo tem apenas `fhir` e `information_schema` | Schema criado com grants; o `298600` passa a poder fechar |
| 6 | [Plataforma NLP] A view do TI-RADS não decifra PII | A view da reumatologia aplica `rdsl_decrypt` e entrega em claro (0 de 173 cifrados); a do TI-RADS entrega **89 de 89 em base64**, em prd e em hml | As cinco colunas de PII em claro na view do TI-RADS, como nas demais linhas |
| 7 | [Plataforma NLP] Nome de arquivo particionado não carrega hora | `{now:%Y_%m_%d}` no ramo particionado contra `%Y%m%d_%H%M%S` no ramo sem partição — re-run no mesmo dia colide no mesmo caminho | Nome único por execução em **todas** as linhas, ou o comportamento na colisão documentado |

⚠️ **O item 7 é de ferramenta compartilhada** (`tools/data_exchange`) — mexer altera o nome do arquivo de **todas** as linhas. Não entra de carona em PR de coluna.

---

# Resumo

| bloco | cards | ação |
|---|---|---|
| **A** — parados por Ops / alinhamento | 3 | escrever o CA que falta e destravar |
| **B** — mudou de dono | 1 | reatribuir |
| **C** — entregue ou desatualizado | 3 | mover · partir o `285305` |
| **D** — trabalho nosso em curso | 4 | executar |
| **E** — higiene da lib | 21 | 9 aguardam · 3 exigem trabalho · 4 passe de CA · 5 são a `0.13.0` |
| **F** — herdados | 6 | decidir com quem abriu |
| **G** — Ops sem card | 7 **a criar** | direcionar ao backlog do Ops |

🔴 **Dez dos 38 estão sem critério de aceite escrito** — `283644`, `283646`, `283647`, `283648`,
`285305`, `298600`, `300200`, `171773`, `174859` e (vazio, só um ponto) `298598` e `303791`.
**Card sem CA não fecha**, e é a raiz de boa parte da bagunça.
