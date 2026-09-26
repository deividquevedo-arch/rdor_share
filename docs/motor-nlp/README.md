---
tags: [indice, motor-nlp]
---

# Motor NLP — índice

> **Mapa de navegação.** Reescrito em **2026-09-25**, quando o índice de 13/07 já não conhecia
> metade das frentes. Os links usam a sintaxe de wiki do Obsidian, então abrem com um clique e aparecem no grafo.

---

## Comece por aqui

| | |
|---|---|
| **Onde cada coisa está agora** | [[ESTADO]] — documento vivo, carregado em toda sessão |
| **O que aconteceu e por quê** | [[2026-09-25\|diário mais recente]] · pasta `_processo/diario/` |
| **Como revisar um PR do time** | [[checklist-revisao-pr-ds]] |
| **Regras que valem sempre** | `.claude/rules/` na raiz — fonte canônica |
| **Fatos duráveis e armadilhas** | memória do projeto (`/memory`), índice em `MEMORY.md` |

🔴 **A divisão é dura:** *estado* vai no [[ESTADO]], *história* vai no diário, *fato durável* vai
na memória. Estado na memória apodrece e passa a enganar.

---

## Linhas de cuidado

| linha | situação | documento de entrada |
|---|---|---|
| **hepatologia** | em prd, trabalho consolidado | [[Relatorio-final-homologacao-hepatologia-v1]] · [[calibracao-hepatologia-camadas-v0]] |
| **tireoide / TI-RADS** | em prd | [[spec-negocio-tireoide-discovery-v1]] · [[relatorio-final-tirads-v1-2026-07-12]] · [[doc-spec-camada-criterios-quantitativos-v0]] |
| **câncer de estômago** | em prd | [[spec-negocio-cancer-estomago-v1]] · [[medicao-ganho-gold-filter-2026-09-17]] |
| **câncer de rim** | em prd | [[diagnostico-config-cancer-rim]] |
| **reumatologia** | em prd, **filtro e régua corrigidos, PR não aberto** | [[spec-migracao-reumatologia-v0]] · [[briefing-migracao-reumatologia-v0]] |
| **transplante de pulmão** | em prd, entrega zero | [[spec-negocio-transplante-pulmao-v1]] · [[checkpoint-pulmao-v1-2026-07-14]] |
| **DII** | em hml, PR 7383 mergeado | [[dii-metricas-legado-x-atual]] · [[motor-nlp/_processo/dii-direcionamento-pos-medicao\|direcionamento ao Leandro]] |
| **ateromatose · tumor ósseo · câncer de cólon** | **em hml, paradas** | ver [[ESTADO]] |
| **neuroimunologia** | PR 7428 aprovado | ver [[ESTADO]] |
| **próstata (PI-RADS)** | 🟡 SPEC escrita, não iniciada | [[spec-migracao-prostata-pirads-v0]] |
| **doenças biliares** | 🟡 SPEC escrita, não iniciada | [[spec-migracao-doencas-biliares-v0]] · [[migracao-biliares-e-neuro-inventario]] |

---

## Biblioteca — `nlp_engine`

- [[doc-arquitetura-config-rads-v0]] · [[doc-regras-clinicas-rads-v0]] — extração ordinal xxRADS
- [[doc-spec-camada-criterios-quantitativos-v0]] — critérios quantitativos e gates
- [[proposta-arquitetura-alvo-v0]] — arquitetura alvo
- [[mapa-gaps-lib-plataforma-2026-08-21]] — os 15 gaps entre lib e plataforma
- As SPECs de cada versão vivem em `nlp-engine-lib/docs/`, **não** aqui.

### Medições que sustentam as versões correntes

[[validacao-0.13.0-em-ambiente-2026-09-21]] · [[validacao-0.14.0-em-ambiente-2026-09-21]] ·
[[medicao-ca5-p0-29-coorte-dirigida-2026-09-21]] · [[medicao-ca4-tokens-camada-quantitativa-2026-09-21]] ·
[[investigacao-vinculo-lesao-medida-2026-09-23]] · [[golden-0.15.0-delta-contra-v0.14.0-2026-09-22]]

---

## Plataforma, Ops e governança

- [[alinhamento-ops-2026-09-10]] — 19 itens em 7 temas, com evidência
- [[pauta-minima-ops]] — só o que está aberto e depende do Ops
- [[alinhamento-configuracao-nlp-2026-09-15]] — o bloco `runtime` e o contrato
- [[pedido-grant-mlops-fabrica-ia]] — o grant que destravou a camada semântica
- [[indice-de-cards]] — número → título, obrigatório para citar card

---

## Repositório clínico (gold) e filtros de entrada

- [[medicao-endoscopia-colonoscopia-repositorio-2026-09-17]] — o repositório em 12 meses
- [[medicao-ganho-gold-filter-2026-09-17]] — custo e ganho de ampliar filtro, nos dois sentidos

⚠️ **Filtro de entrada é invisível à paridade.** `match_rate` só mede o que chega ao motor.

---

## Estrutura das pastas

| pasta | conteúdo |
|---|---|
| `_fundacao/` | discovery, diretrizes, anexos, arquitetura, contratos — a base, pouco volátil |
| `_processo/` | medições, auditorias, pareceres, atas e o **diário por dia** |
| `_versoes-estaveis/` | cópias de config fora do repo da plataforma, com [[PONTEIRO-versoes-estaveis]] |
| `checklists/` | checklists executáveis |
| `rads/`, `sprints/` | extração ordinal e evidências de sprint |
| uma pasta por linha | `hepatologia/`, `tireoide/`, `reumatologia/`, `prostata/`, … |

---

## Convenções

- **Card sempre com número E título** — `283644` sozinho não identifica nada. Índice em
  [[indice-de-cards]].
- **Uma pasta por linha de cuidado**, com o nome do `specialty_id`.
- **Documento de medição leva a data no nome** (`...-AAAA-MM-DD.md`) — é registro, não documento vivo.
- **Sem PHI no repositório.** Texto de laudo e identificador de paciente ficam fora do git, em
  `Desktop/Rede D'Or/`.
