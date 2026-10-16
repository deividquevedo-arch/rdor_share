---
tags: [randomizacao, apresentacao, pendente]
---

# Cruzamento — *[IA] Randomização — Apresentação do Projeto v2*

> 15/10/2026. Apresentação em refinamento pelo responsável de negócio; **retomar depois**, sem
> ação agora. Fonte local: `.alt.doc/[IA] Randomização — Apresentação do Projeto v2.pdf` (fora do git).

## Divergências de fato

| slide | afirma | medido / decidido | sugestão |
|---|---|---|---|
| 2, 4, 5, 7 | onda 1 em homologação, produção em 29/10 | prd randomizado desde 12/10; hml desde 30/09 (reumato) e 07/10 (TI-RADS); decisão de 15/10: onda 1 já virou | onda 1 em prd desde 12/10 a 5%; 23/10 formaliza o de acordo |
| 2, 3 | controle de 10% (90/10) | config vigente 5%; o slide 7 lista 5% ou 10% como pendência | usar 5% como vigente |
| 3 | intervenção segue para a Central de Captação | em prd o envio sai por OneDrive; task `api_` fora dos jobs desde 09/10 | "segue para a navegação" |
| 5 | onda 2: relatório 05/11, homologação até 06/11 | — | relatório em 06 ou 09/11, ou janela até 05/11 |

## Pontos de método

1. Uplift **por linha** → por **onda agregada**; por linha só descritivo (neuro e próstata terão 2–3 controles em 10 dias a 5%).
2. "Impacto esperado ↑15%" → dar o horizonte: ~14% detectável com 10% nas cinco linhas em ~1 ano; com 5%, ~2× o tempo.
3. KPI de qualidade, "desvio a definir" → percentual real da onda com IC 95% contendo o alvo e |z| ≤ 3 (onda 1 em 15/10: 4,5%, IC 2,4–8,3%).
4. Etapas do uplift → acrescentar intenção de tratar, ajuste pelo consumo pré, contaminação, corte por unidade com navegadora, chave `num_cpf_paciente_d` (85,5% de casamento).
5. Incluir **monitoria e observabilidade** em "Depois da produção".
6. Dependência da onda 2: jobs sem task de randomização; jobs pausados em hml.

## Coerente

Arquitetura (base apartada de ~1 bi de CPFs, sorteio depois do modelo e antes da navegação);
adoção "12 de 16"; papéis por etapa (refletidos no [[metodo-avaliacao-por-onda-v0]]).
