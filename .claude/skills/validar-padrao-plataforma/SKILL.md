---
name: validar-padrao-plataforma
description: Avalia e valida código da plataforma NLP (fabrica-ia-nlp-platform) contra o padrão do time — estrutura de notebook, carga por %run, classes, config, job, SQL/Spark, dados, testes e alçada. Use antes de commit, push ou PR na plataforma, ao criar linha nova ou módulo de tools/, e ao revisar PR de outro autor.
---

# Validar padrão da plataforma

A regra é `.claude/rules/09-plataforma-padrao-codigo.md`; a fonte de verdade são as specs, guias e POPs da plataforma.
Esta skill é a etapa que **prova** que o código segue a regra — em três camadas: o que é determinístico (script), o que
é padrão × desvio (comparar com a plataforma) e o que exige julgamento (checklist).

## 1. Checks determinísticos — o script

```bash
# arquivos alterados na branch contra a hml (padrão), na pasta do repositório da plataforma
python .claude/scripts/validar-padrao-plataforma.py --repo <pasta-do-repo>
# arquivos específicos
python .claude/scripts/validar-padrao-plataforma.py --repo fabrica-ia-nlp-platform jobs/definicoes/x-batch.json
```

- **ERRO bloqueia** commit/PR. **ALERTA** pede olhar e decisão registrada.
- Rodar `--autoteste` quando o script mudar: cada check precisa disparar no caso errado e calar no certo (regra 08 #10).
- ⚠️ **Zero achado não é aprovação** se a lista de arquivos veio vazia — conferir a linha `N arquivo(s)`.

## 2. Padrão × desvio — comparar com a plataforma

Para cada ALERTA ou dúvida sobre chave de config/job:

```bash
python .claude/scripts/e-desvio-ou-padrao.py <chave>          # configs
python .claude/scripts/e-desvio-ou-padrao.py <chave> --jobs   # definições de job
```

Se a maioria declara igual, **é padrão** — não cobrar do autor; vai para o card acumulador da plataforma. E abrir um
**módulo vizinho** do mesmo diretório antes de concluir: o código vizinho vence a memória.

## 3. Julgamento — checklist

| # | pergunta | onde conferir |
|---|---|---|
| 1 | O arquivo está no quadro de quem o edita? (config/job/navegação = DS; runner/tools = plataforma; cluster/ambientes/schema = Administrador) | POP-IA-08, regra 09 §9 |
| 2 | Muda contrato de saída ou da view? Se sim, é **demanda à plataforma**, não mudança local | spec 14/22 |
| 3 | Chave de config nova ou campo novo na saída? Exige alinhamento antes | memória `campo-novo-na-saida-exige-alinhamento` |
| 4 | Docstrings dizem o que o código faz (não o que deveria fazer)? Exemplos batem com a assinatura? | leitura |
| 5 | Teste de **recusa** além de aceitação? Mutação nos comportamentos novos, com aplicação afirmada? | regra 06, regra 08 #5 |
| 6 | Se promete não mudar resultado: **paridade** medida, com **controle positivo**? | regra 09 §8 |
| 7 | SQL: entrada só por literal escapado ou identificador validado; janela de datas fechada; sem `\b` no Spark? | regra 09 §6 |
| 8 | PII só na view; nada de laudo/CPF em teste, log, fixture ou artefato de depuração? | regra 09 §7, `gate-phi.py` |
| 9 | Job: `pause_status` obrigatório, versão literal, `disabled` no envio, `${env_motor}`? | spec 29/31 |
| 10 | Ruff limpo e pytest com exit code conferido **sem pipe**? | regra 08 #1 |

## 4. Relatório

Uma tabela por camada — **ERRO / ALERTA / ok** — com arquivo, linha, regra (§ da regra 09) e encaminhamento:
corrigir, registrar como desvio deliberado (no cabeçalho do arquivo), ou levar à plataforma (padrão divergente).

Nada sobe com ERRO aberto. Desvio deliberado só com motivo escrito **no arquivo**.

## Manter sincronizado

Quando a plataforma mudar (spec, guia, POP ou o código vizinho), **atualizar a regra 09 e o script no mesmo dia**,
rodar `--autoteste` e `python .claude/scripts/sync-cursor-rules.py`. Regra que diverge da plataforma é pior que não
ter regra: cobra o errado com autoridade.
