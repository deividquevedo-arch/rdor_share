# Pauta mínima — agenda com o time de Ops

> **O que é.** Só o que está **em aberto** e **depende do time de Ops**. Cada item traz a evidência
> medida em uma linha e o que se pede. O histórico e o que já foi resolvido ficam de fora.
>
> **13/09/2026** · 9 itens · sugestão de 45 minutos.

---

## Antes de tudo — os POPs

🟢 **Em resolução.** Os dez POPs da Fábrica cobrem boa parte do que levantamos: fluxo da biblioteca,
esteira, catálogos e schemas, controle de versões, e a fronteira de alçada da NLP Platform.

⚠️ **Só falta o passo final:** os dez estão em **versão 1.0**, com `Vigência: a definir na aprovação`
e `Aprovado por: a definir`. O POP-IA-09 declara que *"nenhum elo da cadeia está em uso hoje"*.

**Pedido único:** aprovar e datar. Vários itens abaixo mudam de natureza — ou somem — no instante em
que os POPs passam a valer.

---

# 1. Bloqueiam trabalho hoje

## 1.1 🔴 Dev não instala versão que só existe na `hml`

**Evidência.** O cluster de dev resolve o `pip` contra `fabrica-ai`, que é o feed de **produção**.
Erro de 09/09: `Could not find a version that satisfies nlp-engine==0.12.2 (from versions: 0.9.4,
0.10.0, 0.10.1, 0.11.2)`.

✅ **A wheel existe.** O volume de HML tem `0.12.1`, `0.12.2` e `0.12.3`. **O que parou foi o
consumo:** o widget `nlp_engine_volume` foi removido e não há índice extra declarado.

**Consequência.** Só se valida em dev o que **já está em produção**. A branch `hml` da lib perde a
função, e a escolha vira: promover sem validar, ou não validar.

**Pedido.** Restaurar a leitura do volume de HML **ou** declarar `fabrica-ai-hml` como índice extra
do `pip` no cluster de dev — e só de dev.

---

## 1.2 🔴 Pin da versão por linha — história `301938`

**Evidência.** `jobs/ambientes/{prod,hml}.json` declaram `"latest"` nas branches `main` e `hml`. A
versão em produção mudou **quatro vezes em nove dias**: `0.9.4` → `0.10.0` → `0.10.1` → `0.11.2` →
`0.12.3`, sem ninguém tocar no job. Verificado no `engine_version` das tabelas de saída.

**Pedido.** Aplicar o pin nas linhas ativas. A lista está na história `301938`.

❓ **Pergunta que pode encurtar o caminho.** A Figura 2 do POP-IA-08 coloca
`jobs/definicoes/<job>.json` no quadro *"você edita"*, e é lá que a versão é declarada por linha —
hoje como `"${nlp_engine_version}"`, resolvido por `jobs/ambientes/`. **Trocar a variável por um
literal fixa a linha sem tocar na infraestrutura.** Isso é aceitável?

---

## 1.3 🔴 Campos novos no contrato de saída — trava um PR de config

**Evidência.** A `0.12.3` da lib emite `gate_waived_by` e `gate_waived_error` quando a config
declara a chave `waive`. A config `0.9.0-tirads` está pronta e **o PR não foi aberto**, aguardando
este aval. Medido: 3 laudos promovidos, 0 rebaixados, 28 dispensas aplicadas.

Entra junto a contabilidade de tokens da extração quantitativa — hoje **221 das 270 chamadas diárias
ao LLM não registram token nenhum**, porque só o caminho do juiz grava.

**Pedido.** Aval dos campos novos, de uma vez, para não abrir uma terceira rodada de mudança de
contrato.

---

# 2. Defeitos abertos, sem correção

## 2.1 🔴 Os embeddings não funcionam em produção

**Evidência**, medida em 10/09 — as três linhas que declaram embeddings rodam em `token_overlap`:

| linha | laudos | com `FileNotFoundError` |
|---|---|---|
| hepatologia | 5.172 | **5.108 (98,8%)** |
| cancer_estomago | 201 | **201 (100%)** |
| tirads | 1.568 | **1.348 (86,0%)** |

**Causa.** `embedding_model` aponta para `/Volumes/diamond_ia_hml/…`, o volume do workspace antigo,
em caminho literal idêntico nos três ambientes. Em produção ele não existe.

**Consequência.** A config declara `decision_mode: hybrid`; o que executa é régua mais sobreposição
de tokens. **Produção roda um perfil que nunca foi homologado.**

**Pedido.** Provisionar o schema `nlp_engine` em `gold_fabrica_ia` — hoje só tem `fhir` e
`information_schema`. A resolução do caminho por ambiente é nossa (card `298600`).

---

## 2.2 🔴 A monitoria não registra nada de LLM

**Evidência.** As colunas são `total`, `relevantes`, `relevance_rate`, `confidence_*` e
`exames_distintos`. No TI-RADS a taxa ficou em **3,17% → 3,21% enquanto 4.703 chamadas ao LLM
falhavam** — a régua sustenta o número e o alerta não dispara.

**Consequência.** Falha de LLM não gera sinal. Foi assim que o 403 passou despercebido por semanas.

**Pedido.** Acrescentar chamadas, erros e tokens, por linha e por dia.

---

## 2.3 🟡 O filtro da view de exportação decide entrega e é invisível

**Evidência.** `load_validation_rules()` lê `CONFIG_NAV['validacao']` e filtra a view. São **18
arquivos** — 6 especialidades × 3 ambientes — com regra de escopo e **regra clínica**: hepatologia
limitada a `RJ/SP/BA`, transplante de pulmão a 6–75 anos, quatro linhas com lista branca de unidade.

🔴 **Falha aberto:** arquivo ausente → registra um aviso → devolve `{}` → **view sem filtro nenhum**,
em silêncio.

**Consequência.** `fl_relevante = 1` não é o que o negócio recebe, e um critério clínico vive fora da
config clínica, sem revisão clínica.

**Pedido.** Tornar o filtro explícito no contrato de saída, decidir onde regra clínica deve morar, e
falhar fechado quando o arquivo faltar.

---

## 2.4 🟡 A entrada recebe o documento RTF cru

**Evidência.** `exm_laudo_texto` vem de `laudo_original`. Em 4.321 laudos do TI-RADS, **116 (2,7%)
são o RTF inteiro** e ocupam **63,4 dos 68,6 MB do dia**; o maior tem 815 KB. O mesmo struct traz
`laudo_transformado`, com o texto limpo — vazio em 158 dos 4.321.

**Pedido.** Avaliar `coalesce(laudo_transformado, laudo_original)`, com custo medido antes.

---

# 3. Processo

## 3.1 🟡 Comunicação de mudança que atravessa a fronteira

🟢 **Em resolução pelo POP-IA-08.** Ele já estabelece que *"o alinhamento não é uma etapa da revisão
de Pull Request — é anterior a ela"*, e a Figura 2 define os três quadros de alçada.

⚠️ **O gate está escrito numa direção só** — a de quem calibra. A matriz diz a quem o Cientista de
Dados recorre antes de tocar cada quadro, e **não diz a quem o dono de um quadro recorre antes de
alterá-lo**.

**O que se observou.** Duas mudanças no caminho de instalação foram para produção em 03/09 e 08/09 e
**foram identificadas por tentativa em 09/09**, não por comunicação prévia. O efeito é o item 1.1.

**Pedido.** Acrescentar a coluna inversa ao POP-IA-08 §5: mudança em caminho de instalação, policy
de cluster, arquivo de ambiente ou contrato é alinhada antes do merge, com o mesmo registro que o
POP já exige na outra direção.

---

## 3.2 🟡 Validação de config na esteira

**Evidência.** Não existe etapa de validação de config no CI. Nos 48 `.py` da plataforma o único
sinal de validação é Pydantic nos *loaders*, em tempo de execução. Nenhum PR é barrado por config
inválida.

**Pergunta.** Está previsto um passo de validação? Em que estágio, e o que ele checa? Do nosso lado
isso define o que precisamos cobrir antes de abrir PR.

---

# Anexo — o que já está documentado e segue sem correção

Não são pedidos novos; são itens que o próprio time já registrou e que continuam ativos.

| # | consta em | situação |
|---|---|---|
| bug 5 | POP-IA-08 §13 — *"janela de datas em hml/prd usa UTC; evite agendar 21h–00h"* | contorno operacional, sem correção |
| bug 2 | POP-IA-08 §13 — *"dedup aponta fixo para hepatologia/dev; a saída duplica"* | ⚠️ pode ser a causa do card `300201`; estamos cruzando |
| `298596` | `limit_rows` não isola coorte — o teto corta depois da união da fila | card aberto, em execução |

---

# O que pedimos, em uma lista

1. **Aprovar e datar os POPs.**
2. **Religar o consumo da lib em dev** — volume de HML ou índice extra.
3. **Aplicar o pin por linha** — e responder se a definição do job serve.
4. **Avalizar os campos novos do contrato** — `waive` e tokens.
5. **Provisionar o schema `nlp_engine`** em `gold_fabrica_ia`.
6. **Acrescentar métricas de LLM à monitoria.**
7. **Tornar o filtro da view explícito** e falhar fechado.
8. **Avaliar o `coalesce`** na origem do laudo.
9. **Estender o gate do POP-IA-08** para a direção inversa.

---

# Do nosso lado, já resolvido

Para a agenda não gastar tempo com isto:

- ✅ `0.12.2` e `0.12.3` publicadas nos dois feeds, validadas em ambiente.
- ✅ O defeito da esteira que pulava a publicação em produção — corrigido.
- ✅ O LLM voltou a funcionar em produção: 270 chamadas em 10/09, zero erro.
- ✅ Reumatologia em produção, legado desligado.
- ✅ `gold_filter` da punção: confirmado como nossa alçada pela Figura 2 do POP-IA-08 — segue por PR
  de config, sem depender de ninguém.
