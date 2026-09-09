# SPEC — a entrada do motor recebe o laudo em RTF cru

> Fase **Plan** do SDD. Não há implementação associada: a mudança é do pipeline da plataforma, e
> MLOps/infra não se implementa sem decisão do time. O entregável é a medição e o desenho.
>
> Medido em **2026-09-09**, catálogo `diamond_fabrica_ia`, janela **07–09/09** (3 dias), quatro
> linhas ativas. Perfil `nlp-platform`.

---

## 1. O defeito

O campo obrigatório `exm_laudo_texto` recebe, para uma fatia dos exames, **o documento RTF
inteiro** em vez do texto do laudo. O conteúdo começa em `{\rtf1\ansi\ansicpg1252`, vem numa
**única linha**, e o maior exemplar da janela tem **814.685 caracteres** — 482 mil dígitos contra
389 espaços, o que caracteriza payload hexadecimal de imagem embutida, não texto.

| linha | laudos | em RTF | % | maior laudo |
|---|---|---|---|---|
| tirads | 4.163 | **116** | 2,8% | 815 KB |
| hepatologia | 5.374 | **231** | 4,3% | 1.039 KB |
| **cancer_estomago** | 753 | **105** | **13,9%** | 33 KB |
| transplante_pulmao | 265 | **25** | 9,4% | 512 KB |

**São 477 laudos por período de 3 dias, nas quatro linhas.**

### 1.1 O que isso custa hoje — três consequências medidas

**Volume.** O pipeline carrega marcação que não é texto clínico:

| linha | lido hoje (3 dias) | lido com o texto extraído | redução |
|---|---|---|---|
| tirads | 65,6 MB | 5,2 MB | −92% |
| hepatologia | 95,3 MB | 8,1 MB | −91% |
| cancer_estomago | 1,2 MB | 0,9 MB | −25% |
| transplante_pulmao | 12,2 MB | 7,0 MB | −43% |
| **total** | **174,3 MB** | **21,2 MB** | **−88%** |

≈ **51 MB por dia** de marcação atravessando todas as etapas. Em tirads, **92% do volume de texto
do dia sai de 2,8% dos registros**.

**Robustez.** Qualquer leitura em lote da coluna estoura o teto de 25 MB por resposta
(`Inline byte limit exceeded`). Como os exemplares grandes têm `id_exame` sequencial, um lote de
tamanho fixo falha sempre no mesmo ponto, independentemente do tamanho escolhido. Duas medições do
A/B da `0.12.2` foram interrompidas por isso, e a leitura só fechou com lote que se parte pela
metade em caso de estouro.

**Degradação da decisão — o número que mais pesa.** O tratamento (`to_plain`) remove a marcação,
mas o que sobra é incompleto. Presença do termo-âncora no texto que o motor efetivamente avalia,
contra o texto extraído disponível na própria Gold, restrito aos laudos em RTF:

| linha | termo | chega ao motor hoje | existe no texto extraído |
|---|---|---|---|
| tirads | `nodul` | **6** de 116 | **53** de 116 |
| hepatologia | `nodul` | 12 de 231 | **147** de 231 |
| cancer_estomago | `ulcer` | 6 de 105 | 9 de 105 |
| cancer_estomago | `tumor` | 5 de 105 | 5 de 105 |

O motor decide sobre texto empobrecido em 477 laudos por 3 dias. Não é hipótese: o texto íntegro
está gravado na mesma linha da Gold.

---

## 2. A causa — a coluna escolhida, e por que foi escolhida

`exm_laudo_texto` resolve por lista de candidatos, `['proced_laudo_exame_original',
'proced_laudo_exame']`. O primeiro **não existe na Gold**: é derivado pelo próprio pipeline, em
`nlp_ia_02_input.py`, passo 3 do fluxo descrito na spec 17:

```python
df_gold = df_gold.withColumn(
    "proced_laudo_exame_original",
    F.array_join(F.transform("proced_lista_exames", lambda x: x["laudo_original"]), "\n"),
)
```

O domínio `exame.laudos` traz apenas `id_exame` e o array `proced_lista_exames`. **Cada struct
desse array carrega os dois campos** — `laudo_original` e `laudo_transformado`. A escolha do
`laudo_original` acontece nessa linha do pipeline, não no registry de domínios.

⚠️ **A escolha é deliberada e está documentada como obrigatória.** O guia
`boas-praticas/04-selecao-de-dados-na-gold.md` §5.2 registra em vermelho: *"`exm_laudo_texto`
sempre com `proced_laudo_exame_original` como primeiro candidato"*, e o checklist do guia 09 tem
item próprio para isso. A spec 12 §6 repete a orientação. **Não há, em nenhum dos três documentos,
justificativa registrada para a preferência** — e nenhum deles menciona a existência do
`laudo_transformado`.

ℹ️ O mesmo repositório usa o campo oposto noutro contexto: o registry de tabelas de histórico
(`ntb_ia_data_manager.py`) expõe, no domínio `exames_historico.laudo`, exatamente
`laudo_transformado`. A convenção da plataforma, portanto, **não é uniforme**: o histórico do
paciente lê o texto extraído, o pipeline do motor lê o documento bruto.

### 2.1 A hipótese que justificaria a preferência não se sustenta

A justificativa plausível seria o `laudo_transformado` ser menos coberto que o `laudo_original`.
**Medido, e é falso.**

| linha | laudos | `transformado` vazio | `original` vazio | vazio no `transformado` **e** cheio no `original` |
|---|---|---|---|---|
| tirads | 4.321 | 158 | 158 | **0** |
| hepatologia | 5.374 | 0 | 0 | **0** |
| cancer_estomago | 753 | 0 | 0 | **0** |
| transplante_pulmao | 286 | 21 | 21 | **0** |

Os laudos sem `laudo_transformado` são **exatamente os mesmos** que estão sem `laudo_original`: são
registros sem texto nenhum. **O conjunto que uma troca direta perderia é vazio.**

ℹ️ O join com a Gold é 1:1 na janela — 10.734 ids, nenhum órfão, nenhuma duplicata.

---

## 3. O que muda e o que não muda

### 3.1 A proposta

Trocar a derivação do passo 3 por:

```python
df_gold = df_gold.withColumn(
    "proced_laudo_exame_original",
    F.array_join(
        F.transform(
            "proced_lista_exames",
            lambda x: F.coalesce(F.nullif(F.trim(x["laudo_transformado"]), F.lit("")),
                                 x["laudo_original"]),
        ),
        "\n",
    ),
)
```

`laudo_transformado` primeiro, `laudo_original` como reserva. **O nome da coluna derivada não muda**
— nenhum `column_map` de especialidade é tocado, nenhuma config precisa ser alterada.

### 3.2 Por que essa ordem, e por que o `coalesce` em vez da troca seca

**Nos laudos que não são RTF, os dois campos são o mesmo texto, byte a byte.** Comparação por
`md5`, na janela inteira:

| linha | laudos não-RTF | idênticos | razão de comprimento (mín · mediana · máx) |
|---|---|---|---|
| tirads | 4.047 | **4.047** | 1,0 · 1,0 · 1,0 |
| hepatologia | 5.143 | **5.143** | 1,0 · 1,0 · 1,0 |
| cancer_estomago | 648 | **648** | 1,0 · 1,0 · 1,0 |
| transplante_pulmao | 240 | **240** | 1,0 · 1,0 · 1,0 |
| **total** | **10.078** | **10.078** | — |

**Zero exceções.** A troca é, para 95,5% dos laudos, uma operação nula — e é isso que torna o
alcance da mudança inspecionável: só os 477 em RTF mudam de entrada.

O `coalesce` não tem caso medido que o justifique (§2.1 mostra o conjunto vazio). Entra mesmo
assim porque custa nada e porque a premissa que ele protege — cobertura idêntica entre os dois
campos — é da fonte, não deste pipeline, e pode mudar sem aviso.

### 3.3 Desenho alternativo, mais estreito

Aplicar o `laudo_transformado` **apenas quando o `laudo_original` for RTF**, detectando pelo
prefixo. Como os não-RTF são byte-idênticos, os dois desenhos produzem **hoje o mesmo resultado**.

O condicional é mais fácil de aprovar em revisão (o alcance está escrito no código) e mais fácil de
reverter por linha. Em troca, embute detecção de formato no pipeline de entrada — regra de conteúdo
onde hoje só há mapeamento — e deixa de corrigir o caso em que a fonte passe a divergir fora do RTF.

**Recomendado o §3.1.** O §3.3 fica registrado como a alternativa a adotar se a revisão preferir
alcance explícito a generalidade.

### 3.4 O que não muda

- **Contrato de entrada do motor:** os 7 campos, todos string. Nenhum campo novo, nenhum removido.
- **`column_map` das seis especialidades:** intocado.
- **Biblioteca `nlp_engine`:** intocada. A mudança é anterior a ela.
- **Tabela de saída, view de exportação, envio:** intocados.

---

## 4. Edge cases

| caso | frequência medida | comportamento proposto |
|---|---|---|
| `laudo_transformado` vazio e `laudo_original` cheio | **0 em 10.734** | `coalesce` devolve o `original` |
| ambos vazios | 179 em 10.734 | inalterado: laudo sem texto segue sem texto |
| não é RTF e os dois divergem | **0 em 10.078** | `coalesce` devolve o `transformado`, idêntico |
| RTF cujo `transformado` é mais curto que o texto tratado de hoje | **23 de 231** (hepatologia) | 🔴 é o caso a inspecionar no A/B — ver §6 |
| múltiplos laudos no array `proced_lista_exames` | não medido | o `coalesce` é aplicado **por elemento**, antes do `array_join`; a colagem por `\n` não muda |

### 4.1 Seções — a regressão que hepatologia obriga a checar

Hepatologia é a única linha em `segmentation.mode: auto` e já descarta 86% dos laudos na
segmentação (card `300202`). Se o texto extraído não trouxer os marcadores de seção, a mudança
piora esse quadro. Medido nos laudos em RTF:

| linha | marcador | hoje | com o texto extraído |
|---|---|---|---|
| tirads | `conclus` | 109 | 109 |
| cancer_estomago | `conclus` | 104 | 104 |
| hepatologia | `conclus` | 197 | **191** |
| hepatologia | `impress` | 20 | **27** |
| hepatologia | `tecnica` | 0 | **7** |
| tirads | `tecnica` | 0 | 1 |

Hepatologia perde `conclus` em **6** laudos — e **os 6 ganham `impress` ou `opiniao`**. É troca de
rótulo da mesma seção, não perda de estrutura. Nas outras linhas não há perda, e há ganho de
`tecnica`, marcador que hoje não sobrevive ao tratamento da marcação.

⚠️ Isso **reduz** a suspeita de regressão; não a elimina. A conferência de `segmentation_coverage`
está no aceite (§6).

---

## 5. O que esta SPEC não faz

- 🔴 **Não corrige a corrupção de encoding (`U+FFFD`), e é preciso dizer isso com clareza.**
  Medido na janela 25/08–03/09, onde o mojibake ocorre: ele **persiste integralmente** no
  `laudo_transformado` — **46 de 46** em tirads, **32 de 32** em hepatologia, **2 de 2** em
  cancer_estomago. E **nenhum** laudo com `U+FFFD` está em RTF: os dois fenômenos são **disjuntos**.
  A correção do mojibake segue sendo assunto separado, na origem do dado ou por expansão léxica.
- **Não altera a biblioteca**, nem a taxonomia de `skipped_*`, nem o gate da `0.12.2`.
- **Não trata o laudo de uma linha só.** O texto extraído continua vindo sem quebras em parte dos
  casos; a regra de boilerplate por linha permanece como está.
- **Não resolve a inconsistência de convenção** entre o registry de histórico e o pipeline do
  motor. Registra que existe.
- **Não homologa nada.** Aumento de recall sem gabarito não vai ao negócio (regra de entrega).

---

## 6. Critério de aceite

### 6.1 A medição que decide

**A/B por linha**, mesma coorte, dois runs no mesmo ambiente, tudo igual exceto a derivação do
passo 3. Comparar `fl_relevante` e `findings` por `id_exame`.

**Pré-condição obrigatória, sob pena de medição vazia:** confirmar que a coorte contém laudos em
RTF. Alvo **≥ 30** por linha medida. Sem isso, "zero divergência" não mede coisa alguma — a
mudança é nula por construção nos 95,5% não-RTF.

### 6.2 Alvos

| dimensão | alvo | o que reprova |
|---|---|---|
| laudos **não-RTF** | **zero divergência**, sem exceção | qualquer divergência derruba a premissa da §3.2 e a mudança volta para análise |
| laudos **em RTF** | divergências **apenas** `0 → 1` | qualquer `1 → 0` é perda de entrega e exige explicação laudo a laudo |
| `segmentation_coverage`, hepatologia | **não piora** contra o run de referência | queda reprova, por §4.1 |
| volume lido | redução ≥ 85% nas linhas com RTF | — |

### 6.3 🔴 O aceite técnico não autoriza a subida

A mudança **aumenta recall**: em tirads, o termo-âncora passa a alcançar 53 dos 116 laudos em RTF
contra 6 hoje. Recall novo sem gabarito **não sobe direto** — a régua do projeto é explícita:
camada que traz casos que o negócio nunca viu invalida a homologação existente.

O caminho é levar ao negócio **apenas o conjunto de discordâncias** (o que a mudança acrescenta),
por linha, e re-homologar o delta. Estimativa de tamanho do delta, teto pelo termo-âncora: **47**
laudos em tirads e **135** em hepatologia por 3 dias.

---

## 7. Risco de não fazer nada

- **51 MB por dia** de marcação atravessando leitura, tratamento e persistência, nas quatro linhas.
- **Leitura em lote da coluna continua estourando** o teto de 25 MB. Já custou duas medições
  interrompidas; custará qualquer auditoria futura que precise varrer a entrada.
- **A decisão clínica continua sendo tomada sobre texto empobrecido** em 477 laudos por 3 dias,
  com o texto íntegro gravado na mesma linha da Gold. Em tirads, 47 laudos por 3 dias mencionam
  nódulo no texto real e não mencionam no texto que chega ao motor.
- **A inconsistência de convenção permanece**, e o próximo pipeline a ser escrito herda a
  orientação do guia sem que a razão dela esteja registrada em lugar nenhum.

---

## 8. Rastreabilidade

Card a abrir: plataforma. **Distinto do `300201`** (texto de entrada duplicado `2n+1` vezes), ainda
que os dois sejam da montagem da entrada. Relacionado a `300202` (segmentação da hepatologia) pela
§4.1, e ao `299238` (SPEC 27 contradiz o código) pela §2, que acrescenta uma divergência entre guia
e comportamento desejável.

Consultas da medição em `$CLAUDE_JOB_DIR/tmp/rtf/`. ⚠️ Nenhum texto de laudo foi gravado em disco:
todas as medições são agregadas.
