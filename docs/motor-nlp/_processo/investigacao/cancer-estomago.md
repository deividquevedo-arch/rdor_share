# Investigação — câncer de estômago

> Append-only. Hipótese, medição, veredito. Três linhas. **Registro local, não documento.**
> O que impede alguém de repetir trabalho sobe para o changelog da config ou para a SPEC.

---

## 2026-08-24 · escopo da úlcera

**H:** o gate de órgão resolve os falsos positivos de úlcera — estaríamos pegando úlcera duodenal.
**Medido:** dos 54 FP, **54 mencionam duodeno** e 53 mencionam antro ou piloro. São EDAs, descrevem toda a anatomia sempre.
**Refutada.** Órgão não discrimina nada aqui. Devolver o `\b` do regex também não resolveria.

**H:** o discriminador é lexical — algum termo separa a úlcera que entra da que não entra.
**Medido:** o discriminador é **estrutural**. Nos FP, a úlcera é o **único achado** em 52 de 54 (96%); nos TP, em 5 de 13 (38%).
**Confirmada** — virou o critério da `0.6.3`.

---

## 2026-08-25 · calibração

**H:** Sakita indica benignidade e pode servir de veto (leitura da Carol).
**Medido:** dos 56 laudos do lote que citam Sakita, o negócio **aprovou 8**. Como exclusão total: precisão 0,591 / recall 0,619 — regressão nos dois eixos.
**Refutada como veto.** Vale como sinal, não como determinante.

**H:** tirar o Sakita como âncora do FALSE recupera os FN sem custo (`0.6.4`).
**Medido:** precisão 0,929 → 0,500. Recuperou 3 verdadeiros e trouxe **14 falsos**.
**Refutada.** A pergunta também alargou o TRUE; o gate caiu de 84 para 64 atuações.

**H:** um segundo critério em `ulcera_suspeita` destrava o `gate_allowed` e tira FP (`0.6.5`).
**Medido:** −1 falso positivo, **−2 verdadeiros**.
**Refutada.** `ulcera_suspeita` é a régua dizendo que já julgou; reavaliar por LLM é pedir que o juiz revise a regra.

**H:** dar precedência ao sinal de alarme sobre a benignidade recupera FN (`0.6.7`).
**Medido:** **0 verdadeiros recuperados**, +2 falsos positivos. Precisão 0,929 → 0,812.
**Refutada.**

**H:** declarar a frase inteira como negação encurta a distância até o termo.
**Medido:** 0 spans negados. **O motor ancora a janela no INÍCIO da frase de negação** — declarar mais texto não aproxima nada.
**Refutada.** O que resolve é `negation.window`: 10 falha, **12 é o mínimo que corrige**, 13/15/20 não acrescentam.

**H:** simulação com regex sobre o laudo prevê o comportamento do critério.
**Medido:** acertou **exato** na parte determinística (TP 19, FN 2) e **errou por 15** na parte de julgamento.
**Refutada.** Onde há juízo, só medindo.

---

## 2026-08-26 · o que separa os aprovados

Universo: **57 laudos de úlcera isolada**, 5 aprovados — taxa de base **8,8%**.

**H:** menção a biópsia discrimina (observação do Deivid).
**Medido:** 5 aprovados citam · **46 rejeitados também**. Taxa 9,8% — ganho de **1,0×**.
**Refutada.** Necessária nos aprovados, sem poder de separação.

**H:** retração ou convergência de pregas é sinal de alarme.
**Medido:** aparece em **5 rejeitados e 2 aprovados**. Descrições quase idênticas, veredito oposto.
**Refutada.** Adicioná-la à lista traria FP.

**H:** tamanho da úlcera discrimina.
**Medido:** < 10 mm → **0 de 13 aprovados**. 10–19 mm → 11,1%. ≥ 20 mm → 12,5%.
**Parcial.** Sinal limpo de **exclusão**, não de captura — os FN já são todos ≥ 10 mm. Registrado na SPEC §9.3, não implementado.

**H:** localização, multiplicidade, seguimento, hemoclipe, metaplasia ou NBI discriminam.
**Medido:** nenhum passa de **1,5×** com amostra que sustente. Hemoclipe e metaplasia dão 25% com **um** aprovado cada — ruído.
**Refutadas.**

**H:** o conceito de sinal de alarme discrimina.
**Medido:** 30% contra base de 8,8% — ganho de 3,4×, mas **7 de cada 10 laudos com sinal de alarme seguem sendo rejeitados**.
**Parcial e fraco.** Com 5 positivos em 57, combinar sinais fracos é sobreajuste ao lote.

**H:** os 8 não entregues são falha da régua.
**Medido:** **5** têm evidência que a SPEC sustenta — falha nossa. **3** não têm nada: a régua fez o que está especificado.
**Refutada como enunciada.** E revelou o maior: aplicar o critério da decisão 7 ao pé da letra captura 10 e acerta 3 — **o critério escrito também diverge da anotação**.

---

## Ferramenta — subiu para memória

- CSV do negócio é **cp1252**, não UTF-8. Ler como UTF-8 faz regex acentuado falhar **em silêncio**.
- `az boards --discussion` quebra com **aspas duplas** no texto; usar `&quot;`.
- PowerShell: `$FP` e `$fp` são a **mesma** variável — colisão silenciosa em contadores.
- `[ordered]@{}` com chave numérica trata `$c[283648]` como **índice**, não chave.
