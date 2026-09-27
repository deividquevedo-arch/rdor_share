# FMEA 01 — o texto tratado apaga a negação e o achado sai invertido

> Primeiro caso. Além de analisar a falha, fixa o formato que os próximos vão seguir.
> **14/09/2026** · linha afetada: câncer de estômago (e mais três) · estado: **corrigido**.

---

## 1. O caso, em três linhas

Em **04/09/2026**, o câncer de estômago entregou ao rastreio **6 laudos** com os achados
`Tumor; Úlcera`. Os laudos diziam *"sem úlceras ou tumorações"*.

A entrega não foi um erro de julgamento clínico da régua — foi o **tratamento do texto** apagando o
espaço antes da palavra acentuada. `sem úlceras` virou `semúlceras`, um único token, e o negador
deixou de existir para o motor.

---

## 2. Análise

| campo | conteúdo |
|---|---|
| **Etapa do processo** | tratamento do texto do laudo, antes da aplicação da régua |
| **Modo de falha** | palavra de 1 a 4 letras seguida de palavra iniciada por acento são unidas num token só |
| **Efeito** | o negador some e o achado **negado é entregue como presente** — decisão invertida |
| **Efeito no destinatário** | paciente entra na fila de rastreio sem indicação; e no sentido oposto, paciente com achado real **sai** da fila (`de úlcera` → `deúlcera`) |
| **Causa raiz** | regra de normalização herdada do algoritmo legado, aplicada sem medição |
| **Controles de detecção vigentes** | **nenhum** — a monitoria acompanha total, relevantes, taxa e confiança |

### 2.1 Severidade — **8**

Entrega decisão clínica **invertida**. Mitigado por haver conferência humana a jusante, o que
impede que se classifique como 9 ou 10.

### 2.2 Ocorrência — **7**

Medida, não estimada:

| medição | resultado |
|---|---|
| primeiro dia em produção do ca-estômago (04/09) | **6 de 151 laudos** entregues invertidos |
| coorte de 400 laudos, motor 2× com LLM desligado | **17 → 11 relevantes** (−6, 35%) |
| laudos com úlcera, efeito no sentido oposto | o achado **sumia** em **27%** deles |
| as 9 regras medidas uma a uma, em 616 laudos | **825 junções, ZERO legítimas** |

⚠️ **A função nunca acertou.** As 5 regras que a justificavam (`çã o` → `ção`) **nunca dispararam**
em nenhum laudo real.

### 2.3 Detecção — **10**

🔴 **A falha é invisível aos controles atuais.** O alerta da monitoria é queda de taxa de
relevância, e a régua sustenta a taxa: no TI-RADS ela ficou em **3,17% → 3,21%** enquanto **4.703
chamadas ao LLM falhavam**. O caso foi encontrado por leitura manual de laudo entregue, não por
sinal do sistema.

### 2.4 RPN — **8 × 7 × 10 = 560**

---

## 3. Alcance — não ficou no ca-estômago

Mesma medição, nas quatro linhas, motor duas vezes por linha com LLM desligado dos dois lados:

| linha | antes | depois |
|---|---|---|
| `cancer_estomago` | 17 | **11** (−6) |
| `tirads` | 47 | 47 |
| `hepatologia` | 5 | 5 |
| `cancer_rim` | 2 | 2 |

⚠️ **Zero não é medição vazia aqui:** o texto tratado mudou em 105, 135 e 257 de 400 laudos nas
três linhas que deram zero. A pré-condição foi verificada antes de aceitar o resultado.

---

## 4. Ações

### 4.1 Corretivas — concluídas

- Regra removida (4 das 9), as 5 legítimas mantidas — versão `0.11.2`, em produção.
- **16 testes onde havia zero**, 4 mutantes mortos.

### 4.2 Preventivas — abertas

| # | ação | estado |
|---|---|---|
| 1 | 🔴 **Reavaliar as homologações das quatro linhas** — foram feitas sobre texto com o defeito. O recall 0,600 / precisão 1,000 do ca-estômago inclui estes falsos positivos | **aberta** |
| 2 | 🔴 **Dar sinal à monitoria** — hoje não há nenhuma coluna de LLM nem de erro; falha silenciosa não gera alerta | **aberta** |
| 3 | **Código herdado do legado entra medido, regra a regra, sobre corpus real** — presença do padrão não é impacto na decisão | adotada |

---

## 5. A lição que o caso deixa

**O defeito não estava na régua clínica, e nenhuma revisão de régua o encontraria.** Estava na
camada anterior, herdada do algoritmo legado e nunca medida — e o sistema continuou entregando
números plausíveis o tempo todo.

🔴 **A pergunta que este FMEA deixa aberta é a da detecção**, não a da correção: o controle que
falhou continua sendo o único que existe. Enquanto a monitoria acompanhar só volume e taxa,
qualquer defeito que a régua consiga sustentar numericamente passa igual.
