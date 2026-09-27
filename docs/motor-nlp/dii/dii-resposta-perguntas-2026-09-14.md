# DII — respostas

aquiMedições boas: três variantes na mesma janela, denominador explícito, pré-condição declarada.

---

## 1. Caminho

**As duas configs saem** — perdem 22 contra 20 e custam manutenção dobrada.

**Antes de escolher entre as outras duas, roda o M2d:** o `document_vet` já expressa a régua em
config pura. Ele rebaixa `1 → 0` quando **todas** as categorias que dispararam estão em
`soft_findings` **e** o texto casa alguma `normality_phrases` — as duas listas vêm do config.

Com `soft_findings` = categoria do termo `colonoscopia`, e `normality_phrases` = boilerplate de
laudo de colonoscopia (*"aparelho introduzido"*, *"íleo terminal"*, *"preparo"*):

| laudo | efeito |
|---|---|
| colonoscopia, só o termo disparando | **rebaixa** — é o `+771` |
| colonoscopia com achado real (Crohn, úlcera) | **não rebaixa** — categorias não são subconjunto |
| imagem com *"correlacionar com colonoscopia"* | **não rebaixa** — não tem o boilerplate |

**Rodar:** união `full_doc`, janela 6, termo ativo nos dois corpora, `document_vet` ligado.

**Contar:** quantos dos `+771` o vet rebaixa · quantos dos 25 da imagem caem junto (esperado zero) ·
total de não marcados, no formato de sempre.

🔴 **Pré-condição, imprimir junto:** dos laudos que o termo acrescenta na colonoscopia, em quantos
ele é a **única** categoria a disparar? Onde não for, o vet não age por construção.

---

## 2. Os 5 laudos — classificar em duas caixas

O critério validado é **indicação de correlacionar**, não menção. Os 5 não são uma classe só:

- *"A colonoscopia **poderá trazer informações**"* → **é** indicação, em forma modal. O vocabulário
  do M2c não cobre — falha do regex, recuperável em config.
- *"**visto na** colonoscopia"* → **não** é indicação, é referência a exame já feito. **Não marcar
  está correto.**

O que cair em *indicação* vira vocabulário novo do regex; o resto sai da conta.

⚠️ Falta um número para ampliar o regex com segurança: **quantos laudos o regex do M2c traz no
corpus de colonoscopia.**

❓ Para o clínico: *"visto na colonoscopia"*, num laudo de imagem, é achado?

---

## 3. Card na lib

Não precisa abrir nada. **Segue pelo config.**

---

## 4. Os 12 regex — entram já

Adiar os 12 é adiar a homologação: os 27 valem 29% dos relevantes do legado.

Medidos **por regex**, não como bloco: quantos dos 27 recupera · quantos traz em imagem · quantos
traz em colonoscopia. **Regex com zero disparos não sobe.**

---

## 5. Os 6 FP do legado — não copiamos

- **Tirar os 6 do denominador antes de calcular a perda.** Se algum está dentro dos 20 ou dos 25, a
  distância entre as opções encolhe — é anterior à decisão.
- Enumerar caso a caso com a evidência, agrupado por via, no relatório de migração.
- ⚠️ Confirmar se são os mesmos 6 do M3b (janela 6, `fistula` solto) ou conjunto distinto.

---

## 6. Navegação

**Você, com o negócio.** Formato de referência: **`reumatologia`** — é o `cancer_rim` com as
colunas vazias removidas, e é a versão mais limpa em produção hoje.

🔴 **Decidir o filtro explicitamente, inclusive para dizer "sem filtro"** — a função falha aberto:
arquivo ausente → aviso → `{}` → view sem filtro nenhum, em silêncio.

Os 6 arquivos caem para **3** (dev, hml, prd) quando a config única valer.

---

## 7. A tabela §1.1

Correção certa, já aplicada: totais **31** e **20**. Numa config única o corpus de colonoscopia
continua sendo processado, então os 2 permanecem.

---

## Ordem

1. Depurar o denominador — tirar os 6 FP do legado
2. Classificar os 5 laudos — indicação × referência ao passado
3. **M2d — `document_vet`**, com a pré-condição impressa
4. Ampliar o regex, só se o M2d não bastar
5. Os 12 regex, um a um
6. Config único — `full_doc`, janela 6
7. Navegação — 3 arquivos, formato `reumatologia`, filtro explícito
