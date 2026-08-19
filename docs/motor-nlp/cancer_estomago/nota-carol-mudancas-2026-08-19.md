# Câncer de Estômago — o que mudou e o que precisamos de você

**Para:** Dra. Carol · **Data:** 2026-08-19

Aconteceram **duas coisas diferentes**, e só uma muda o que o algoritmo aponta. Vale separar,
porque a confusão entre as duas é o que costuma gerar interpretação errada de métrica.

---

## 1. Mudança de plataforma — não altera nada clinicamente

O algoritmo saiu da plataforma antiga para a nova. **Nenhuma regra clínica foi tocada nessa
migração**: os achados, os termos, as exclusões, o tratamento de negação e o critério de decisão
são idênticos aos de antes — verificamos caractere a caractere.

Foi deliberado. Se um dia o resultado divergir do anterior, saberemos que a causa é a plataforma,
não a régua. Sem essa garantia, seria impossível distinguir *"mudou de sistema"* de *"mudou de
critério"*.

**Para você:** nada muda. É infraestrutura.

---

## 2. Úlcera entrou na régua — isto sim muda o que é apontado

O negócio decidiu em 18/08:

> *"Vamos considerar somente as úlceras, pois podem ser lesões neoplásicas. Restante das palavras
> vamos continuar desconsiderando."*

Ou seja: **pólipo, gastrite atrófica, metaplasia e displasia continuam fora**. Só úlcera entrou.

### Como implementamos

Criamos **dois achados separados**, em vez de um só:

| achado | o que exige | exemplo |
|---|---|---|
| **Úlcera suspeita** | úlcera **com** sinal morfológico de malignidade — bordas irregulares ou elevadas, fundo sujo, base endurecida, pregas interrompidas | *"lesão ulcerada com bordas elevadas e fibrina central"* |
| **Úlcera** | qualquer úlcera gástrica | *"úlcera em cicatrização, grande curvatura"* |

Separados de propósito: assim a fila de navegação distingue os dois, e a priorização pode
considerar a diferença. Antes existia só o primeiro — e era por isso que *"úlcera em cicatrização"*
escapava, já que não tem sinal de malignidade e estava explicitamente excluída.

### O que fica de fora, e por quê

Úlcera **duodenal, esofágica, de palato, boca ou língua** não conta — a linha de cuidado é
estômago. Um dos casos que vocês haviam marcado era literalmente *"úlcera em palato duro"*.

---

## 3. O que isso fez com os 7 casos divergentes

Vocês tinham apontado 7 laudos como relevantes que o algoritmo não pegou. Com o critério novo:

| | quantos | |
|---|---|---|
| **continuam sendo falha nossa** | **2** | úlcera gástrica real — agora são detectados |
| saem de escopo | 3 | pólipo, área enantemática, pólipo + gastrite atrófica |
| a úlcera está **negada** no texto | 1 | *"sem ulceração e/ou umbilicação"* |
| úlcera em **palato duro** | 1 | fora do estômago |

**Sensibilidade (recall) foi de 0,533 para 1,000** naquele lote — nenhum caso em escopo escapa.

⚠️ **A precisão ainda não foi medida.** Úlcera gástrica é achado comum, então é esperado que o
volume de apontamentos suba. Vamos medir antes de comprometer qualquer data com a operação.

---

## 4. Um achado importante sobre o material que revisamos

Ao preparar isto, medimos o corpus: de **10.037 exames**, **4.157 (41%) não têm o laudo no banco**.
No lugar do texto há apenas um aviso — *"Laudo gerado por sistema especialista. Para visualizar,
acesse a 'Imagem'"*.

Isso não é falha do algoritmo: **nenhuma régua encontra o que não está escrito**. Mas muda como
qualquer número deve ser lido — o denominador real é ~5.900 exames, não 10.037.

Vale vocês saberem porque afeta a interpretação de qualquer taxa que apresentarmos, e porque é
uma limitação de origem que só se resolve com a área que gera os laudos.

---

## 5. O que precisamos de você

Assim que o ambiente estiver provisionado, vamos rodar o algoritmo com a régua nova e gerar um
lote para sua revisão. Pela nossa estimativa, serão da ordem de **50 laudos novos** — os que a
regra da úlcera passa a apontar e antes não apontava.

Duas perguntas que só você responde:

1. **Os novos apontamentos são clinicamente pertinentes?** É a validação da regra da úlcera.
2. **Um caso específico para reconfirmar** — há um laudo que vocês marcaram como *não relevante*
   antes da decisão do negócio, e que descreve *"lesão ulcerada recoberta de fibrina, com pontos de
   necrose, bordas friáveis (biopsiada)"*. Pelo critério novo ele entra. Confirma?

Se achar útil, podemos incluir no lote também uma amostra dos casos que a regra **descartou** —
serve para validar o outro lado: se algum deveria ter entrado, descobrimos uma exclusão errada.
