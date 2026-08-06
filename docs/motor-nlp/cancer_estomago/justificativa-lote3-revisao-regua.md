# Câncer de Estômago — por que precisamos de uma nova rodada de revisão

**Data:** 2026-08-06 · **Para:** PO e área de negócio · **Anexo:** `homologacao_Cancer_Estomago_lote3_v0.1.8.xlsx` (348 laudos)

---

## 1. O que aconteceu

O retorno de negócio da versão `0.1.8` trouxe **37 laudos revisados**, com **15 marcados como
relevantes**. O motor havia sinalizado 11.

Cruzando os dois:

| | resultado |
|---|---|
| acertos (motor e negócio dizem *relevante*) | **8** |
| o motor sinalizou e o negócio disse *não* | **3** |
| **o negócio disse *relevante* e o motor não sinalizou** | **7** |
| ambos dizem *não relevante* | 18 |

Em taxa: o motor está **capturando pouco mais da metade** do que o negócio considera relevante.

> Vale o registro: em avaliações anteriores reportamos recall alto. Aquela leitura estava
> **enviesada por construção** — a amostra revisada era formada pelos casos que o próprio motor
> havia sinalizado, então casos perdidos nunca entravam na conta. Este retorno é o primeiro que
> inclui laudos que o motor **não** marcou, e por isso é o primeiro que mede de verdade.

---

## 2. O diagnóstico: não é falha técnica, é divergência de régua

Analisamos os 7 casos perdidos, um a um. **Nenhum** contém os achados que a régua V1 definiu como
alvo (neoplasia, tumor, massa, linfoma, lesão vegetante/infiltrativa). O que o negócio marcou como
relevante foi:

| caso | o que o laudo conclui |
|---|---|
| 1 | pólipo gástrico · retração cicatricial na incisura |
| 2 | área elevada e enantemática em antro (biópsias) |
| 3 | lesão ulcerada em antro (Sakita H1) |
| 4 | pólipos gástricos (Paris 0-Is) |
| 5 | **gastrite atrófica** · lesão polipoide séssil → mucosectomia |
| 6 | gastrite atrófica · cicatrizes (Sakita S2) |
| 7 | úlcera em cicatrização · pólipos gástricos |

São, na sua maioria, **lesões pré-malignas e situações de vigilância** — não câncer estabelecido.

Isso importa porque a régua V1 foi fechada **excluindo explicitamente** esse grupo: displasia,
metaplasia, adenoma e pólipo Paris sem outros qualificadores ficaram fora do escopo, por decisão
registrada.

**Ou seja: o motor fez o que foi especificado.** A divergência está entre a especificação e o
critério aplicado na revisão — não no algoritmo.

Os 3 falsos-positivos são o espelho do mesmo problema: o motor os marcou **sem nenhum achado de
regra**, por decisão do modelo de linguagem na zona de incerteza. São ruído de borda, e se
resolvem ajustando a faixa de acionamento — problema muito menor.

---

## 3. Por que não corrigimos direto

Seria simples ampliar o vocabulário do motor para capturar pólipo, gastrite atrófica e metaplasia.
**Não fizemos de propósito**, por três razões:

**Contrariaria uma decisão já tomada.** A exclusão desses achados foi deliberada, não esquecimento.

**O impacto é de outra ordem de grandeza.** Medimos: dos 3.189 laudos que o motor considerou não
relevantes no corpus limpo, **1.575 — 49% — mencionam algum desses termos**. Incluí-los não é
ajuste de sensibilidade: potencialmente **metade do volume de endoscopias** passaria a ser
encaminhada. Isso é dimensionamento de linha de cuidado, decisão de negócio e de capacidade
assistencial — não de configuração.

**Uma amostra de 37 laudos não sustenta a mudança.** Com 15 positivos, cada caso individual move o
indicador de qualidade em vários pontos. Não há como distinguir mudança real de régua de variação
da amostra.

---

## 4. O que estamos pedindo

Uma revisão de **348 laudos**, desenhada para responder objetivamente à pergunta:

> **A régua muda para incluir lesões pré-malignas e vigilância, ou o critério aplicado na última
> revisão foi mais amplo que o combinado?**

A planilha tem o mesmo formato das anteriores: preencher **"Achado Relevante"** (Sim/Não) e, quando
útil, **"Observações"**.

### Como a amostra foi montada

| grupo | laudos | para que serve |
|---|---|---|
| tudo que o motor sinalizou | 18 | mede a **precisão** com todos os casos, sem amostragem |
| sorteio aleatório de não-sinalizados | 250 | mede o **quanto escapa** — sorteio independente da decisão do motor, o que a revisão anterior não tinha |
| casos com termos de lesão pré-maligna | 80 | **decide a régua**: se vierem marcados como relevantes, o escopo mudou de fato |

Os laudos estão **embaralhados e sem identificação de grupo**, de propósito: saber a que grupo um
laudo pertence influenciaria a resposta. A correspondência fica do nosso lado.

Base: **3.207 laudos** de maio e junho/2026 com conteúdo diagnóstico aproveitável, de 10.037
processados. A diferença é explicada na seção 6.

### O que faremos com cada resposta

- **Se o grupo de pré-malignos vier majoritariamente "Sim"** → a régua mudou. Tratamos como
  ampliação de escopo: revisão da spec, nova estimativa de volume e nova base de referência. Não é
  ajuste de configuração.
- **Se vier majoritariamente "Não"** → os 7 casos foram critério mais amplo do avaliador. O motor
  está correto, o indicador real é bom, e resta apenas afinar a faixa de acionamento do modelo de
  linguagem para eliminar os 3 falsos-positivos.

Nos dois cenários, os 250 sorteados nos dão **pela primeira vez** uma medida confiável do que
escapa.

---

## 5. O que pedimos que seja evitado

Marcar como relevante "por precaução". A amostra aleatória só cumpre sua função se refletir o
critério real de encaminhamento — se a dúvida for resolvida sempre para "Sim", perdemos justamente
a informação que estamos buscando. **Havendo dúvida, registre em "Observações"**: é mais útil que
um Sim defensivo.

---

## 6. Uma limitação que não depende da régua

Dos 10.037 laudos processados em maio e junho, **apenas 3.207 (32%) têm conteúdo diagnóstico**. O
restante chega sem texto aproveitável — predominantemente com o aviso *"Laudo gerado por sistema
especialista. Para visualizar, acesse a Imagem"*, além de registros vazios ou só com nota de
procedimento.

Isso limita o alcance **por origem do dado**, não por qualidade do algoritmo: nenhum ajuste de
régua alcança um laudo que não tem texto. Se a cobertura for prioridade, o caminho é junto à
origem dos laudos, e é uma frente própria.

Junho é o pior mês da série (apenas 32% aproveitável, contra 72% em abril), o que sugere variação
por unidade ou por período — vale investigar separadamente.

---

## 7. Resumo

- O motor **está aderente à régua V1**; a divergência é de escopo, não de implementação.
- O indicador anterior estava **enviesado**; este é o primeiro retorno que permite medir o que escapa.
- A mudança sugerida pelos 7 casos afeta **potencialmente metade do volume** — precisa de decisão
  formal, não de ajuste técnico.
- Os **348 laudos** anexos respondem à pergunta com base estatística, e **não recomendamos alterar
  a configuração antes desse retorno**.
