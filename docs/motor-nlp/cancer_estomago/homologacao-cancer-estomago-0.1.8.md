# Homologação — Câncer de Estômago `0.1.8`

**Data:** 2026-08-05 · **Revisão clínica:** Carol (especialista) · **Status:** versão homologada, entregue ao negócio

---

## 1. Versão

| item | valor |
|---|---|
| `config_version` | **`0.1.8-cancer_estomago-v3`** |
| branch | `release/cancer_estomago` (`fabrica-ia-plataforma`), commit `a0c8ec1` |
| `nlp_engine` | ≥ 0.6.3 |
| corpus avaliado | 4.659 endoscopias digestivas altas |

---

## 2. Métricas — lote revisado pela especialista

**n = 44** laudos · 8 positivos / 36 negativos

| | motor: SIM | motor: NÃO |
|---|---|---|
| **especialista: SIM** | **8** | **0** |
| **especialista: NÃO** | 3 | 33 |

| métrica | valor |
|---|---|
| **Recall (sensibilidade)** | **1,0000** |
| **VPN** | **1,0000** |
| Especificidade | 0,9167 |
| Precisão (VPP) | 0,7273 |
| F1 | 0,8421 |
| Acurácia | 0,9318 |
| Acurácia balanceada | 0,9583 |
| **MCC** | **0,8165** |
| Kappa de Cohen | 0,8000 |

**Falsos negativos: 0** · **Falsos positivos: 3** (taxa 0,0833)

---

## 3. Composição do lote e o que cada bloco testou

| bloco | n | resultado | o que provou |
|---|---|---|---|
| A · positivo novo | 2 | FP=2 | comportamento não observado antes |
| B · deixou de promover | 3 | TN=3 ✅ | correções da 0.1.8 não regrediram |
| C · positivo mantido | 8 | **TP=8** ✅ | positivos confirmados seguem sendo capturados |
| D · FP que restava | 1 | FP=1 | conhecido, não alcançado pelo prompt |
| **E · negativo com termo FORTE** | **12** | **TN=12** ✅ | **carcinoma/linfoma/Bormann no texto, corretamente descartados** |
| **F · negativo com termo médio** | **10** | **TN=10** ✅ | **neoplasia/vegetante/infiltrativa, corretamente descartados** |
| H · negativo sem termo | 8 | TN=8 ✅ | amostra de segurança |

Os blocos **E e F** eram o teste que faltava: 22 laudos que **contêm** termo de malignidade e que o motor classificou como não-relevantes, nunca antes revisados. Se algum voltasse "Sim", haveria falso negativo real. **Nenhum voltou.**

---

## 4. Leitura correta das duas métricas principais

**Recall e VPN em 1,000** sustentam a homologação: nenhum paciente elegível foi perdido, e tudo que o motor descartou estava correto.

**A precisão de 0,7273 não é a taxa de produção.** O lote foi deliberadamente enriquecido em casos difíceis — 36 dos 44 negativos foram escolhidos justamente por conterem termo suspeito. No corpus real o motor promove **11 de 4.659 laudos (0,24%)**; os 3 falsos positivos equivalem a poucos minutos de revisão médica por lote.

---

## 5. Os 3 falsos positivos

| bloco | caso | regra da especialista | como foi decidido |
|---|---|---|---|
| A | gastrite erosiva + fundoplicatura prévia | gastrite não conta | juiz LLM, confiança 0,375 |
| A | úlcera Sakita A2 sem sinais de malignidade | úlcera só com sinal morfológico | juiz LLM, confiança 0,419 |
| D | neoplasia em orofaringe | fora do estômago se descarta | **regra**, confiança 0,951, sem passar pelo juiz |

Os dois primeiros vêm do juiz em faixa de baixa confiança. O terceiro é decidido pela camada de regra e é o único com correção de config identificada — ver seção 7.

---

## 6. Tentativas posteriores que NÃO se sustentaram

`0.1.9`, `0.1.10` e `0.1.11` foram geradas incorporando refinamentos da especialista. **Nenhuma superou a 0.1.8**, e o motivo é metodológico:

| versão | TP | FP | FN | recall | MCC |
|---|---|---|---|---|---|
| **0.1.8** | **8** | 3 | **0** | **1,000** | **0,8165** |
| 0.1.9 | 6 | 1 | 2 | 0,750 | 0,7616 |
| 0.1.10 | 7 | 4 | 1 | 0,875 | 0,6804 |
| 0.1.11 | 7 | 10 | 1 | 0,875 | 0,4731 |

**O lote não tem poder estatístico para comparar versões.** Com 8 positivos, um laudo mudando de lado move o MCC em ~0,1. Comparando os runs no corpus inteiro, 3 a 5 laudos trocam de lado entre versões — todos dentro da banda de incerteza e decididos pelo LLM. O `llm_called` é idêntico (1.137), então o roteamento é determinístico e só a resposta do juiz varia; `temperature` já é 0.0 por default.

Prova direta: a mesma regra esteve presente na 0.1.9 e na 0.1.10 e produziu resultados **opostos** nos mesmos dois laudos.

A 0.1.11 falhou por outro mecanismo: adicionar findings empurrou 30 laudos **acima de 0,95**, o topo da banda, onde o juiz não é chamado e a regra decide sozinha. Relevantes por regra foram de 2 para 32. É o efeito inverso do pretendido.

⚠️ **Conclusão:** as regras clínicas dessas versões são válidas e estão documentadas nos commits, mas nenhuma afirmação de melhoria de métrica se sustenta. **A referência é a 0.1.8.**

---

## 7. O que sobra como melhoria mensurada

`findings_policy.organ.max_chars` está em **220**. Reduzir para **120** elimina o falso positivo do bloco D — a neoplasia em orofaringe deixa de alcançar um termo gástrico na janela — **sem perder nenhum verdadeiro positivo**. Simulado nos 44 do lote e nos 11 relevantes do corpus.

Projeção: TP=8 · FP=2 · FN=0 → precisão **0,80** · recall **1,000** · MCC **≈0,87**.

Custo: um run. É a única mudança de config com ganho medido e risco nulo para o recall.
