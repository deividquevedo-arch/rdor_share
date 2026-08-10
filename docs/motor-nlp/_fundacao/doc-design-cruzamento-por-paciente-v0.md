# Design — cruzamento por paciente na camada de decisão

**Data:** 2026-08-10 · **Status:** desenho, não implementado · **Lib:** `nlp_engine` (proposta ≥ 0.9.0)
**Escopo:** capacidade **global**, agnóstica a especialidade

> Documento de desenho. Registra **os casos que motivam**, **onde fica a fronteira de
> responsabilidade**, **o contrato proposto** e **o que fica de fora**.

---

## 1. Por que existe

O motor decide **laudo a laudo**. Cinco necessidades reais já mapeadas exigem olhar **mais de um
exame do mesmo paciente** — e todas foram encontradas trabalhando, não especuladas.

| # | caso | especialidade | o que exige |
|---|---|---|---|
| 1 | **Relação T3/T4 total** — separa doença-alvo de diferencial | tireoide | comparar duas medidas de **exames diferentes** |
| 2 | **T4 livre depende do TSH** — elevado sem TSH suprimido não é hipertireoidismo | tireoide | condicionar um critério ao resultado de **outro exame** |
| 3 | **Anti-TPO "só acompanhada"** | tireoide | promover apenas se **acompanhado** de TSH suprimido ou TRAb |
| 4 | **V3.2** — sangue positivo → buscar USG com doppler | tireoide | encadear exames por paciente |
| 5 | **Paciente já em acompanhamento** — tumor tratado sem recidiva fica fora | ca_estômago | histórico do paciente |

Três desses vieram de **decisões clínicas fechadas nesta semana**. O caso 3 é literal: a resposta da
médica foi *"só acompanhada"* — o critério **não é expressável** hoje.

### O tamanho do que está em jogo

| caso | volume/mês afetado |
|---|---|
| Anti-TPO isolado (hoje resolvido virando flag) | 442 encaminhamentos |
| T4 livre sem saber o TSH | 597 encaminhamentos |
| Relação T3/T4 — pacientes no perfil de doença-alvo | 101 identificáveis |

Os dois primeiros foram **contornados** virando flag — o que preserva a precisão, mas **desiste do
caso**. A relação T3/T4 nem contorno tem.

---

## 2. O que a lib é hoje

```python
def process(self, rows: Sequence[Mapping[str, Any]], nlp_config: Mapping[str, Any],
            *, specialty_id: str = "", config_version: str = "") -> list[dict[str, Any]]
```

Três fatos verificados no código, que definem o desenho:

**Ela já recebe o lote inteiro.** `rows` é uma sequência — o motor só **escolhe** processar cada
linha isoladamente (`run_row` por row, independentes). A matéria-prima para agrupar já está lá.

**`id_paciente` já chega.** Está em `_INPUT_PASS_THROUGH`, hoje apenas repassado à saída. O motor
não tem nenhuma noção de paciente além disso.

**A lib não faz I/O, por princípio.** Não lê tabela, não conhece Spark, recebe o dict pronto. É o
que a torna testável offline e reprodutível.

E uma limitação da camada quantitativa: ela compara **medida contra limiar fixo**
(`Condition(measure, op, value)`). **Não sabe comparar uma medida com outra.**

---

## 3. A fronteira de responsabilidade

A pergunta que originou este desenho foi: *isso cabe na lib ou precisa ser externo?*

**Cabe na lib — a parte que é decisão.** E não cabe — a parte que é dado.

| responsabilidade | de quem | por quê |
|---|---|---|
| montar o lote **com o histórico** do paciente | **plataforma** | é I/O; a lib não lê tabela |
| agrupar por paciente | **lib** | já recebe o lote |
| normalizar unidade entre exames | **lib** | já tem `normalize_value` |
| aplicar limiar, razão, condição | **lib** | é onde a régua clínica mora |
| janela temporal | **config**, aplicada pela lib | é parâmetro de negócio |
| deduplicar encaminhamento por paciente | **plataforma / consumidor** | é política de entrega |

O argumento decisivo contra colocar fora: **a régua clínica já mora na lib**, com testes, auditoria e
versionamento. Uma segunda casa de régua significaria duas fontes de verdade — e a segunda sem nada
disso. Já pagamos esse preço quando `gold_filter` passou a significar coisas diferentes em duas
plataformas.

---

## 4. Contrato proposto

### 4.1 Config — bloco novo, opt-in

```python
'patient_criteria': {
    'razao_t3_t4': {
        'label': 'Perfil Graves / nódulo tóxico',
        'window_days': 30,
        'measures': {
            't3_total': {'from': 't3_total_elevado', 'unit': 'ng/dL'},
            't4_total': {'from': 't4_total_medido',  'unit': 'ug/dL'},
        },
        'ratio': {'num': 't3_total', 'den': 't4_total', 'op': '>', 'value': 20},
        'on_met': 'promote',
    },
    'anti_tpo_acompanhado': {
        'label': 'Autoimunidade com disfunção',
        'window_days': 90,
        'requires_all': ['anti_tpo_positivo'],
        'requires_any': ['tsh_suprimido', 'trab_positivo'],
        'on_met': 'promote',
    },
}
```

Duas formas, porque os casos são de naturezas diferentes:

- **`ratio`** — compara duas medidas entre si. Capacidade nova.
- **`requires_all` / `requires_any`** — combina critérios de laudo já avaliados. É o caso 2, 3 e 5.

`from` aponta para o critério por-laudo que **já produziu a medida**. O motor não re-extrai nada:
consome o que está no audit da primeira passada.

### 4.2 Semântica

**Duas passadas.** A primeira é a atual, por laudo. A segunda agrupa por `id_paciente` e avalia os
`patient_criteria` sobre os audits já produzidos.

**A janela é por par de exames**, contada em dias a partir do exame mais recente. Fora da janela, o
resultado não entra na avaliação.

**Medida ausente não decide.** Se o paciente não tem T4 total no lote, o critério de razão devolve
`met=None` — mesmo fail-safe da camada quantitativa. Não inventa.

**⚠️ Unidade é obrigatória e é onde isso mais pode falhar em silêncio.** O lake grava T3 total em
**ng/mL** e a relação clássica exige **ng/dL** — fator 100. Sem a conversão a razão dá **0,15** em
vez de **14,6**, e **nenhum caso apareceria**, sem erro nenhum. Mesma armadilha do T4 livre em
pmol/L. Por isso `unit` é campo **exigido** em `measures`, e a conversão usa o `normalize_value` que
já existe.

### 4.3 Linhas de contexto

O histórico que a plataforma passa pode estar **fora da janela de saída** — um TSH de 60 dias atrás
não deve virar linha na tabela de hoje.

Proposta: a plataforma marca essas linhas, e a lib as usa para decidir **sem emiti-las**:

```python
{'id_exame': ..., 'id_paciente': ..., 'exm_laudo_texto': ..., '_context_only': True}
```

Sem isso, ou o histórico polui a saída, ou a plataforma precisa filtrar depois — e aí a contagem de
processados deixa de bater com o que foi decidido.

### 4.4 Saída

**O contrato não muda: um registro por exame.** A decisão de paciente escreve nos registros daquele
paciente, dentro da janela.

Campos novos no audit, seguindo o padrão já estabelecido:

| campo | conteúdo |
|---|---|
| `patient_audit.<id>` | bloco por critério de paciente: `met`, `on_met`, `label`, `source`, medidas usadas, `window_days` |
| `source` | `patient_ratio` · `patient_requires` · `insufficient_history` · `implausible` |

⚠️ `insufficient_history` precisa ser **distinguível** de "não atende". É a lição do
`parse_failed`: falha silenciosa vira perda de recall invisível.

---

## 5. O que é genuinamente novo

| item | esforço | risco |
|---|---|---|
| agrupar por paciente na segunda passada | baixo — o lote já está em memória | baixo |
| `requires_all` / `requires_any` sobre audits existentes | baixo | baixo |
| **comparação medida-a-medida (`ratio`)** | médio | **conversão de unidade** |
| linhas de contexto (`_context_only`) | baixo na lib, **médio na plataforma** | contagem de processados |
| janela temporal | baixo | fuso e granularidade de `dt_exame` |
| **plataforma buscar o histórico** | **médio a alto** | volume, custo de leitura |

O trabalho maior não é na lib — é a **plataforma passar a montar lote com histórico**. Hoje ela
seleciona por janela de data; passaria a precisar de um segundo fetch por paciente.

---

## 6. O que fica de fora

**Decisão que atravessa lotes.** Se o TSH suprimido está num run de mês passado e o T4 chega hoje, a
correlação exige estado persistente entre execuções — outra classe de problema. O desenho aqui cobre
**o que está no mesmo lote**, com o histórico que a plataforma anexar.

**Deduplicação de encaminhamento.** Um paciente com três exames relevantes gera três registros. Quem
decide se isso é um contato ou três é a política de entrega, não o motor. (Já é necessário hoje: 25%
dos relevantes são exames repetidos do mesmo paciente.)

**Identidade do paciente.** O desenho confia no `id_paciente` que vem da Gold. Deduplicação de
cadastro não é problema do motor.

---

## 7. Faseamento sugerido

**Fase 1 — `requires_all` / `requires_any`.** Resolve os casos 2, 3 e 5 sem capacidade de razão nem
conversão de unidade. É o que devolve o Anti-TPO e o T4 livre à condição de promotores, com
segurança clínica. Menor esforço, maior retorno imediato.

**Fase 2 — `ratio` com conversão de unidade.** Habilita a relação T3/T4 (caso 1), que é o critério
de maior potencial para reduzir falso-positivo.

**Fase 3 — encadeamento (caso 4, V3.2).** "Sangue positivo → buscar USG com doppler" é diferente:
não é decidir sobre exames existentes, é **provocar a busca de outro exame**. Pode nem ser do motor.

A Fase 1 vale por si. Recomendo não tratar as três como um pacote.

---

## 8. Pré-requisito antes de construir

**Medir o custo do histórico.** A plataforma precisa saber quantos exames por paciente traria, em
qual janela, e o que isso custa de leitura. Sem esse número, a Fase 1 é desenho sem dimensionamento.

É uma medição que dá para fazer antes de escrever qualquer linha de código.

---

## 9. Referências

- SPEC V3 e as pendências 2b e V3.3: `docs/motor-nlp/tireoide/spec-negocio-tireoide-v3.md`
- Medida determinística e a armadilha de unidade: `doc-design-medida-estruturada-v0.md`
- Contrato da camada quantitativa: `nlp-engine-lib/docs/REFERENCIA-PARAMETROS.md` §9 e §10.3
