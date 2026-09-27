# Mapa do bump pendente e dos bugs abertos — o que exige o Ops antes de subir

> Levantado em **14/09/2026**, com os estados lidos do board, não da nossa anotação.
> Objetivo: fechar os cards abertos e saber, **antes da agenda**, o que não pode subir sem
> alinhamento — para levar tudo numa conversa só.

---

## 1. 🔴 Nove cards estão prontos e abertos no board

A nossa anotação registra 15 cards da `0.12.x` como *Pronto para QA*, e dois da `0.13.0` como
entregues. O board discorda em **nove**:

| card | título | board diz |
|---|---|---|
| `253574` | [P2-08] Uniformizar a convenção de tipos de config e eliminar `type: ignore` | Em Execução |
| `253579` | [P2-13] Consolidar o singleton do spaCy e torná-lo thread-safe | Em Execução |
| `253581` | [P2-15] Dividir `rads_extraction.py` por responsabilidade | Em Execução |
| `253586` | [P2-20] Popular `tests/conftest.py` com fixtures compartilhadas | Em Execução |
| `253587` | [P2-21] Adotar `@pytest.mark.parametrize` de forma ampla na suíte | Em Execução |
| `253589` | [P2-23] Gerar referência de API a partir dos docstrings | Em Execução |
| `253590` | [P2-24] Criar `CONTRIBUTING.md` e declarar política de versionamento | Em Execução |
| `253592` | [P3-26] Separar documentação para humanos da meta-documentação para agentes | Em Execução |
| `253594` | [P3-28] Adotar hooks de pre-commit espelhando o `make check` | Em Execução |

**É trabalho feito que não aparece como feito.** Conferir a evidência de cada um e mover é a ação
mais barata da lista — fecha nove cards sem escrever uma linha de código.

✅ Já em *Desenvolvido*: `253573`, `253575`, `253576`, `253577`, `253578`, `253583`, `253585`,
`253588` e `300200`. Encerrado: `253584`.

---

## 2. O bump pendente

| versão | conteúdo | cards | estado |
|---|---|---|---|
| **`0.13.0`** | estrutura | `253579` `253581` feitos · `253580` `253582` `253591` `253593` abertos | 🟡 em curso |
| **`0.14.0`** | juiz sem evidência **+ contabilidade de tokens** | `283648` | 🔴 não iniciado |
| **`0.15.0`** | vínculo lesão ↔ medida | `285305` (defeito 2) | 🔴 não iniciado |

---

## 3. Os bugs abertos, por urgência

### 3.1 Ativos em produção

| # | card | o que está acontecendo agora | dono |
|---|---|---|---|
| 1 | `298600` — *embedding_model aponta para volume de HML do workspace antigo* | 3 linhas rodam perfil **não homologado**: 98,8% · 100% · 86,0% de `FileNotFoundError` | nosso + **schema em prd (Fábrica)** |
| 2 | `283648` — *[P0-29] Impedir que o juiz LLM promova sem evidência de regra* | o juiz pode promover sem evidência de régua | nosso |
| 3 | `283644` — *Juiz LLM ligado por contorno não documentado* | P1, **23 dias sem medição registrada** | nosso |
| 4 | **`gold_filter` da punção (TI-RADS)** — sem card | **67 punções citando TR4 em 16 dias nunca chegam ao motor**, contra 36 que o gate rebaixava | nosso |
| 5 | `300202` — *Hepatologia descarta 86% dos laudos na segmentação* | `segmentation_coverage < 1,0` em 3.867 de 4.507 | nosso, **sem dono no board** |
| 6 | `285305` — *TI-RADS entrega TR falso* (defeito 2) | medida associada ao nódulo errado | nosso |

### 3.2 Da plataforma

| # | card | situação |
|---|---|---|
| 7 | `299238` — *SPEC 27 contradiz o código* | **Novo, sem dono** — e é o que destrava o PR `7228` |
| 8 | `300201` — *Texto de entrada duplicado 2n+1 vezes* | **Novo, sem dono** · ⚠️ pode ser o bug 2 do POP-IA-08 |
| 9 | `298596` — *limit_rows não limita a fila* | Em Execução |
| 10 | **monitoria sem nenhuma coluna de LLM** — sem card | é o controle que falhou em todos os casos acima |
| 11 | **laudo chega em RTF cru** — sem card | 92% do volume de texto do dia sai de 2,7% dos registros |

---

## 4. 🔴 O que NÃO pode subir sem alinhamento — e por quê

**São quatro, e três são a mesma conversa.**

### 4.1 Contrato de saída e chaves de config — uma conversa só

| o quê | classe | detalhe |
|---|---|---|
| `0.12.3` — `waive` na config, `gate_waived_by` e `gate_waived_error` na saída | chave de config **+** campos de saída | PR de config **segurado**, branch `tirads/feature/waive-paaf` pronta |
| `0.14.0` — contabilidade de tokens na extração quantitativa | campos de saída | hoje **221 das 270 chamadas diárias** não registram token |
| `applies_to_exam_type` em `findings` (vem do DII) | chave de config | o mecanismo já existe no caminho quantitativo |

**Pedido único:** avalizar os três de uma vez. Separá-los abre três rodadas de mudança de contrato
para o mesmo time revisar.

⚠️ **A `0.15.0` provavelmente entra nesta lista** — vínculo lesão↔medida muda o conteúdo de
`findings` na saída. **A confirmar antes de especificar**, para não repetir o `waive`, que subiu na
lib antes do alinhamento.

### 4.2 🔴 Quebra de import — conversa diferente, e é deploy coordenado

**`253591` — *[P3-25] Avaliar e (se aprovado) achatar a estrutura de pacote duplamente aninhada*.**

Não é higiene interna. A plataforma importa assim:

```python
# fabrica-ia-nlp-platform/plataform/ntb_ia_motor_e2e.py:235-236
from nlp_engine.nlp_engine.config_loader import merge_with_shared_organs
from nlp_engine.nlp_engine.engine import ClinicalNlpEngine
```

Achatar `nlp_engine/nlp_engine/` para `nlp_engine/` **quebra as duas linhas**. Com produção
instalando por `latest`, a lib nova chega ao job **sem ninguém tocar no job** — e o run quebra no
import.

**O que se pede:** decidir entre (a) não achatar, (b) achatar com alias de compatibilidade, ou
(c) achatar com deploy coordenado — e, em qualquer caso, **pinar a versão antes**, porque hoje a
plataforma não tem defesa contra isso.

ℹ️ O `253593` — *[P3-27] Avaliar Protocol/dataclass nas fronteiras de API pública* — pode cair na
mesma classe. A confirmar ao especificar.

ℹ️ Este item liga-se ao POP-IA-04, que declara **layout flat, sem `src/`**, enquanto a
`nlp-engine-lib` usa `src/`. O card é anterior ao POP e trata do mesmo assunto.

### 4.3 Infraestrutura

**`298600`** depende do schema `nlp_engine` em `gold_fabrica_ia` — já está no pedido de
provisionamento enviado à Fábrica.

---

## 5. O que NÃO exige o Ops — para a agenda não gastar tempo

| item | por quê |
|---|---|
| **`gold_filter` da punção** | vive no config da especialidade, quadro *"você edita"* da Figura 2 |
| `300202` — segmentação da hepatologia | é `segmentation.mode` no config da especialidade. ⚠️ Muda o **volume entregue**, então exige alinhamento **clínico**, não de Ops |
| `283644`, `283648`, `285305` | mudam decisão, não contrato. Exigem **medição por linha** antes de subir — que é régua nossa |
| Os demais cards da `0.13.0` (`253580`, `253582`, `253574`, `253586`, `253587`, `253589`, `253590`, `253592`, `253594`) | internos à lib, sem efeito em contrato nem em import |

---

## 6. Resumo para a pauta

**Dois itens novos**, além dos que já estão na pauta mínima:

1. **Avalizar os três campos/chaves de contrato de uma vez** — `waive`, tokens, `applies_to_exam_type`.
   *(amplia o item 1.3 que já existe)*
2. 🔴 **Decidir sobre a quebra de import do `253591`** — e pinar a versão antes de qualquer decisão,
   porque com `latest` a quebra chega sozinha em produção. *(reforça o item 1.2, o pin)*

**E um item que é nosso e fecha rápido:** mover os nove cards prontos que seguem em *Em Execução*.
