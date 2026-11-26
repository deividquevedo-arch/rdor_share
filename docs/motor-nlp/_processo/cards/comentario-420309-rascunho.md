# Rascunho — comentário no card `420309` — *[Ca de Colon] Sem envios de TC e RM*

> Rascunho de 25/11/2026, resultado preenchido em 26/11 (run `928025031296291`, dev, 25/11 17:54–21:40, SUCCESS).
> **Não postado.**

---

**Diagnóstico (medido em 25/11/2026)**

- O envio de TC do Ca de Cólon está parado desde **02/10/2026**, não desde novembro. Saída do algoritmo legado de TC
  (`tb_diamond_mod_colon_saida`), laudos / relevantes por mês: jun 45.149 / 221 · jul 45.269 / 189 · ago 49.046 / 212 ·
  set 47.842 / 178 · out 1.909 / 4 (só 01/10) · nov 0.
- Causa, em duas partes:
  1. o job legado foi pausado em 02/10 pelo card `360625` — *[Captação] Envios de e-mail duplicados em Neuro, Colon e
     Ateromatose*;
  2. a linha `cancer_colon` da plataforma (em produção desde 29/09) foi migrada só com colonoscopia (filtro de entrada
     `colonoscop`). O recorte é de 21/08 e não está registrado no board. TC só entra quando a descrição cita
     colonoscopia (1 a 4 laudos/mês).
- O legado selecionava **só tomografia de abdome/pelve** (`tomografia|tomo|tc` e `abd|pelve`, sem `angio` e sem
  `superior`); **ressonância não era processada**. Universo: ~46 mil TC/mês, ~170–220 relevantes/mês.
- Perda estimada desde 02/10: ~350–400 laudos relevantes de TC não enviados.

**Medição em dev — só ampliar o filtro NÃO resolve**

Régua atual da `cancer_colon` (config `0.4.0`, lib `0.15.4`, as de produção) com o filtro de entrada ampliado para
TC de abdome/pelve, exames de 01 a 28/09/2026 (52.515 laudos: 44.080 TC + 8.435 colonoscopias), comparada exame a
exame com a saída do legado de TC.

- **Controle:** as 8.407 colonoscopias pareadas com produção deram decisão idêntica em 100% (559 relevantes nos dois).
  A mudança de filtro não altera o que já roda.
- **TC, 43.304 exames pareados com o legado (97% do universo dele):**

| | legado relevante | legado não relevante |
|---|---|---|
| plataforma relevante | 127 | **1.439** |
| plataforma não relevante | **34** | 41.704 |

- **A régua de colonoscopia marca ~10× mais TC que o legado** (1.566 × 161; taxa 3,6% × 0,37%). Dos 1.439 marcados só
  pela plataforma, 1.129 vêm apenas do achado "Sinal endoscópico de gravidade", e 99% deles pelo termo `infiltra*` —
  palavra de gravidade endoscópica que em laudo de tomografia descreve outra coisa.
- **E perde 21% do que o legado achava:** 34 de 161 (tumor ou massa 20, lesão 9, ulceração 4, lesão + pólipo 1).
- Fora dos pares: 776 TC só na plataforma (51 relevantes) e 1.321 só no legado (2 relevantes).

**Conclusão:** a régua de colonoscopia não serve para laudo de imagem. A TC precisa de linha própria, com a régua de
imagem do legado migrada para config e medida contra ele, como foi feito com a colonoscopia.

**Proposta de critérios de aceite**

1. Laudos de TC de abdome/pelve voltam a ser processados diariamente em produção, cobrindo ≥ 95% do universo que o
   legado selecionava (medido numa janela de referência).
2. Concordância com o legado de TC medida na mesma janela; divergências (só legado / só plataforma) avaliadas pela
   navegação antes da entrada em produção.
3. Envio pelo mesmo fluxo da colonoscopia (view, randomização, arquivo/API), sem duplicidade com o legado.
4. Escopo da ressonância decidido e registrado (o legado não a processava).
