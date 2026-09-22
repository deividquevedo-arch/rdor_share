# CDI — repositório clínico e legibilidade do laudo

> **Frente aberta em 2026-09-22 para dar lugar a trabalho que JÁ EXISTE e estava espalhado.**

## O que é esta frente

O que chega — e o que **não** chega — ao motor, antes de qualquer régua. Cobre o repositório
clínico na Gold, a legibilidade do laudo, os filtros de entrada medidos nos dois sentidos e a
auditoria dos relatórios gerados sobre essa base.

🔴 **É a frente com o maior desperdício medido do projeto**, e quase nada dela é de biblioteca:

| achado | tamanho |
|---|---|
| EDA que o `gold_filter` do ca-estômago **não pega** | **124 legíveis/dia**, contra 106 que ele traz |
| laudo sem conteúdo legível no repositório | **menos da metade** tem texto (25–28% apontam para outro sistema, 24–26% sem laudo) |
| punção que nunca chega ao motor no TI-RADS | **67 exames** citando TI-RADS 4 em 16 dias |

## 🔴 Material que JÁ EXISTE e pertence aqui

**Ainda em `docs/motor-nlp/_processo/` — mover quando esta frente tiver dono:**

- `medicao-endoscopia-colonoscopia-repositorio-2026-09-17.md` — 12 meses da Gold, colonoscopia
  126.351 e endoscopia alta 176.697, com a fatia legível de cada uma.
- `docs/motor-nlp/cancer_estomago/medicao-ganho-gold-filter-2026-09-17.md` — a ampliação do filtro,
  medida nos dois sentidos, com custo: **+76.533 exames, precisão 99,72%**.

**Fora do git, em `Desktop/Rede D'Or/_ferramentas/`:** os harnesses de legibilidade. Ficam fora
porque carregam texto de laudo — **dado clínico vai para o LAKE, não para o repositório**.

## Fatos que a frente já estabeleceu

⚠️ Estes custaram medição errada antes de estarem escritos:

- **O `gold_filter` lê `proced_descricao`, NÃO o laudo.** Keyword escrita supondo o texto do laudo
  perde exame em silêncio.
- **Laudo sem conteúdo tem SEIS famílias de marcador** — viewer/imagem, pdf, anexo, outro sistema,
  vazio, não-laudo. Medir por tamanho de string erra; medir por marcador acerta.
- **Exame sem item não é PDF perdido** — é linha de faturamento, e infla a família em 40% se
  entrar na conta.
- **`%endoscopia%` corta nos dois sentidos** — traz ~20 mil linhas/ano de otorrino, anestesia e
  faturamento, e **deixa de fora** retossigmoidoscopia e CPRE.
- **Acento em literal SQL casa zero linhas em silêncio.** Validar com âncora sem acento e cruzar o
  total com fonte independente.

📄 Detalhe operacional na memória: `repositorio-clinico-gold-guia.md`,
`gold-filter-le-a-descricao-nao-o-laudo.md`, `gold-laudo-sem-conteudo-tres-marcadores.md`,
`filtro-endoscopia-colono-na-gold.md`, `auditar-relatorio-gerado-genie.md`.

## Cards

**Nenhum.** Filtro de entrada é alçada do time de ciência de dados pelo POP-IA-08, então a
ampliação do `gold_filter` não precisa de card do Ops — mas **precisa de medição dos dois sentidos
antes de subir**, e isso ainda não foi feito para o ca-estômago nem para o TI-RADS.

## O que NÃO entra aqui

- Régua clínica e decisão do motor — `docs/motor-nlp/`.
- Texto de laudo, em qualquer forma. **Nunca.**

## Próximo passo concreto

**Um dia em dev com o filtro ampliado do ca-estômago**, medindo volume, chamadas ao juiz, taxa de
entrega e tempo de run. ⚠️ Ampliar **invalida** a comparação com a homologação (recall 0,600 /
precisão 1,000 é contra o corpus estreito) e a taxa de 3,97% de produção — o antes/depois tem de
sair na mesma rodada.
