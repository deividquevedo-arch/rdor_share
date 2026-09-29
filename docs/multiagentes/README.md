# Multiagentes da plataforma

> **Frente nova, aberta em 2026-09-22.** Índice para o trabalho não nascer espalhado — o desenho
> ainda não foi produzido.

## O que é esta frente

A plataforma deixa de ter **um** motor e passa a ter **vários componentes que decidem** sobre o
mesmo paciente, em momentos diferentes e sobre fontes diferentes. Esta frente trata de como eles
convivem: contrato entre eles, ordem, quem arbitra e o que acontece quando discordam.

**Componentes no horizonte:**

| componente | estado hoje |
|---|---|
| **`nlp_engine`** | ✅ em produção, seis linhas, versão pinada — é o único maduro |
| **prontuário** (campo evolução) | 🟡 estudo/mapeamento feito, apresentação na central em aberto |
| **histórico do paciente** | 🟡 desenho macro concluído; pipeline `fabrica-ia-historico-paciente` já roda na esteira |
| **NER** | 🔴 nada levantado aqui |

## Cards

| card | título | estado |
|---|---|---|
| `280008` | *[Motor NLP] Estudo e planejamento - contexto do paciente na decisao* | Novo |
| `261228` | *[Dados prontuário] Estudo/mapeamento dados campo evolução prontuário* | Desenvolvido |
| `261285` | *Organização e apresentação dados campo evolução prontuário na central navegação* | Novo |
| `197272` | *[Fábrica IA CDH] Discovery investigação prontuário* | Encerrado |

ℹ️ **O `280008` é o mais avançado:** doc macro, drawio e nota de review concluídos.
🔴 **Pendência única dele:** a **SPEC da fase 0** — exclusão e refutação no escopo do laudo. É o
critério que fecha o card, e **não depende de nenhuma das seis decisões em aberto**.

## 🔴 O que esta frente precisa decidir, e ninguém decidiu ainda

1. **Quem é a fonte da verdade quando dois componentes discordam?** O motor diz que o laudo é
   relevante e o histórico diz que o paciente já está em seguimento — quem vence, e onde isso
   fica escrito?
2. **A ordem é pipeline ou composição?** Um componente consome a saída do outro, ou os dois
   escrevem e alguém concilia? A escolha muda o contrato inteiro.
3. **O contrato entre eles é o mesmo que o da lib com a plataforma?** Se for, ele já tem dono
   (card `283647`) e não deve nascer um segundo.
4. **Cada componente tem a própria versão e o próprio pin?** Com um motor isso já produziu quatro
   trocas de versão em nove dias sem ninguém tocar no job. Com quatro componentes, multiplica.

## Lições do `nlp_engine` que valem para os outros, e custaram caro

⚠️ Estas não são teoria — cada uma tem incidente medido atrás:

- **Publicar não é adotar.** Versão disponível no feed não muda nada; quem adota é o pin.
- **Campo novo na saída exige alinhamento prévio** — e isso vale para chave nova de configuração
  também, que o Ops revisa.
- **Verificação de contrato por fixture é estruturalmente insuficiente.** Sete chaves escaparam em
  três ondas; três só apareceram no blob de um run real.
- **Falha de infraestrutura vira decisão clínica se ninguém impedir.** `fallback_policy` devolveu
  positivo em erro de transporte e entregou **1.371 laudos pela falha**, com o run fechando em
  sucesso e a taxa subindo — nenhum alerta pega isso.
- **Bloco de config declarado e não consumido reprova revisão.** Valores plausíveis não parecem
  placeholder.
- **Componente que degrada em silêncio é pior que componente que cai.** A camada semântica rodou
  meses em `token_overlap` sem ninguém notar, porque a régua sustentava a taxa.

## O que NÃO entra aqui

- Régua clínica de especialidade — `docs/motor-nlp/<especialidade>/`.
- Evolução da lib `nlp_engine` isoladamente — `docs/motor-nlp/ESTADO.md`.
- Agents de desenvolvimento (os que ajudam a **construir**) — `agents/` e `docs/agents/`. **São
  outra coisa:** aqui é arquitetura de produto, lá é ferramenta de trabalho.

## Estado

Nada produzido além do que está nos cards. O estado desta frente vive **aqui** quando houver.
