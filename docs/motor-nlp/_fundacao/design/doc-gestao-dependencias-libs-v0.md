# Gestao de Dependencias das 3 Libs no Databricks

**Natureza:** documento tecnico para discussao com MLOps
**Tema:** como instalar e versionar `nlp_engine`, `data_manage` e `monitoring` nos notebooks de especialidade
**Status:** em aberto -- requer alinhamento

---

## 1. Contexto

A arquitetura define 3 libs independentes (multi-repo, wheel no feed interno) consumidas por notebooks orquestradores no Databricks. Dois pontos de vista surgiram sobre como gerenciar a instalacao:

**Perspectiva MLOps:** manter `%pip install` em cada notebook de especialidade, usando recursos nativos do Databricks. Isso permite que cada especialidade use uma versao diferente da lib se necessario (ex: biliar valida v1.3 enquanto hepato permanece em v1.2).

**Perspectiva DS:** repetir `%pip install` com versoes pinadas em cada notebook de cada especialidade e um anti-pattern identificado na auditoria. Gera risco de drift e viola DRY.

Ambas as perspectivas tem fundamento tecnico. Este documento analisa as opcoes e propoe um caminho que atende aos dois lados.

---

## 2. Opcoes analisadas

### Opcao A -- `requirements.txt` centralizado

```python
# _template/ntb_motor.py (mesmo comando em todos os notebooks)
%pip install -r /Workspace/plataforma-nlp/requirements.txt --quiet
```

```
# plataforma-nlp/requirements.txt (1 arquivo, fonte unica)
nlp_engine==1.2.3
data_manage==1.0.0
monitoring==1.1.0
```

- Todas as especialidades usam as mesmas versoes
- Atualizar versao = editar 1 arquivo
- Usa `%pip install` (recurso nativo Databricks)
- Limitacao: nao permite versao diferente por especialidade

### Opcao B -- `%pip install` por notebook (versao pinada individualmente)

```python
# hepatologia/ntb_motor.py
%pip install nlp_engine==1.2.3 data_manage==1.0.0 monitoring==1.1.0

# biliar/ntb_motor.py
%pip install nlp_engine==1.3.0 data_manage==1.0.0 monitoring==1.1.0
```

- Cada especialidade controla sua versao
- Flexibilidade maxima
- Usa `%pip install` (recurso nativo Databricks)
- Limitacao: N notebooks x 3 libs = N pontos de manutencao; risco de drift

### Opcao C -- Cluster library (zero pip no notebook)

As 3 wheels sao anexadas ao cluster via UI, API, Terraform ou init script. O notebook nao contem nenhum `%pip install`.

- Dependencias resolvidas antes do notebook rodar
- Consistencia total (todas as especialidades no mesmo cluster usam mesma versao)
- Limitacao: requer gestao de cluster por MLOps; menos visibilidade no notebook

---

## 3. Cenario levantado pelo MLOps: versoes diferentes por especialidade

### Quando e real

- Uma versao nova da lib introduziu bug que afeta **so** uma especialidade (ex: mudanca na negacao que impacta biliar mas nao hepato)
- Uma especialidade esta em validacao de versao nova enquanto as outras permanecem na estavel

### Quando e hipotetico

- Se as libs seguem **semver com backward compatibility**, versoes novas nao quebram especialidades existentes
- Se o comportamento por especialidade e controlado por **YAML** (nao por versao da lib), nao ha razao para versoes diferentes

### Frequencia esperada

O cenario de versao diferente por especialidade e **transitorio** (dura ate o bug ser corrigido ou a validacao ser concluida). Nao e um estado permanente. Em um motor bem desenhado com backward compatibility, a frequencia e baixa.

---

## 4. Alternativa: feature flags via YAML

Em vez de usar versoes diferentes da lib para obter comportamentos diferentes, o comportamento varia por **configuracao** (YAML):

```yaml
# hepatologia/config.yaml -- nao usa scoring novo
nlp:
  use_new_scoring: false
  scoring_version: v1

# biliar/config.yaml -- usa scoring novo
nlp:
  use_new_scoring: true
  scoring_version: v2
```

```python
# dentro do nlp_engine (mesma versao para todos)
def score(result, config):
    if config.get("scoring_version") == "v2":
        return score_v2(result)
    return score_v1(result)
```

**Principio:** a variacao de comportamento entre especialidades vem do YAML (configuracao), nao da versao da lib (codigo). Versao diferente por especialidade e uma solucao de infraestrutura para um problema que se resolve melhor com design de software.

**Fundamento:**
- **Open/Closed Principle (SOLID):** o motor e aberto para extensao (via config) sem modificacao de codigo por especialidade
- **Feature flags:** padrao consolidado em microservicos para ativar/desativar funcionalidades por contexto
- **Semver:** minor versions adicionam funcionalidade sem quebrar; major versions podem quebrar (e ai todas migram juntas)

---

## 5. Canary release (cenario transitorio)

Se mesmo assim houver necessidade de testar uma versao nova em uma especialidade antes de liberar para todas:

```
# plataforma-nlp/requirements-stable.txt
nlp_engine==1.2.3
data_manage==1.0.0
monitoring==1.1.0

# plataforma-nlp/requirements-canary.txt
nlp_engine==1.3.0
data_manage==1.0.0
monitoring==1.1.0
```

O YAML de integracao (build-time) define qual arquivo cada especialidade usa:

```yaml
# deploy/integration/hepatologia.yaml
requirements_file: requirements-stable.txt

# deploy/integration/biliar.yaml
requirements_file: requirements-canary.txt
```

Sao **2 arquivos** (stable + canary), nao 9. Quando a validacao termina, canary vira stable e todas migram. Rastreavel, controlado, temporario.

---

## 6. Tabela comparativa

| Criterio | A: requirements.txt | B: pip por notebook | C: cluster library |
|----------|--------------------|--------------------|-------------------|
| **DRY** | 1 arquivo | N arquivos | Config de cluster |
| **Risco de drift** | Nenhum | Alto (N pontos) | Nenhum |
| **Versao por especialidade** | Nao (canary resolve) | Sim | Nao |
| **Visibilidade no notebook** | Alta (`%pip install -r`) | Alta (`%pip install`) | Baixa (implicito) |
| **Complexidade operacional** | Baixa | Baixa (mas cresce com N) | Media |
| **Usa recurso Databricks** | Sim | Sim | Sim |
| **Compativel com code gen** | Sim | Sim | Sim |
| **Tempo de instalacao** | Por execucao | Por execucao | No startup do cluster |
| **Escalabilidade (30 esp.)** | Funciona | Problematico | Funciona |

---

## 7. Recomendacao

### Fase 1 (imediato): Opcao A -- `requirements.txt` centralizado

- Usa `%pip install` (MLOps fica confortavel com recurso nativo)
- Centraliza versoes em 1 arquivo (DS fica confortavel com DRY)
- Canary release disponivel para cenarios transitorios
- Feature flags via YAML para comportamento diferenciado por especialidade
- Zero fricao de adocao; funciona hoje

### Fase 2+ (evolucao): Opcao C -- cluster library ou code generation

- Quando a esteira de code generation estiver madura, o `%pip install` pode ser gerado automaticamente a partir do YAML de integracao
- Quando cluster policies estiverem padronizadas, libs podem ser anexadas ao cluster (zero pip no notebook)
- A migacao e transparente: o notebook nao precisa mudar (so o mecanismo de instalacao)

### O que evitar

- Opcao B como estado permanente (pip por notebook com versoes individuais)
- Versao diferente por especialidade como solucao de design (usar feature flags)
- Versoes fixas sem processo de atualizacao (drift silencioso)

---

## 8. Resumo para a conversa

| Ponto MLOps | Resposta |
|-------------|----------|
| "Preciso de `%pip install` no notebook" | Concordamos. `%pip install -r requirements.txt` usa o mesmo recurso. |
| "Especialidades podem precisar de versoes diferentes" | Cenario transitorio resolvido com canary (2 arquivos, nao 9). Comportamento diferente = YAML, nao versao. |
| "Cluster library e complexo demais agora" | Concordamos. Opcao A para Fase 1. Cluster library como evolucao futura. |
| "Nao quero perder flexibilidade" | Feature flags + canary dao mais flexibilidade que versionamento por notebook, com menos risco. |
