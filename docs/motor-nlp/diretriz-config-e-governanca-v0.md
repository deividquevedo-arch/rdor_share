# Diretriz -- Configuracao e Governanca Clinica

**Regra:** toda variacao entre especialidades vem do YAML. Toda mudanca clinica passa pelo fluxo de governanca.

---

## 1. Configuracao em duas camadas

### Camada compartilhada: `shared/organs.yaml`

- **Conteudo:** universo de orgaos para desambiguacao cross-specialty
- **Localizacao:** repo `plataforma-nlp/shared/organs.yaml`
- **Carregamento:** runtime, via path configuravel no YAML da especialidade
- **Quem atualiza:** DS (PR com review)
- **Impacto:** todas as especialidades se beneficiam automaticamente

```yaml
# shared/organs.yaml (exemplo)
figado:
  seeds: [figado, hepatico, hepatica, parenquima hepatico]
  regex: ["(?i)f[ií]gado", "(?i)hep[aá]tic[oa]"]
vias_biliares:
  seeds: [vias biliares, ducto hepatico, coledoco]
  regex: ["(?i)col[ée]doco", "(?i)ducto\\s+hep[aá]tico"]
```

### Camada de especialidade: `{nome}/config.yaml`

- **Conteudo:** secoes por lib (`nlp`, `data`, `monitoring`)
- **Localizacao:** `plataforma-nlp/especialidades/{nome}/config.yaml`
- **Carregamento:** notebook orquestrador le e injeta dict em cada lib
- **Quem atualiza:** DS (PR com validacao medica)

```yaml
# hepatologia/config.yaml (exemplo Fase 1)
specialty_id: hepatologia
version: "1.0.0"

nlp:
  shared_organs_path: "../../shared/organs.yaml"
  target_organs: [figado, vias_biliares, vesicula_biliar]
  negation_window: 7
  findings:
    cirrose: [cirrose, fibrose, hepatopatia cronica]
    hipertensao_portal: [hipertensao portal, trombose veia porta]
  threshold: 0.5

data:
  input_table: "{catalog}.ia.tb_diamond_mod_hepato_entrada"
  output_table: "{catalog}.ia.tb_diamond_mod_hepato_saida"

monitoring:
  metrics_table: "{catalog}.ia.tb_diamond_mod_metricas_qualidade"
  alert_threshold_relevance_drop: 0.15
```

### YAML build-time: `deploy/integration/{nome}.yaml`

- **Conteudo:** parametros de integracao (tabelas, catalogo, lib_version)
- **Responsabilidade:** MLOps
- **Uso:** esteira de code generation / CI/CD

---

## 2. Regras de configuracao

| Regra | Detalhe |
|-------|---------|
| Libs nao leem YAML | Notebook extrai secao e passa dict. Lib recebe dict puro. |
| Nenhum hardcode clinico | Listas de keywords, thresholds, orgaos = YAML. Nunca no `.py`. |
| Schema validado | Config loader valida campos obrigatorios e tipos ao carregar. |
| Backward compatible | Campo novo = valor default. Config antiga continua funcionando. |
| Feature flag | Comportamento diferente entre especialidades = flag no YAML, nao versao da lib. |

---

## 3. Notebook orquestrador (Composition Root)

O notebook e fixo (~50 linhas). Toda variacao vem do YAML.

```python
# ntb_motor.py (template)
# Cmd 1: instalar libs
%pip install -r /Workspace/plataforma-nlp/requirements.txt --quiet

# Cmd 2: widgets
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalogo", "dados_cientistas")
dbutils.widgets.text("specialty_id", "hepatologia")

# Cmd 3: carregar config + processar
import yaml
from nlp_engine import config_loader as nlp_config, engine
from data_manage import loader, saver
from monitoring import metrics

config = yaml.safe_load(open(f"{specialty_id}/config.yaml"))
nlp_cfg = nlp_config.load(config["nlp"])
df = loader.load(config["data"])
result = engine.process(df, nlp_cfg)
metrics.emit(result, config["monitoring"])
saver.save(result, config["data"])
```

### O que nao vai no notebook

- Logica de NLP (vai na lib `nlp_engine`)
- Logica de leitura/escrita de dados (vai na lib `data_manage`)
- Calculo de metricas (vai na lib `monitoring`)
- Listas clinicas, thresholds, regras (vai no YAML)

---

## 4. Governanca clinica

### Fluxo de mudanca

```
DS propoe mudanca       -->  PR no Git (diff do YAML)
  |                            |
  v                            v
Medico valida           -->  Review funcional (amostra de impacto)
  |                            |
  v                            v
Deploy versionado       -->  Git tag + merge; config_version no output
```

### Regras

| Regra | Detalhe |
|-------|---------|
| Mudanca de keyword/threshold | PR no YAML + validacao medica |
| `config_version` | Obrigatorio em todo output (campo na tabela de saida) |
| `engine_version` | Obrigatorio em todo output (versao da lib) |
| Rastreabilidade | Git tag por release de config. Diff auditavel. |
| Validacao pre-deploy | Rodar motor novo em amostra, comparar com baseline |

### Criterios de validacao por fase

| Fase | Criterio | Aprovador |
|------|----------|-----------|
| 1 (MVP) | Paridade: exact match contra baseline (outputs atuais) | DS + medico por amostragem |
| 2 (NLP avancado) | Paridade + melhoria mensuravel (precision/recall) | DS + medico |
| 3 (Encoder) | F-beta(2) >= baseline com p < 0.05 (McNemar) | DS + medico + gate estatistico |

---

## 5. Nova especialidade (passo a passo)

1. Copiar `especialidades/_template/` para `especialidades/{nome}/`
2. Editar `config.yaml`: preencher `specialty_id`, `target_organs`, `findings`, tabelas
3. Se necessario, adicionar orgaos em `shared/organs.yaml`
4. Rodar notebook em dev com amostra
5. Validar output (paridade com pipeline atual ou validacao medica)
6. PR com review (DS + medico)
7. Deploy versionado (Git tag)

---

*Detalhamento: `anexo04-decisoes-config-e-deploy-v0.md`, `07-relatorio-final-v0-plataforma-nlp-clinica.md` (sec 8, 10)*
