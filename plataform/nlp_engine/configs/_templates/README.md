# Templates de Configuração por Especialidade

Objetivo: replicar o método de evolução sem alterar código clínico do motor.

## Arquivos
- `specialty_strategy_matrix.template.yaml`: matriz base de cenários A/B.

## Como usar
1. Copiar o template para `configs/<especialidade>/scenarios/strategy_matrix.yaml`.
2. Ajustar apenas `config_patch` (findings, thresholds, bandas, prompt/contexto).
3. Rodar pipeline padrão `audit -> compare -> matrix -> promotion`.
4. Promover cenário vencedor conforme `active_profile`.
