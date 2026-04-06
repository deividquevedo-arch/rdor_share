# {{ cookiecutter.project_name }}
==============================

## Descrição
{{ cookiecutter.description }}


## Responsável:
Nome: {{ cookiecutter.author_email }}
Email:  {{ cookiecutter.author_name }}

Estrutura:

```
├── data
│   ├── ntb_ia_entrada.py
│   ├── ntb_ia_treinamento.py
│   └── ntb_ia_monitoramento.py
├── monitoring
│   ├── ntb_ia_datadrift.py
│   ├── ntb_ia_dashboard.py
│   └── ntb_ia_modeldrift.py
├── serving
│   └── ntb_ia_predicao.py
├── setup
│   ├── ntb_ia_gold_ddl.py
│   └── ntb_ia_diamond_ddl.py
├── test
│   ├── ntb_ia_entrada.py
│   ├── ntb_ia_treinamento.py
│   ├── ntb_ia_predicao.py
│   └── ntb_ia_monitoramento.py
├── workflow
│   └── ia-{{ cookiecutter-repo_name }}.json
└── README.md
```