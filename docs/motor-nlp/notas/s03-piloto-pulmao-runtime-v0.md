# S03/S06b — Piloto Pulmao via runtime YAML (v0)

**Objetivo:** operacionalizar o piloto Pulmao no `nlp_engine` com config em 2 camadas (`shared` + `specialty`) e validar comportamento no mesmo input contra saida legado.

**Vinculo backlog:** `S03 (T03.1, T03.2, T03.5, T03.6, T03.7)` + `S06b (T06.5, T06.6, T06.7, T06.8)` + governanca `S08 (T08.1)`.

---

## Decisoes novas desta execucao (nao documentadas antes)

1. **Especialidade piloto:** Pulmao.
2. **Gate funcional do piloto:** para o mesmo input, motor com resultado **semelhante ou superior** ao legado.
3. **Paridade S01 nao bloqueia Pulmao:** ausencia de `laudo_tratado` no schema Pulmao nao impede validacao; comparacao e feita em **saida motor vs saida legado** no mesmo conjunto.
4. **Config runtime obrigatoria:** execucao local do piloto deve usar `--config-yaml` real de Pulmao (sem fallback demo).
5. **Classificacao minima de divergencias:** `normalizacao`, `regra_clinica`, `regressao_funcional`.

---

## SPEC — Etapa 1 (contrato YAML em 2 camadas)

- **Input:** inventario de termos/achados Pulmao do legado + diretrizes de governanca.
- **Output:** contrato de:
  - `plataform/nlp_engine/configs/shared/organs.yaml`
  - `plataform/nlp_engine/configs/pulmao/config.yaml`
- **Nao faz:** tuning ad-hoc em codigo para regra clinica.
- **Edge cases:** conflito de precedencia entre `shared` e `specialty`; alias duplicado de cabecalho.
- **Aceite:** merge consistente no `config_loader` e execucao do motor com config real.

---

## SPEC — Etapa 2 (implementacao runtime Pulmao)

- **Input:** contrato YAML aprovado na etapa anterior.
- **Output:** YAMLs versionados com campos obrigatorios:
  - `specialty_id`, `config_version`, `nlp`
  - `nlp.shared_organs_path`, `nlp.target_organs`, `nlp.organs`, `nlp.findings`
- **Nao faz:** hardcode de termos clinicos em Python.
- **Edge cases:** YAML invalido, listas vazias, caminho de shared inexistente.
- **Aceite:** `audit_engine_from_csv.py --config-yaml <pulmao>` executa sem fallback demo.

---

## SPEC — Etapa 3 (testes locais da especialidade)

- **Input:** config Pulmao + frases sinteticas (sem PHI) + amostras `_local_samples`.
- **Output:** testes unitarios/integracao para:
  - merge de config shared+specialty,
  - deteccao de achados com/sem negacao,
  - validacao de YAML valido/invalido.
- **Nao faz:** dependencia exclusiva de validacao manual.
- **Edge cases:** texto sem orgao explicito, negacao curta, cabecalho ausente.
- **Aceite:** testes passam na suite local (`pytest`) e cobrem cenarios positivos/negativos.

---

## SPEC — Etapa 4 (comparacao motor vs legado no mesmo input)

- **Input:** CSV entrada Pulmao + CSV saida legado + CSV auditoria do motor.
- **Output:** relatorio com:
  - taxa de concordancia de relevancia,
  - tabela de divergencias por chave,
  - classificacao de causa (`normalizacao`, `regra_clinica`, `regressao_funcional`).
- **Nao faz:** exigir `laudo_tratado` para Pulmao.
- **Edge cases:** chave duplicada, linha sem texto, arquivo de metricas sem laudo.
- **Aceite:** decisao de gate por status: `ok`, `ok_com_gap_documentado`, `bloqueado`.

---

## Roteiro de comandos (local)

```powershell
Set-Location -LiteralPath '...\Projects\plataform\nlp_engine'

.venv\Scripts\python.exe scripts\audit_engine_from_csv.py `
  --csv "_local_samples/diamond/dev_tb_diamond_mod_pulmao.csv" `
  -o "_local_samples/exports/pulmao_motor_audit.csv" `
  --config-yaml "configs/pulmao/config.yaml" `
  --validate-output-invariants
```

Comparacao com legado (S06b) via artefato dedicado de diff por chave (`id_pct`/`exm_an`) no mesmo input.

---

## Resultado da execucao local (piloto Pulmao)

### Etapa 2 — runtime config real

- Comando executado com sucesso:
  - `audit_engine_from_csv.py --config-yaml configs/pulmao/config.yaml --validate-output-invariants`
- Evidencia: `_local_samples/exports/pulmao_motor_audit.csv` gerado com **100 linhas**.

### Etapa 3 — suite objetiva da especialidade

- Testes executados:
  - `tests/test_pulmao_runtime_config.py`
  - `tests/integration/test_pulmao_runtime_integration.py`
- Resultado: **4 passed**.

### Etapa 4 — comparacao com legado no mesmo input

- Comparador executado: `scripts/compare_pulmao_audit_vs_legacy.py`.
- Evidencia: `_local_samples/exports/pulmao_compare_report.json`.
- Resultado:
  - `n_joined=0`, `match_rate=0.0`
  - `status=bloqueado_sem_chave_comum`
  - diagnostico: sem intersecao entre `audit(id_exame)` e `legacy(id_pct)`.

### Classificacao de divergencias (corrida atual)

- `normalizacao`: **nao avaliavel** (sem join).
- `regra_clinica`: **nao avaliavel** (sem join).
- `regressao_funcional`: **nao avaliavel** (sem join).
- **Causa raiz da corrida:** coorte/chave de pareamento divergente entre os dois CSVs usados no comparador.

### Gate S06b (piloto Pulmao)

- **Status:** `bloqueado`.
- **Justificativa:** impossibilidade de comparar motor vs legado no mesmo input por ausencia de chave comum no recorte atual.
- **Proximos passos objetivos para desbloqueio:**
  1. gerar auditoria do motor para exatamente o mesmo subconjunto da saida legado (`dev_tb_diamond_mod_pulmao_saida.csv`);
  2. definir e fixar chave canonica de join (preferencia `exm_an` no par de artefatos, com fallback explicito);
  3. rerodar comparador e registrar `match_rate` + matriz de confusao + exemplos de mismatch.

### Decisao complementar (amostra padrao S06b)

- A amostra padrao para validacao local Pulmao passa a ser gerada a partir da saida legado (contendo texto + classificacao), via `scripts/build_pulmao_standard_sample.py`.
- Especificacao e comando em `notas/s06b-amostra-padrao-pulmao-v0.md`.
