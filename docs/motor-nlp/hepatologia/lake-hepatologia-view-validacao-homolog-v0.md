# Lake — view/tabela de validação Hepatologia (motor vs legado vs homolog)

**Contexto:** testes no Databricks com o mesmo espírito do fluxo local (`build_hepatologia_standard_sample.py` + audit/compare), usando gold Hive em `hive_metastore.ia`.

**Fontes (DEV):**

| Papel | Tabela |
|--------|--------|
| Laudo + legado (`flgRelevante`) | `hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida` |
| Homologação clínica | `hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_retorno` |

**Chave de join esperada:** `idPredicao` (confirmar com `DESCRIBE` — se for só `idExame`, ajustar a view).

---

## Recomendação: VIEW + snapshot opcional

| Artefacto | Quando usar | Prós | Contras |
|-----------|-------------|------|---------|
| **`vw_dev_hepatologia_motor_validacao`** | Sandbox, exploração, reruns diários | Sempre atualizado; sem duplicar laudo; alinhado ao `build` local | Schema gold muda → atualizar view |
| **`dev_tbl_hepatologia_motor_validacao_snapshot`** | Gate S06/S10, benchmark congelado | Mesma amostra em várias corridas (`run_id`) | Precisa job de refresh; armazena laudo (cuidado PHI/ACL) |

**Default:** criar só a **VIEW**. Snapshot só quando o time fechar um lote de homologação para promoção.

---

## Passo 0 — Descobrir colunas (notebook lake)

```sql
DESCRIBE TABLE hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida;
DESCRIBE TABLE hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_retorno;

-- Cardinalidade do join
SELECT COUNT(*) AS n_saida FROM hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida;
SELECT COUNT(*) AS n_retorno FROM hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_retorno;
SELECT COUNT(*) AS n_inner
FROM hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida s
INNER JOIN hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_retorno r
  ON s.idPredicao = r.idPredicao;  -- ajustar se necessário
```

---

## Passo 1 — VIEW de validação (modelo)

Gold Hive usa **camelCase** no `retorno` (`achadoRelevante`, `dataHoraRetorno`). Diamond usa snake_case (`cod_achado_relevante`).

```sql
CREATE OR REPLACE VIEW hive_metastore.ia.vw_dev_hepatologia_motor_validacao AS
SELECT
    s.idExame AS id_exame,
    s.idPaciente AS id_paciente,
    s.idPredicao AS id_predicao,
    s.laudoExame AS exm_laudo_texto,
    CAST(s.dataExecucaoModelo AS STRING) AS dt_execucao_modelo,
    CASE
        WHEN UPPER(TRIM(CAST(s.flgRelevante AS STRING))) IN ('TRUE', '1', 'T', 'S') THEN 1
        ELSE 0
    END AS fl_legado,
    r.achadoRelevante AS cod_achado_relevante,
    CASE
        WHEN r.achadoRelevante IN (
            '1 - Sim (Tem Doença Fígado)',
            '2 - Sim (Mas Não Tem Doença Fígado)'
        ) THEN 1
        ELSE 0
    END AS fl_homolog_relevante,
    r.dataHoraRetorno AS ts_homolog
FROM hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_saida AS s
INNER JOIN hive_metastore.ia.dev_tbl_gold_modelo_hepatologia_retorno AS r
    ON s.idPredicao = r.idPredicao
WHERE s.laudoExame IS NOT NULL
  AND LENGTH(TRIM(s.laudoExame)) > 50
  AND r.achadoRelevante IS NOT NULL
  AND (
    r.achadoRelevante LIKE '1 -%'
    OR r.achadoRelevante LIKE '2 -%'
    OR r.achadoRelevante LIKE '3 -%'
  );
```

**Contrato para o sandbox (`ClinicalNlpEngine.process`):**

- Entrada: `id_exame`, `id_paciente`, `exm_laudo_texto`, `exm_mod` (vazio ok), `exm_tipo`, `dt_exame` (pode mapear `dt_execucao_modelo`)
- Referência legado: `fl_legado`
- Referência homolog: `fl_homolog_relevante`, `cod_achado_relevante`

---

## Passo 2 — Coleta no sandbox (widget fonte = gold_validacao)

```sql
SELECT id_exame, id_paciente, exm_laudo_texto,
       '' AS exm_mod, '' AS exm_tipo, dt_execucao_modelo AS dt_exame,
       fl_legado, fl_homolog_relevante, cod_achado_relevante
FROM hive_metastore.ia.vw_dev_hepatologia_motor_validacao
ORDER BY ts_homolog DESC
LIMIT 150
```

Filtrar hepato no driver (como no sandbox) ou acrescentar predicado na view se houver coluna de modalidade.

---

## Passo 3 — Snapshot (opcional, gate)

```sql
CREATE TABLE IF NOT EXISTS hive_metastore.ia.dev_tbl_hepatologia_motor_validacao_snapshot
USING DELTA
AS
SELECT
    'baseline-2026-05-19' AS run_id,
    current_timestamp() AS sampled_at,
    v.*
FROM hive_metastore.ia.vw_dev_hepatologia_motor_validacao v
WHERE 1 = 0;  -- só DDL; popular com INSERT abaixo

-- Popular lote fechado (ex.: 500 linhas)
INSERT INTO hive_metastore.ia.dev_tbl_hepatologia_motor_validacao_snapshot
SELECT 'baseline-2026-05-19', current_timestamp(), v.*
FROM hive_metastore.ia.vw_dev_hepatologia_motor_validacao v
ORDER BY ts_homolog DESC
LIMIT 500;
```

Promoção de motor: comparar sempre contra o **mesmo `run_id`**.

---

## Paridade com o fluxo local

| Local | Lake |
|-------|------|
| `dev_tbl_gold_modelo_hepatologia_saida.csv` | `..._saida` |
| `build_hepatologia_standard_sample.py --source gold_hive` | VIEW acima → export opcional CSV (sem versionar PHI) |
| `compare` vs `expected` (legado) | `fl_relevante` motor vs `fl_legado` |
| (novo) homolog Diamond | `fl_relevante` motor vs `fl_homolog_relevante` |

---

## Governança

- View em schema `ia`, prefixo `vw_dev_` — leitura para cientistas de dados / motor.
- Sem PHI em git; ACL no catálogo.
- Alterações de schema gold: atualizar view + nota neste doc; snapshot antigo permanece para histórico.

**Backlog:** alinhar com história de validação hepato (S06) antes de job produtivo de refresh do snapshot.
