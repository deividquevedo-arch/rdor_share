# Evidencia de fecho S01 (v0)

Registo no repositorio para alinhar com o board; o fecho formal continua no task board.

## Criterios tecnicos (gate CI)

- `ruff check .` sem erros em `plataform/nlp_engine`.
- `pytest tests -q --ignore=tests/local` (ou `pytest tests` na CI): todos os testes S01 a verde.
- Mapeamento **T01.1–T01.5** para ficheiros de teste: ver tabela em [plataform/nlp_engine/README.md](../../plataform/nlp_engine/README.md).

## Opcional (maquina local, sem PHI no Git)

- Paridade `to_plain(Laudo)` vs `laudo_tratado`: env `NLP_HML_LAUDOS_CSV`, testes em `tests/local/test_hml_sample_to_plain_parity.py`.
- Relatorio de taxa: `NLP_HML_PARITY_REPORT_ONLY=1`, `NLP_HML_MAX_ROWS`, ver README da lib.

## Nota de produto

O alvo funcional do text pipeline para consumo do motor e o **Grupo 1b** (`to_plain` + `remove_final_laudo`, etc.). Colunas Diamond podem divergir se nao tiverem sido geradas pela mesma cadeia.

**Historia:** S01 (anexo03). **Proximo foco:** S02 T02.2+.
