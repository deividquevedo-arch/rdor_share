# local_samples (sem dados no Git)

**Nao coloque CSVs aqui para commit.** Por decisao atual do time, **toda a base de teste** (exports gold/diamond, `hive_metastore.*`, etc.) fica apenas em **`_local_samples/`** na raiz da lib — pasta [ignorada pelo Git](../../../.gitignore).

Este diretorio existe como marcador; ficheiros `*.csv` nesta pasta estao no `.gitignore` para evitar commits acidentais.

Ver: secao **Amostras locais** no [README.md](../../../README.md) da lib e [scripts/copy_downloads_to_local_samples.ps1](../../../scripts/copy_downloads_to_local_samples.ps1).

Fixtures **sinteticas minimas** para CI continuam noutros caminhos sob `tests/fixtures/` (ex.: `hml_parity_minimal.csv`), nao aqui.
