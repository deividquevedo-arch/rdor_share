# Databricks notebook source
# MAGIC

# COMMAND ----------

dbutils.widgets.text("release_version", "1", "Release")
release_version = dbutils.widgets.get("release_version")
release_version = f"r{release_version}"

# COMMAND ----------

dbutils.widgets.text("table_version", "1", "Versão")

table_version = dbutils.widgets.get("table_version")
table_version = f"v{table_version}"

# COMMAND ----------

dbutils.widgets.text("catalog", "ia", "Catalogo")
catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")
print("Environment:", environment)

# COMMAND ----------

if environment in ["hml", "prd"]:
    environment_tbl = ""
else:
    environment_tbl = f"{environment}_"

print("environment_tbl:", environment_tbl)

# COMMAND ----------

dbutils.widgets.text("dat_referencia", "", "Data de Referência")

# COMMAND ----------

dat_referencia = dbutils.widgets.get("dat_referencia")
if dat_referencia == "":
    dbutils.notebook.exit("Sem data de agendamento")

# COMMAND ----------

tablename = f"{catalog_name}.{environment_tbl}tbl_diamond_{{ cookiecutter.repo_name }}_treinamento_{release_version}_{table_version}"
print(tablename)

# COMMAND ----------

# DBTITLE 1,Pasta raiz que os dados devem ser salvos
root_folder = f"/mnt/trusted/datalake/{catalog_name}/data/{{ cookiecutter.repo_name }}/diamond"
print(root_folder)