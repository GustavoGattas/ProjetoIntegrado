# Databricks notebook source
# DBTITLE 1,Passando arquivo de covid para tabela 
# TABELA COVID19
# File location and type
file_location = "/FileStore/tables/base_final/Covid_final.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("charset", "iso-8859-1") \
  .load(file_location)

# COMMAND ----------

# DBTITLE 1,Excluindo tabela Covid19_final antes de cria-la
# MAGIC %sql
# MAGIC drop table if exists Covid19_final;

# COMMAND ----------

# DBTITLE 1,Criando tabela Covid19_final
#Criar tabela COVID19
permanent_table_name = "Covid19_final"

df.write.format("csv").saveAsTable(permanent_table_name)

# COMMAND ----------

# DBTITLE 1,Passando arquivo de população para tabela
# TABELA POPULACAO_ESTADOS
# File location and type
file_location = "/FileStore/tables/base_final/Pop.estados.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("charset", "iso-8859-1") \
  .load(file_location)

# COMMAND ----------

# DBTITLE 1,Excluindo tabela Populacao_Estados antes de cria-la
# MAGIC %sql
# MAGIC drop table if exists Populacao_Estados;

# COMMAND ----------

# DBTITLE 1,Criando tabela Populacao_Estados
#Criar tabela POPULACAO_ESTADOS
permanent_table_name = "Populacao_Estados"

df.write.format("csv").saveAsTable(permanent_table_name)

# COMMAND ----------

# DBTITLE 1,Passando arquivo de recursos para tabela
# TABELA RECURSOS_GOVERNO
# File location and type
file_location = "/FileStore/tables/base_final/Recursos_Governo_Full.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("charset", "UTF-8") \
  .load(file_location)

# COMMAND ----------

# DBTITLE 1,Excluindo tabela Recursos_Gov antes de cria-la
# MAGIC %sql
# MAGIC drop table if exists Recursos_Gov;

# COMMAND ----------

# DBTITLE 1,Criando tabela Recursos_Gov
#Criar tabela RECURSOS_GOV
permanent_table_name = "Recursos_Gov"

df.write.format("csv").saveAsTable(permanent_table_name)
