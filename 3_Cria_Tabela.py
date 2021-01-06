# Databricks notebook source
# TABELA COVID19
# File location and type
file_location = "/FileStore/tables/base_final/Covid_final.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("charset", "iso-8859-1") \
  .load(file_location)

display(df)

# COMMAND ----------

#Criar tabela COVID19
permanent_table_name = "Covid19_final"

df.write.format("csv").saveAsTable(permanent_table_name)

# COMMAND ----------



# COMMAND ----------

# TABELA POPULACAO_ESTADOS
# File location and type
file_location = "/FileStore/tables/base_final/Pop.estados.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("charset", "iso-8859-1") \
  .load(file_location)

display(df)

# COMMAND ----------

#Criar tabela POPULACAO_ESTADOS
permanent_table_name = "Populacao_Estados"

df.write.format("csv").saveAsTable(permanent_table_name)

# COMMAND ----------



# COMMAND ----------

# TABELA RECURSOS_GOVERNO
# File location and type
file_location = "/FileStore/tables/base_final/Recursos_Governo_Full.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("charset", "iso-8859-1") \
  .load(file_location)

display(df)

# COMMAND ----------

#Criar tabela RECURSOS_GOV
permanent_table_name = "Recursos_Gov"

df.write.format("csv").saveAsTable(permanent_table_name)
