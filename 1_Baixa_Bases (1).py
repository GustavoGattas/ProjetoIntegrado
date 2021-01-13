# Databricks notebook source
# DBTITLE 1,Livrarias
# Para criar lista com anomeses na célula 2:
import datetime 
from datetime import date
import dateutil

# Para baixar arquivos:q
import requests 
import time

# Para unzipar recursos governo baixado:
from zipfile import ZipFile

# Para unzipar covid-19 baixado:
import gzip 


# COMMAND ----------

# DBTITLE 1,Recursos do Governo
# LISTA COM ANOMESES AUTOMÁTIZADA (número de arq. possiveis):

data_inic = datetime.datetime(2019, 1, 1) 
data_atual = datetime.date.today()
# número de meses até hoje:
num_mes = (data_atual.year - data_inic.year) * 12 + (data_atual.month - data_inic.month)

# lista que receberá nomes dos arq. possiveis até hoje:
lista_recursos_anomeses = list()

# Cria nome do arquivo e armazena na lista:
for i in range(num_mes):
  a_date = datetime.datetime.strptime("2019-01-01", "%Y-%m-%d") #Data inicial
  a_month = dateutil.relativedelta.relativedelta(months = i) #Adição de meses
  date_plus_month = a_date + a_month # Adiciona mês a data:
  lista_recursos_anomeses.append(str(date_plus_month)[0:7].replace('-','') + '_RecebimentosRecursosPorFavorecido.csv')
  

# COMMAND ----------

# LISTA COM ARQUIVOS JÁ BAIXADOS:

recursos_ja_baixados = list()

# Função que gera/atualiza lista com os arq. que já foram baixados
def bases_recurso_baixadas():
  recursos_baixados = dbutils.fs.ls("/FileStore/tables/base_fonte/recursos_gov") # lista com arq. no databricks
  for i in range(len(recursos_baixados)):
    recursos_ja_baixados.append(recursos_baixados[i][1]) # extrai apenas o nome do arq. para criar lista com arquivos já baixados.

bases_recurso_baixadas()


# COMMAND ----------

#url = 'dbfs:/FileStore/tables/base_fonte/recursos_gov/anomes'
#for i in range(len(lista_recursos_anomeses)):
#  dbutils.fs.rm(url.replace('anomes',lista_recursos_anomeses[i]),True)

# COMMAND ----------

# VERIFICA SE ARQUIVO FOI BAIXADO, CASO NÃO, BAIXA-O E EXTRAI PARA O DIRETÓRIO:

url = 'http://www.portaltransparencia.gov.br/download-de-dados/despesas-favorecidos/'
 
num_arqui_recur = len(dbutils.fs.ls("/FileStore/tables/base_fonte/recursos_gov"))
 
# Tenta baixar arquivos enquanto houver menos do que o total: 
while num_arqui_recur < num_mes: 
  # Tenta baixar arquivos:
  try:
    for i in range(num_mes): #numero de meses/arquivos entre 201901 e hoje
      if lista_recursos_anomeses[i] in recursos_ja_baixados: # se arquivo na lista de arq. possiveis está na lista de arq. baixados
        continue
      else:
        # Baixa aquivo:
        r = requests.get(url + lista_recursos_anomeses[i][0:6], allow_redirects=True)
        open('anomes_Despesas.zip'.replace('anomes', lista_recursos_anomeses[i][0:6]), 'wb').write(r.content)
        print('baixado', lista_recursos_anomeses[i][0:6])
    
        # Extrai para diretório:
        zf = ZipFile('/databricks/driver/anomes_Despesas.zip'.replace('anomes', lista_recursos_anomeses[i][0:6]), 'r')
        zf.extractall('/dbfs/FileStore/tables/base_fonte/recursos_gov')
        zf.close()
        print('extraido', lista_recursos_anomeses[i][0:6])
        time.sleep(60)
        bases_recurso_baixadas()
        num_arqui_recur = len(dbutils.fs.ls("/FileStore/tables/base_fonte/recursos_gov"))
  #Repete o loop de download caso de erro de conexão:
  except ConnectionError:
    # Atualiza lista com bases baixadas
    bases_recurso_baixadas()
    #Reinicia loop:
    continue
    

# COMMAND ----------

# DBTITLE 1,Covid-19
# VERIFICA SE BASE JÁ ESTÁ BAIXADA:

url = 'https://data.brasil.io/dataset/covid19/caso_full.csv.gz'

covid_baixado = dbutils.fs.ls("/FileStore/tables/base_fonte/covid_19")[0][1]

if 'caso_full.csv' in covid_baixado: # se arquivo está na lista de baixados
  pass
else:
  # Baixa aquivo:
  r = requests.get(url, allow_redirects=True)
  open('caso_full.csv.gz', 'wb').write(r.content)
  
  # Extrai para diretório
  with gzip.open('caso_full.csv.gz', 'rt') as f:
    data = f.read()
    with open('/dbfs/FileStore/tables/base_fonte/covid_19/caso_full.csv', 'wt') as f:
      f.write(data)
  

# COMMAND ----------

# DBTITLE 1,Inicializa Notebook 2
# MAGIC %run /Users/heitor.santos@blueshift.com.br/2_Transforma_Bases
