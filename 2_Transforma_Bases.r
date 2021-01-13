# Databricks notebook source
# DBTITLE 1,Bibliotecas
# Biblioteca R usada:

library(dplyr)

library(readr)

library(stringr)

library(lubridate)

# COMMAND ----------

# DBTITLE 1,Lista de Caminhos Para as Bases Recurso.
# CRIA LISTA CAMINHO DOS ARQUIVOS

path_recurso <- '/dbfs/FileStore/tables/base_fonte/recursos_gov/anomes_RecebimentosRecursosPorFavorecido.csv' 

lista_anomes_recurso <- list()
lista_nome_arquivos_recursos <- list()

data_inic <- as.Date('2019-01-01')
data_atual <- today()
# Numero de meses até hoje para o loop:
num_mes <- (year(data_atual) - year(data_inic)) * 12 + (month(data_atual) - month(data_inic))

# Alimenta lista com os caminhos para os arquivos:
for (i in 1:num_mes){
  lista_anomes_recurso[[i]] <- data_inic %>% as.character() %>% substr(start=1, stop=7) %>% str_remove('-')
  lista_nome_arquivos_recursos[[i]] <- str_replace(path_recurso,'anomes', lista_anomes_recurso[[i]])
  data_inic <- data_inic %m+% period("1 month")
  }


# COMMAND ----------

# DBTITLE 1,Base Recursos_Governo_Full
# inicia uma lista vazia que conterá as bases geradas no loop abaixo
lista_bases_recursos <- list()

# Loop pelas bases da lista de caminhos:
for (i in 1:length(lista_nome_arquivos_recursos)) {
  
  # Tabela com o subset de colunas da base fonte, aqui formadatas com o tipo correto.
  recurso_bruto <- read_csv2(as.character(lista_nome_arquivos_recursos[i]), 
                              col_names = T, 
                              col_types = c('--c--c-c--cd'),
                              locale(decimal_mark = ",", encoding = "iso-8859-1"))
  
  # Renomeia colunas
  names(recurso_bruto) <- c('UF','Orgao_Superior','Orgao','Ano_Mes','Valor_Recebido') 
   
  # Exclui Distrito Federal:
  recurso_bruto <- filter(recurso_bruto, UF != "DF")
  
  # Cria tabela sumário para cada arquivo da lista.
  recursos_mes <- recurso_bruto %>%
     group_by(UF, Orgao, Ano_Mes) %>%
     summarise(Orgao_Superior = first(Orgao_Superior),
               Valor_Recebido = sum(abs(as.numeric(Valor_Recebido)))) %>%
     select(Ano_Mes,UF,Orgao_Superior,Orgao,Valor_Recebido)
    
  # Preenche a lista vazia com todas as tabelas sumários geradas no loop. 
  lista_bases_recursos <- append(lista_bases_recursos, list(recursos_mes))
}

# Gera dabela final com todas as tabelas sumário salvas na lista gerada no loop
Recursos_Governo_Full <- bind_rows(lista_bases_recursos, .id = NULL)

# Adiciona dia coringa na data para leitura de data
Recursos_Governo_Full$Ano_Mes <- as.Date(str_c('01/', Recursos_Governo_Full$Ano_Mes), format = "%d/%m/%Y")

# Remove variáveis que não serão mais utilizadas 
rm(lista_nome_arquivos_recursos, recurso_bruto, recursos_mes, lista_bases_recursos)


# COMMAND ----------

# DBTITLE 1,Base Covid
# Caminho do arquivo:
path_covid <- '/dbfs/FileStore/tables/base_fonte/covid_19/caso_full.csv'
                  
# Argumento para selecionar colunas e definir tipo:
colunas_tipo_covid <- '--c-ii-lin-ni-cc--'

# Base covid com tratamento inicial (filtros):
covid_inicial <- read_csv(path_covid, col_types = colunas_tipo_covid) %>% 
   filter(place_type=='state', is_repeated=='FALSE') %>%
   select(-c('is_repeated','place_type'))

# Cria dia coringa para formatar como Date:
str_sub(covid_inicial$date, start = 9L, end = 10L) <- '01'
# Substitui caracteres:
covid_inicial$date <- str_replace_all(covid_inicial$date, '-', '/')
# Formata como date:
covid_inicial$date <- as.Date(covid_inicial$date, format = "%Y/%m/%d")

# Considera valores do Distrito Federal (DF) como de Goias (GO):
covid_inicial$state[covid_inicial$state == "DF"] <- "GO"

# Cria base sumarizada:
Covid_final <- covid_inicial %>%
   group_by(date, state) %>%
   summarise(pop2020 = first(estimated_population),
             Casos_total = last(last_available_confirmed),
             Mortes_total = last(last_available_deaths)) %>%
   arrange(state, date)

# Renomeia colunas
names(Covid_final)[1:2] <- c('Ano_Mes', 'UF')

# Cria base sumarizada final:
Covid_final <- Covid_final %>%
   group_by(UF) %>%
   arrange(UF, Ano_Mes) %>%
   mutate(Casos_Confirmados = Casos_total - lag(Casos_total, default = 0),
          Casos_100k_habitantes = Casos_Confirmados / pop2020 * 100000,
          Mortes_Confirmadas = Mortes_total - lag(Mortes_total, default = 0),
          Mortes_100k_habitantes = Mortes_Confirmadas / pop2020 * 100000,
          Taxa_Mortalidade = round(Mortes_Confirmadas / Casos_Confirmados, 4)) %>%
   select(-Casos_total, -Mortes_total, - pop2020)


# COMMAND ----------

# DBTITLE 1,Base População Estados
# Cria base sumarizada com populações retiradas da base de Covid:
pop.estados <- covid_inicial %>%
   select(state,estimated_population,estimated_population_2019) %>%
   group_by(state) %>%
   summarise(estimated_population = first(estimated_population),
             estimated_population_2019 = first(estimated_population_2019)) %>%
   arrange(state)

# Vetor Com Estados:
Estado <- c('Acre','Alagoas','Amazonas','Amapa','Bahia','Ceara',
             'Espirito Santo','Goias','Maranhao',
             'Minas Gerais','Mato Grosso do Sul','Mato Grosso',
             'Para','Paraiba','Pernambuco','Piaui','Parana',
             'Rio de Janeiro','Rio Grande do Norte','Rondonia',
             'Roraima','Rio Grande do Sul','Santa Catarina',
             'Sergipe','Sao Paulo','Tocantins')

# Vetor com Regioes:
Regiao <- c('Norte','Nordeste','Norte','Norte','Nordeste','Nordeste',
            'Sudeste','Centro-Oeste','Nordeste',
            'Sudeste','Centro-Oeste','Centro-Oeste',
            'Norte','Nordeste','Nordeste','Nordeste','Sul',
            'Sudeste','Nordeste','Norte',
            'Norte','Sul','Sul',
            'Nordeste','Sudeste','Norte')

# Insere vetores acima como colunas na tabela pop.estados:
pop.estados <- bind_cols(pop.estados,'Estado' = Estado, 'Regiao' = Regiao) %>%
   select(Regiao,Estado,state,estimated_population,estimated_population_2019)

# Renomeia as colunas:
names(pop.estados) <- c('Regiao', 'Estado', 'UF', 'Pop.2020', 'Pop.2019')


# COMMAND ----------

# DBTITLE 1,Salva base no Drive
# Os comandos abaixo dão OVERWRITE nas bases existentes:

# Base Recursos:
try(write.csv(Recursos_Governo_Full, '/dbfs/FileStore/tables/base_final/Recursos_Governo_Full.csv'), silent=TRUE)

# Base Covid:
try(write.csv(Covid_final, '/dbfs/FileStore/tables/base_final/Covid_final.csv'), silent=TRUE)

# Base Populaçao:
try(write.csv(pop.estados, '/dbfs/FileStore/tables/base_final/Pop.estados.csv'), silent=TRUE)


# COMMAND ----------

# DBTITLE 1,Inicia próximo notebook
# MAGIC %run /Users/heitor.santos@blueshift.com.br/3_Cria_Tabela
