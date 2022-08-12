import os
import pandas as pd
import sqlalchemy
import pymysql

#Itens necessários para acesar um banco de dados Cloud
#user = 'twitch'		#Login
#psw = 'teodoroc'	#Senha
#host = 'database-1.cjyp1fkhums7.us-east-2.rds.amazon.com'	#IP/Host/DNS
#port = '3306'	#Porta

#str_connection = 'mysql+pymysql:///{user}:{psw}@{host}:{port}'		#Caminho para acessar o bando de dados cloud

#Importante o BASE_DIR e o DATA_DIR, para caso seja aberto em outra máquina, irá referenciar o local exato na máquina atual
BASE_DIR =  os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ))     #Identificar endereço da pasta do projeto
DATA_DIR = os.path.join(BASE_DIR, 'data')   #Combinar o endereço BASE_DIR com a String 'data'

files_names = os.listdir(DATA_DIR) #Listar todos os arquivos dentro desde diretório
print(files_names)
files_names = [i for i in os.listdir(DATA_DIR) if i.endswith('.csv')] #Filtrar e listar apenas arquivos com final '.csv'(COMPRESSÃO DE LISTA)
print(files_names)

for i in files_names: #Listar um embaixo do outro
    print(i)

#Abrindo Conexão com o banco de dados
str_connection = 'sqlite:///{path}'		#Definindo String de conexão
str_connection = str_connection.format(path=os.path.join(DATA_DIR,'olist.db'))		#Trocando o caminho do path para o DATA_DIR e concatenando 'olist.db'
connection = sqlalchemy.create_engine(str_connection)

for i in files_names: #Esse loop vai ler todos os arquivos CSV atribuido a files_names 
	df_tmp = pd.read_csv(os.path.join(DATA_DIR, i))	#Ler arquivos no diretório mencionado com o arquivo mencionado em files_names
	#print(df_tmp.head())	#Printar 5 primeiras linhas da tabela do arquivo através da função .head()
	table_name = "tb_" + i.strip('.csv').replace('olist_','').replace('_dataset','')
	df_tmp.to_sql(table_name, connection,if_exists='replace',index=False)

