import os
import sqlalchemy
import argparse
import pandas as pd
import datetime


#Definindo local do diretório de desenvolvimento
BASE_DIR =  os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ))     #Identificar endereço da pasta do projeto
DATA_DIR = os.path.join(BASE_DIR, 'data')       # Combinar o endereço BASE_DIR com a String 'data'
SQL_DIR = os.path.join(BASE_DIR, 'src' , 'sql' )        # Combina o endereço 'BASE_DIR' com a pasta 'src' e depois a pasta 'sql' 

# Criando argumentos para passar a data início e data final
parser = argparse.ArgumentParser()
parser.add_argument('--date_end', '-e', help = 'Data final da extração', default = '2018-06-01')
args = parser.parse_args()

date_end = args.date_end
ano = int(date_end.split('-')[0]) - 1
mes = int(date_end.split('-')[1])
date_init = f'{ano}-{mes}-01'

#Importando a Query
with open( os.path.join(SQL_DIR, 'segmentos.sql')) as query_file:       #Importando a Query no arquivo segmento.sql
    query = query_file.read()       #Lê a query e passa para a variável query

query = query.format( date_init = date_init,   #Formatando a query e passando os argumentos guardados na variável 'args'
                      date_end = date_end)

#Abrindo Conexão com o banco de dados
str_connection = 'sqlite:///{path}'		#Definindo String de conexão
str_connection = str_connection.format(path=os.path.join(DATA_DIR,'olist.db'))		#Trocando o caminho do path para o DATA_DIR e concatenando 'olist.db'
connection = sqlalchemy.create_engine(str_connection)

# Criando Query para criar tabela passando como parâmetro toda a query que importou através da variável 'query'
create_query = f'''     
CREATE TABLE tb_seller_sgmt AS
{query};
'''
# Primeiro deleta todas as linhas que possuem a data de segmento(DT_SGMT) igual a data salva na variável 'date' e após isso faz o insert dentro dessa tabela
insert_query = f'''
DELETE FROM tb_seller_sgmt WHERE DT_SGMT = '{date_end}';
INSERT INTO tb_seller_sgmt {query};
'''

try:
    connection.execute(create_query)   #Tentar executar o que está na variável 'create_query'
except:
    for q in insert_query.split(";")[:-1]:
        connection.execute(q)