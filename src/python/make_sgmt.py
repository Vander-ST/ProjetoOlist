import os
import sqlalchemy
import argparse
import pandas as pd
import datetime
import utils        #importando arquivo utils.py onde se encontram as funções

#Definindo local do diretório de desenvolvimento
BASE_DIR =  os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ))     #Identificar endereço da pasta do projeto
DATA_DIR = os.path.join(BASE_DIR, 'data')       # Combinar o endereço BASE_DIR com a String 'data'
SQL_DIR = os.path.join(BASE_DIR, 'src' , 'sql' )        # Combina o endereço 'BASE_DIR' com a pasta 'src' e depois a pasta 'sql' 

# Criando argumentos para passar a data início e data final
parser = argparse.ArgumentParser()      #Criando o argparser para a variável parser
parser.add_argument('--date_end', '-e', help = 'Data final da extração', default = '2018-06-01') #Argumento da data será repassada pelo terminal(Ex: "python src/python/make_sgmt.py -e "2018-01-01")
args = parser.parse_args()      #Atribuir os argumentos para a variável args

date_end = args.date_end        #Pegando a data através do argparser
ano = int(date_end.split('-')[0]) - 1       #Separando a data em ano, mes e dia, fazendo a subtração de 1 do ano e atribuindo a variavel ano
mes = int(date_end.split('-')[1])           #Separando a data em ano, mes e dia, pegando a posição 1(posição do mês) e atribuindo a variavel mes
date_init = f'{ano}-{mes}-01'               #Criando a data início e atribuindo a variável data_init

#Importando a Query
query = utils.import_query(os.path.join(SQL_DIR, 'segmentos.sql'))      #importando a query através da função import_query from utils
query = query.format( date_init = date_init,   #Formatando a query com as informaçãos das variáveis date_init e date_end
                      date_end = date_end)

#Abrindo Conexão com o banco de dados
conn = utils.connect_db()

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
    utils.execute_many_sql(create_query, conn)      #Tenta executar a criação da tabela(create_query)
except:
    utils.execute_many_sql(insert_query, conn, verbose=True)        #Se não conseguiu criar a tabela, faz a inserção da query para pegar as informações do banco de dados
