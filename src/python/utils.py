import pandas as pd             #Importando bibliotecas
import os
import sqlalchemy
from tqdm import tqdm           #Biblioteca para criar barra de progresso
import pymysql

#Definindo local do diretório de desenvolvimento
BASE_DIR =  os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ))     #Identificar endereço da pasta do projeto
DATA_DIR = os.path.join(BASE_DIR, 'data')       # Combinar o endereço BASE_DIR com a String 'data'
DB_PATH = os.path.join(DATA_DIR, 'olist.db' )        # Combina o endereço 'DATA_DIR' com a string 'olist.db' 

def import_query(path, **kwargs):
    '''Essa função realiza o import de uma query onde pode ser passada vários argumentos de import (read())'''
    with open(path, 'r', **kwargs) as file_query:       #Abre o arquivo no caminho especificado e atribui ao objeto file_query
        query = file_query.read()                       #Lê o arquivo e atribui ao objeto query
    return query                                        #Retorna objeto 'query' ao executar a função

def connect_db():
    '''Função para conectar ao banco de dados local (sqlite)'''
    str_connection = 'sqlite:///{path}'.format(path=DB_PATH)		#Definindo String de conexão
    connection = sqlalchemy.create_engine(str_connection)           #Criando a conexão com o banco de dados
    return connection                                               #Retorna a conexão com o banco de dados

def execute_many_sql(sql, conn, verbose=False):
    '''Função para executar a query com os parâmetros obtidos em 'make_sgmt' '''
    if verbose:
        for i in tqdm(sql.split(";")[:-1]):     #Mostrar barra de progresso com o "tqdm" e separar o código sql por ';'
            conn.execute(i)                     #Executa a query com a conexão ao banco de dados
    else:
        for i in sql.split(";")[:-1]:           #Separa o código sql por ';'
            conn.execute(i)                     #Executa a query com a conexão ao banco de dados