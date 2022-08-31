import os
import pandas as pd
import utils

BASE_DIR =  os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ))     #Identificar endereço da pasta do projeto
DATA_DIR = os.path.join(BASE_DIR, 'data')               # Combinar o endereço BASE_DIR com a String 'data'
SQL_DIR = os.path.join(BASE_DIR, 'src' , 'sql' )        # Combina o endereço 'BASE_DIR' com a pasta 'src' e depois a pasta 'sql' 
DB_PATH = os.path.join(DATA_DIR,'olist.db')             # Combina o endereço 'DATA_DIR com o bando de dados 'olist.db'

conn = utils.connect_db()           #Função para conectar o banco importando de 'utils'

query = utils.import_query(os.path.join(SQL_DIR,'lifetime.sql'))        #Função para importar a query

df = pd.read_sql_query(query, conn)         #Executa a query dentro do banco e atribui ao objeto 'df'

df.to_csv(os.path.join(DATA_DIR,'lifetime.csv'),sep=",",index=False)