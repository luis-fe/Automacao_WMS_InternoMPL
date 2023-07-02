import psycopg2
from sqlalchemy import create_engine

def conexao():
    db_name = "Reposicao"
    db_user = "postgres"
    db_password = "Master100"
    db_host = "localhost"
    portbanco = "5432"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)

def Funcao_Inserir (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    database = "Reposicao"
    user = "postgres"
    password = "Master100"
    host = "127.0.0.1"
    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='Reposicao')
