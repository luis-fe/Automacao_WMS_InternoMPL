import pandas as pd
from sqlalchemy import create_engine

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


def InserirDados():
    tagsreposicao = pd.read_csv('tagsreposicao_202307031329.csv')
    tagsreposicao['codreduzido'] = tagsreposicao['codreduzido'].astype(str)
    tagsreposicao['codbarrastag'] = tagsreposicao['codbarrastag'].astype(str)
    tagsreposicao['totalop'] = tagsreposicao['totalop'].astype(str)
    tagsreposicao['usuario'] = tagsreposicao['usuario'].astype(str)


    print('Iniciando Insercao')
    tamanho = tagsreposicao['codreduzido'].size
    Funcao_Inserir(tagsreposicao,tamanho,'tagsreposicao','replace')
    print('Dados Inseridos com Suecesso!')

InserirDados()