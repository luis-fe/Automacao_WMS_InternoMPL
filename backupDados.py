import pandas as pd
from sqlalchemy import create_engine

import ConexaoPostgreMPL


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
    tagsreposicao = pd.read_csv('pedidoFake.csv',sep=';')
    conn = ConexaoPostgreMPL.conexao()
    tamanho = tagsreposicao['produto'].size
    query = 'insert into "Reposicao".pedidossku ' \
            '(codpedido, produto, qtdesugerida, qtdepecasconf, endereco, necessidade, datahora) ' \
            'values (%s, %s, %s, %s, %s, %s) '
    tagsreposicao['produto'] = tagsreposicao['produto'].astype(str)
    tagsreposicao['datahora'] = tagsreposicao['datahora'].astype(str)
    print(tagsreposicao.dtypes)
    if tamanho != 0:
        # Executar a consulta DELETE
       for i in range(tamanho):
            cursor = conn.cursor()
            cursor.execute(query,(tagsreposicao['codpedido'][i],tagsreposicao['produto'][i],
                                  tagsreposicao['qtdesugerida'][i],tagsreposicao['qtdepecasconf'][i],
                                  tagsreposicao['endereco'][i],tagsreposicao['necessidade'][i],
                                  ))
            conn.commit()
    else:
        print('sem incremento')


    print('Iniciando Insercao')

    print('Dados Inseridos com Suecesso!')
    return tamanho

InserirDados()

