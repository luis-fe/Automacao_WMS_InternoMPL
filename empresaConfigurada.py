import pandas as pd
import ConexaoPostgreMPL


def EmpresaEscolhida():
    # Conectar usando SQLAlchemy
    postgre_engine = ConexaoPostgreMPL.conexaoEngine()

    # Use contexto gerenciado para assegurar o fechamento da conexão
    with postgre_engine.connect() as connection:
        empresa = pd.read_sql('SELECT codempresa FROM "Reposicao".configuracoes.empresa', connection)

    # Retorna o primeiro valor da coluna 'codempresa'
    return empresa['codempresa'][0]