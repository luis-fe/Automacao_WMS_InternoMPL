import pandas as pd
import ConexaoPostgreMPL


def EmpresaEscolhida():
    conn = ConexaoPostgreMPL.conexao()
    empresa = pd.read_sql('Select codempresa from "Reposicao".configuracoes.empresa ',conn)
    conn.close()

    return empresa['codempresa'][0]