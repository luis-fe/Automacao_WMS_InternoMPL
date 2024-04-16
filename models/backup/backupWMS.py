import pandas as pd
import ConexaoPostgreMPL
import empresaConfigurada


def Backuptagsreposicao():
    emp = empresaConfigurada.EmpresaEscolhida()
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select * from "Reposicao".tagsreposicao',conn)

    if emp == '1':
        consulta2 = pd.read_sql('select * from "off".reposicao_qualidade',conn)
        consulta2.to_csv('backup_reposicao_qualidade.csv')

    consulta.to_csv('backup_tagsreposicao.csv')
    conn.close()
