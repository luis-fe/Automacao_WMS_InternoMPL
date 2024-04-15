import pandas as pd
import ConexaoPostgreMPL


def Backuptagsreposicao():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select * from "Reposicao".tagsreposicao',conn)

    conn.close()
    consulta.to_csv('backup_tagsreposicao.csv')