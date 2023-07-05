import ConexaoPostgreMPL
import datetime
def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def RemoveDuplicatasUsuario():
    conn = ConexaoPostgreMPL.conexao()
    query = 'update "Reposicao".filareposicaoportag' \
            'set usuario = null ' \
            'where numeroop in ( ' \
            'Select numeroop from "Reposicao"."duplicatasOP") ' \

    cursor = conn.cursor()
    cursor.execute(query,)
    conn.commit()
    cursor.close()
    datahora = obterHoraAtual()

    return datahora


