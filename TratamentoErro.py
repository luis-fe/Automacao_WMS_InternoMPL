import ConexaoPostgreMPL
import datetime
import pandas as pd
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

def AtualizandoAgrupamento():
    conn = ConexaoPostgreMPL.conexao()
    query1 = pd.read_sql( 'select codigopedido, codcliente from "Reposicao".filaseparacaopedidos ', conn)
    query2 = pd.read_sql( 'select codigopedido as codigopedido2, codcliente from "Reposicao".filaseparacaopedidos ', conn)

    data = pd.merge(query1,query2,on='codcliente')
    data = data[data['codigopedido']!= data['codigopedido2']]
    if not data.empty:
        tamanho = data['codcliente'].size

        for i in range(tamanho):
            pedido1 = data['codigopedido'][i]
            pedido1 = str(pedido1)
            pedido2 = data['codigopedido2'][i]
            pedido2 = str(pedido2)
            agrupamento = pedido1+'/'+pedido2

            query = 'update "Reposicao".filaseparacaopedidos' \
                    ' set agrupamentopedido = %s ' \
                    'where codigopedido = %s '
            cursor = conn.cursor()
            cursor.execute(query,(agrupamento, pedido1) )
            conn.commit()
            cursor.close()

            cursor = conn.cursor()
            cursor.execute(query,(agrupamento, pedido2) )
            conn.commit()
            cursor.close()


        return tamanho

    else:

        return 0


