import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL

def IncrementarPedidos():
    conn = ConexaoCSW.Conexao()
    pedidos = pd.read_sql(BuscasAvancadas.IncrementarPediosProdutos(),conn)
    sugestoes =pd.read_sql(BuscasAvancadas.SugestaoItemAberto(),conn)

    pedidos = pd.merge(pedidos,sugestoes,on=['codPedido','codProduto'],how='left')

    conn.close()
    pedidos['Marca'] ='-'
    pedidos['Marca'] = pedidos.apply(lambda row :  Marca(row['coditemPai'],row['Marca']) ,axis=1)

    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_InserirPCP(pedidos, pedidos['codPedido'].size, 'pedidosItemgrade', 'replace')


def Marca(coditempai, marca):
    coditempai = str(coditempai)
    if coditempai == '202' and marca =='-':
        return 'MPOLLO'
    elif coditempai == '102'and marca =='-':
        return 'MPOLLO'
    elif coditempai == '104'and marca =='-':
        return 'PACO'
    elif coditempai == '204'and marca =='-':
        return 'PACO'
    else:
        return '-'

