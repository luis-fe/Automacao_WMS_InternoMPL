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

    pedidos['Marca'] = pedidos.apply(lambda row :  Marca(row['coditemPai']) ,axis=1)

    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_InserirPCP(pedidos, pedidos['codPedido'].size, 'pedidosItemgrade', 'replace')


def Marca(itemPai):
    marca = itemPai[0:3]
    if marca == '202':
        return 'MPOLLO'
    elif marca == '102':
        return 'MPOLLO'
    elif marca == '104':
        return 'PACO'
    elif marca == '204':
        return 'PACO'
    else:
        return '-'

