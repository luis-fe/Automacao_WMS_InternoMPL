import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL

def IncrementarPedidos():
    conn = ConexaoCSW.Conexao()
    pedidos = pd.read_sql(BuscasAvancadas.IncrementarPediosProdutos(),conn)
    sugestoes =pd.read_sql(BuscasAvancadas.SugestaoItemAberto(),conn)

    item =pd.read_sql(BuscasAvancadas.CadastroSKU(),conn)


    pedidos = pd.merge(pedidos,sugestoes,on=['codPedido','codProduto'],how='left')
    pedidos = pd.merge(pedidos,item,on=['codProduto'],how='left')



    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_InserirPCP(pedidos, pedidos['codPedido'].size, 'pedidosItemgrade', 'replace')

