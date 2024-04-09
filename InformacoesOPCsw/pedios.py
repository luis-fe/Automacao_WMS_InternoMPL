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

    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_InserirPCP(pedidos, pedidos['codPedido'].size, 'pedidosItemgrade', 'replace')

def CadastroSKU():
    conn = ConexaoCSW.Conexao()

    sku =pd.read_sql(BuscasAvancadas.CadastroSKU(),conn)

    conn.close()
    ConexaoPostgreMPL.Funcao_InserirPCP(sku, sku['codProduto'].size, 'SKU', 'replace')


