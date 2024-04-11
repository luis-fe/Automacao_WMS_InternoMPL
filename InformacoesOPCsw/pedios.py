import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL

def IncrementarPedidos():
    conn = ConexaoCSW.Conexao()#Abrindo a Conexao com o CSW
    pedidos = pd.read_sql(BuscasAvancadas.IncrementarPediosProdutos(),conn)
    sugestoes =pd.read_sql(BuscasAvancadas.SugestaoItemAberto(),conn)
    capaPedido =pd.read_sql(BuscasAvancadas.CapaPedido2(),conn)



    pedidos = pd.merge(pedidos,sugestoes,on=['codPedido','codProduto'],how='left')
    pedidos = pd.merge(pedidos,capaPedido,on='codPedido',how='left')


    conn.close()#Fechando a Conexao com o CSW

    pedidos = pedidos[~pedidos['codTipoNota'].contains('38')]

    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_InserirPCP(pedidos, pedidos['codPedido'].size, 'pedidosItemgrade', 'replace')


    # Linkando as chave estrangeira na tabela

    chaveEstrangeira = """ALTER TABLE pcp."pedidosItemgrade" ADD CONSTRAINT pedidositemgrade_fk FOREIGN KEY ("codProduto") REFERENCES pcp."SKU"("codSKU")"""

    conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

    cursor = conn2.cursor()# Abrindo o cursor com o Postgre
    cursor.execute(chaveEstrangeira)
    conn2.commit() # Atualizando a chave
    cursor.close()# Fechando o cursor com o Postgre

    conn2.close() #Fechando a Conexao com o POSTGRE

def CadastroSKU():
    conn = ConexaoCSW.Conexao() #Abrindo a Conexao com o CSW

    sku =pd.read_sql(BuscasAvancadas.CadastroSKU(),conn)


    conn.close() #Fechando a Conexao com o CSW



    ConexaoPostgreMPL.Funcao_InserirPCP(sku, sku['codSKU'].size, 'SKU', 'replace')

    ## Criando a chave primaria escolhendo a coluna codSKU

    chave = """ALTER TABLE pcp."SKU" ADD CONSTRAINT sku_pk PRIMARY KEY ("codSKU")"""

    conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

    cursor = conn2.cursor()# Abrindo o cursor com o Postgre
    cursor.execute(chave)
    conn2.commit() # Atualizando a chave
    cursor.close()# Fechando o cursor com o Postgre

    conn2.close() #Fechando a Conexao com o POSTGRE







