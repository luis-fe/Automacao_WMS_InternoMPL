import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import controle
def IncrementarPedidos():
    conn = ConexaoCSW.Conexao()#Abrindo a Conexao com o CSW
    pedidos = pd.read_sql(BuscasAvancadas.IncrementarPediosProdutos(),conn)
    sugestoes =pd.read_sql(BuscasAvancadas.SugestaoItemAberto(),conn)
    capaPedido =pd.read_sql(BuscasAvancadas.CapaPedido2(),conn)



    pedidos = pd.merge(pedidos,sugestoes,on=['codPedido','codProduto'],how='left')
    pedidos = pd.merge(pedidos,capaPedido,on='codPedido',how='left')


    conn.close()#Fechando a Conexao com o CSW

    pedidos['codTipoNota'] = pedidos['codTipoNota'].astype(str)
    pedidos = pedidos[(pedidos['codTipoNota'] != '38') & (pedidos['codTipoNota'] != '239') & (pedidos['codTipoNota'] != '223')]

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

def CadastroSKU(rotina, datainico):
    conn = ConexaoCSW.Conexao() #Abrindo a Conexao com o CSW

    sku =pd.read_sql(BuscasAvancadas.CadastroSKU(),conn)


    conn.close() #Fechando a Conexao com o CSW
    etapa1 = controle.salvarStatus_Etapa1(rotina,'automacao', datainico,'from cgi.item i')

    ExcluindoRelacoes()
    etapa2 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa1,'Verifica se existe chavePrimaria p/excluir')

    ConexaoPostgreMPL.Funcao_InserirPCP(sku, sku['codSKU'].size, 'SKU', 'replace')
    etapa3 = controle.salvarStatus_Etapa3(rotina,'automacao', etapa2,'inserir no Postgre o cadastroSKU')

    ## Criando a chave primaria escolhendo a coluna codSKU

    chave = """ALTER TABLE pcp."SKU" ADD CONSTRAINT sku_pk PRIMARY KEY ("codSKU")"""

    conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

    cursor = conn2.cursor()# Abrindo o cursor com o Postgre
    cursor.execute(chave)
    conn2.commit() # Atualizando a chave
    cursor.close()# Fechando o cursor com o Postgre

    conn2.close() #Fechando a Conexao com o POSTGRE
    etapa4 = controle.salvarStatus_Etapa4(rotina,'automacao', etapa3,'Criar Chave primaria na tabela codSKU')


def ExcluindoRelacoes():

    try:
        chave = """ALTER TABLE pcp."pedidosItemgrade" DROP CONSTRAINT pedidositemgrade_fk """

        conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

        cursor = conn2.cursor()# Abrindo o cursor com o Postgre
        cursor.execute(chave)
        conn2.commit() # Atualizando a chave
        cursor.close()# Fechando o cursor com o Postgre

        conn2.close() #Fechando a Conexao com o POSTGRE
    except:
        print('sem relacao de chave estrangeira')



