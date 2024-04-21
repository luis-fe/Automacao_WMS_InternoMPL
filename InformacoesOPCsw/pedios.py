import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import controle
import fastparquet as fp

def IncrementarPedidos(rotina,datainico ):
    conn = ConexaoCSW.Conexao()#Abrindo a Conexao com o CSW
    pedidos = pd.read_sql(BuscasAvancadas.IncrementarPediosProdutos(),conn)
    pedidosValores = pd.read_sql(BuscasAvancadas.ValorDosItensPedido(),conn)


    sugestoes =pd.read_sql(BuscasAvancadas.SugestaoItemAberto(),conn)
    capaPedido =pd.read_sql(BuscasAvancadas.CapaPedido2(),conn)

    etapa1 = controle.salvarStatus_Etapa1(rotina,'automacao', datainico,'from ped.pedidositemgrade')

    pedidos = pd.merge(pedidos,pedidosValores,on=['codPedido','seqCodItem'],how='left')
    pedidos = pd.merge(pedidos,sugestoes,on=['codPedido','codProduto'],how='left')
    pedidos = pd.merge(pedidos,capaPedido,on='codPedido',how='left')

    etapa2 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa1,'realizar o mergem entre pedidos+pedidositemgrade ')


    conn.close()#Fechando a Conexao com o CSW

    pedidos['codTipoNota'] = pedidos['codTipoNota'].astype(str)
    pedidos = pedidos[(pedidos['codTipoNota'] != '38') & (pedidos['codTipoNota'] != '239') & (pedidos['codTipoNota'] != '223')]
    etapa3 = controle.salvarStatus_Etapa3(rotina,'automacao', etapa2,'filtrando tipo de notas')

    # Carregando dados no Wms
    #ConexaoPostgreMPL.Funcao_InserirPCP(pedidos, pedidos['codPedido'].size, 'pedidosItemgrade', 'replace')
    # Escolha o diretório onde deseja salvar o arquivo Parquet
    diretorio = '/home/grupompl/PCP_Interno/'

    fp.write(diretorio+'pedidos.itemgrade', pedidos)
    etapa4 = controle.salvarStatus_Etapa4(rotina,'automacao', etapa3,'inserindo dados no Postgre')


    # Linkando as chave estrangeira na tabela

    chaveEstrangeira = """ALTER TABLE pcp."pedidosItemgrade" ADD CONSTRAINT pedidositemgrade_fk FOREIGN KEY ("codProduto") REFERENCES pcp."SKU"("codSKU")"""

    conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

    cursor = conn2.cursor()# Abrindo o cursor com o Postgre
    cursor.execute(chaveEstrangeira)
    conn2.commit() # Atualizando a chave
    cursor.close()# Fechando o cursor com o Postgre

    conn2.close() #Fechando a Conexao com o POSTGRE
    etapa5 = controle.salvarStatus_Etapa5(rotina,'automacao', etapa4,'criando a chave estrangeira na tabela')

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



