import pandas as pd
import gc
import fastparquet as fp
import ConexaoCSW
import ConexaoPostgreMPL
import controle
import paramiko


class AutomacaoPedidos():
    '''
        Nesse Classe é feito a função de coletar os dados de vendas
    '''
    def __init__(self, empresa, rotina, dataInicio):
        self.empresa = empresa
        self.rotina = rotina
        self.dataInicio = dataInicio

    def incrementarPedidos(self):
        sqlcswPedidosProdutos = """
        SELECT top 1500000 
            codItem as seqCodItem, 
            p.codPedido, 
            p.codProduto , 
            p.qtdePedida ,  
            p.qtdeFaturada, 
            p.qtdeCancelada  
        FROM 
            ped.PedidoItemGrade p
        WHERE 
            p.codEmpresa = 1 
            and p.codProduto  not like '8601000%' 
            and p.codProduto  not like '83060062%'  
            and p.codProduto  not like '8306000%' 
            and p.codProduto not like '8302003%' 
            and p.codProduto not like '8306003%' 
            and p.codProduto not like '8306006%' 
            and p.codProduto not like '8306007%'
        order by codPedido desc"""


        sqlcswPedidosProdutos4 = """
        SELECT top 1000000 
            codItem as seqCodItem, 
            p.codPedido, 
            p.codProduto , 
            p.qtdePedida ,  
            p.qtdeFaturada, 
            p.qtdeCancelada  
        FROM 
            ped.PedidoItemGrade p
        WHERE 
            p.codEmpresa = 4
            and p.codProduto  not like '8601000%' 
            and p.codProduto  not like '83060062%'  
            and p.codProduto  not like '8306000%' 
            and p.codProduto not like '8302003%' 
            and p.codProduto not like '8306003%' 
            and p.codProduto not like '8306006%' 
            and p.codProduto not like '8306007%'
        order by codPedido desc"""




        sqlcswValordosProdutos = """
        select top 450000 
            item.codPedido, 
            item.CodItem as seqCodItem, 
            item.precoUnitario, item.tipoDesconto, item.descontoItem, 
            case when tipoDesconto = 1 then ( (item.qtdePedida * item.precoUnitario) - item.descontoItem)/item.qtdePedida when item.tipoDesconto = 0 then (item.precoUnitario * (1-(item.descontoItem/100))) else item.precoUnitario end  PrecoLiquido 
        from 
            ped.PedidoItem as item 
        WHERE 
            item.codEmpresa = 1 
        order by 
            item.codPedido desc """


        sqlcswValordosProdutos4 = """
        select top 450000 
            item.codPedido, 
            item.CodItem as seqCodItem, 
            item.precoUnitario, item.tipoDesconto, item.descontoItem, 
            case when tipoDesconto = 1 then ( (item.qtdePedida * item.precoUnitario) - item.descontoItem)/item.qtdePedida when item.tipoDesconto = 0 then (item.precoUnitario * (1-(item.descontoItem/100))) else item.precoUnitario end  PrecoLiquido 
        from 
            ped.PedidoItem as item 
        WHERE 
            item.codEmpresa = 4 
        order by 
            item.codPedido desc """



        sqlcswSugestoesPedidos = """
        SELECT 
            p.codPedido , 
            p.produto as codProduto , 
            p.qtdeSugerida , 
            p.qtdePecasConf,
            case when 
                (situacaoSugestao = 2 and dataHoraListagem>0) then 'Sugerido(Em Conferencia)' 
                WHEN situacaoSugestao = 0 then 'Sugerido(Gerado)' 
                WHEN situacaoSugestao = 2 then 'Sugerido(A listar)' 
                else '' end StatusSugestao
        FROM 
            ped.SugestaoPedItem p
        inner join 
            ped.SugestaoPed c 
            on c.codEmpresa = p.codEmpresa 
            and c.codPedido = p.codPedido 
            and c.codSequencia = p.codSequencia 
        WHERE 
            p.codEmpresa in (1, 4) """

        sqlcswCapPedidos = """
             SELECT top 500000 
                p.codPedido , 
                p.codTipoNota, 
                p.dataemissao, 
                p.dataPrevFat, 
                p.situacao as situacaoPedido ,
                (select c.nome from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeCliente,
                (select c.nomeEstado from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeEstado ,
                (select r.nome from fat.Representante  r WHERE r.codempresa = 1 and r.codRepresent = p.codRepresentante) as nomeRepresentante 
            FROM 
                ped.Pedido p
            WHERE 
                p.codEmpresa = 1
            order by 
                p.codPedido desc
            """


        sqlcswCapPedidos4 = """
             SELECT top 500000 
                p.codPedido , 
                p.codTipoNota, 
                p.dataemissao, 
                p.dataPrevFat, 
                p.situacao as situacaoPedido ,
                (select c.nome from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeCliente,
                (select c.nomeEstado from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeEstado ,
                (select r.nome from fat.Representante  r WHERE r.codempresa = 1 and r.codRepresent = p.codRepresentante) as nomeRepresentante 
            FROM 
                ped.Pedido p
            WHERE 
                p.codEmpresa = 4
            order by 
                p.codPedido desc
            """

        with ConexaoCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlcswPedidosProdutos)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                pedidos = pd.DataFrame(rows, columns=colunas)
                del rows, colunas

                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlcswPedidosProdutos4)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                pedidos4 = pd.DataFrame(rows, columns=colunas)
                del rows, colunas

                pedidos = pd.concat([pedidos, pedidos4])

                # Executa a segunda consulta e armazena os resultados
                cursor_csw.execute(sqlcswValordosProdutos)
                colunas2 = [desc[0] for desc in cursor_csw.description]
                rows2 = cursor_csw.fetchall()
                pedidosValores = pd.DataFrame(rows2, columns=colunas2)


                # Executa a segunda consulta e armazena os resultados
                cursor_csw.execute(sqlcswValordosProdutos4)
                colunas2 = [desc[0] for desc in cursor_csw.description]
                rows2 = cursor_csw.fetchall()
                pedidosValores4 = pd.DataFrame(rows2, columns=colunas2)

                pedidosValores = pd.concat([pedidosValores, pedidosValores4])


                pedidos = pd.merge(pedidos, pedidosValores, on=['codPedido', 'seqCodItem'], how='left')
                del pedidosValores, rows2

                # Executa a terceira consulta e armazena os resultados
                cursor_csw.execute(sqlcswSugestoesPedidos)
                colunas3 = [desc[0] for desc in cursor_csw.description]
                rows3 = cursor_csw.fetchall()
                sugestoes = pd.DataFrame(rows3, columns=colunas3)
                pedidos = pd.merge(pedidos, sugestoes, on=['codPedido', 'codProduto'], how='left')
                del sugestoes, rows3

                # Executa a quarta consulta e armazena os resultados
                cursor_csw.execute(sqlcswCapPedidos)  # Verifique se a consulta é correta
                colunas4 = [desc[0] for desc in cursor_csw.description]
                rows4 = cursor_csw.fetchall()
                capaPedido = pd.DataFrame(rows4, columns=colunas4)

                cursor_csw.execute(sqlcswCapPedidos4)  # Verifique se a consulta é correta
                colunas4 = [desc[0] for desc in cursor_csw.description]
                rows4 = cursor_csw.fetchall()
                capaPedido4 = pd.DataFrame(rows4, columns=colunas4)

                capaPedido = pd.concat([capaPedido, capaPedido4])



                pedidos = pd.merge(pedidos, capaPedido, on='codPedido', how='left')
                # Limpeza de memória
                del rows4, capaPedido
                gc.collect()

            etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio, 'from ped.pedidositemgrade')

            pedidos['codTipoNota'] = pedidos['codTipoNota'].astype(str)
            pedidos = pedidos[(pedidos['codTipoNota'] != '38') & (pedidos['codTipoNota'] != '239') & (
                        pedidos['codTipoNota'] != '223')]

            fp.write('./dados/pedidos.parquet', pedidos)

            etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1, 'Geracao do arquivo parquet no servidor origem')

            return pedidos




    def trasferenciaDeArquivo(self):
        '''Metodo que transferi o arquivo .fast entre servidores concectados '''

        # Conectar ao servidor
        transport = paramiko.Transport(('10.162.0.53', 22))
        transport.connect(username='mplti', password='wms1990@_')

        sftp = paramiko.SFTPClient.from_transport(transport)

        # Enviar o arquivo
        with open('./dados/pedidos.parquet', 'rb') as fp:
            sftp.putfo(fp, '/home/grupompl/ModuloPCP_confeccaoMPL/dados/pedidos.parquet')

        sftp.close()
        transport.close()

    def trasferenciaDeArquivoVariaveis(self):
        '''Metodo que transferi o arquivo .fast entre servidores concectados '''

        # Conectar ao servidor
        transport = paramiko.Transport(('10.162.0.53', 22))
        transport.connect(username='mplti', password='wms1990@_')

        sftp = paramiko.SFTPClient.from_transport(transport)

        # Enviar o arquivo
        with open('./dados/compVar.parquet', 'rb') as fp:
            sftp.putfo(fp, '/home/grupompl/ModuloPCP_confeccaoMPL/dados/compVar.parquet')

        sftp.close()
        transport.close()

    def trasferenciaDeArquivo2(self):
        '''Metodo que transferi o arquivo .fast entre servidores concectados '''

        # Conectar ao servidor
        transport = paramiko.Transport(('10.162.0.190', 22))
        transport.connect(username='mplti', password='wms1990@_')

        sftp = paramiko.SFTPClient.from_transport(transport)

        # Enviar o arquivo
        with open('./dados/pedidos.parquet', 'rb') as fp:
            sftp.putfo(fp, '/home/mplti/ModuloPCP/dados/pedidos.parquet')

        sftp.close()
        transport.close()


    def IncrementadoDadosPostgre(self):
        dados = self.incrementarPedidos()
        ConexaoPostgreMPL.Funcao_InserirPCPBackupMatriz(dados, dados['codPedido'].size, 'pedidos', 'replace')
        return pd.DataFrame([{'status': True, 'Mensagem': 'Dados carregados com sucesso !'}])



    def inserirComponentesVariaveis(self):

        sql = """
        SELECT 
                            v.codProduto as codEngenharia, 
                            cv.codSortimento, 
                            cv.seqTamanho as codSeqTamanho,  
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            cv.quantidade  
                        from 
                            tcp.ComponentesVariaveis v 
                        join 
                            tcp.CompVarSorGraTam cv 
                            on cv.codEmpresa = v.codEmpresa 
                            and cv.codProduto = v.codProduto 
                            and cv.sequencia = v.codSequencia 
                        WHERE 
                            v.codEmpresa = 1
                            and v.codClassifComponente <> 12
                        UNION 
                        SELECT 
                            v.codProduto as codEngenharia,  
                            l.codSortimento ,
                            l.codSeqTamanho  as codSeqTamanho, 
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE  i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            v.quantidade  
                        from 
                            tcp.ComponentesPadroes  v 
                        join 
                            cgi.Item2 l
                            on l.Empresa  = v.codEmpresa
                            and v.codEmpresa = 1
                            and l.codCor  > 0
                            and '0'||l.codItemPai ||'-0' = v.codProduto
                        where
                            l.Empresa  = 1
        """
        with ConexaoCSW.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                compVar = pd.DataFrame(rows, columns=colunas)
                del rows, colunas
        etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio, 'from componetenstes')

        fp.write('./dados/compVar.parquet', compVar)

        return compVar

