import pandas as pd
import fastparquet as fp
import ConexaoCSW
import requests
import datetime
import pytz

import controle


class ReservaPreFaturamento():
    def __init__(self, empresa,rotina,dataInicio):
        self.empresa = empresa
        self.rotina = rotina
        self.dataInicio = dataInicio

    def conexaoAPIreservaFatCsw(self):
        url = "https://10.162.0.193/api/customci/v10/atualizarSugestaoFaturamento"
        token = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJsdWlzLmZlcm5hbmRvIiwiY3N3VG9rZW4iOiJsU3NVYXNCTyIsImRiTmFtZVNwYWNlIjoiY29uc2lzdGVtIiwiaXNzIjoiYXBpIiwiYXVkIjoi" \
                "YXBpIiwiZXhwIjoxODQ3ODg3Nzg3fQ.xRw6vP87ROIFCs5d-6T5T6LNpUf-bNsX1U2hogrsf2sbLKYKEqPTIVyPgu1YBrhEemgOhSxgEGvfFpIthDb7AQ"

        # Defina os headers
        headers = {
            'accept': 'application/json',
            'empresa': self.empresa,
            'Authorization': f'{token}'
        }

        # Defina os parâmetros em um dicionário
        dataframe = self.StatusSugestaoPedidos()
        pedido = ','.join(dataframe['codPedido'].astype(str))

        params = {
            'pedido': f'{pedido}'
        }

        # Faça a requisição POST com parâmetros e headers usando o método requests.post()
        response = requests.post(url, headers=headers, params=params, verify=False)

        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            # Converter os dados JSON em um dicionário
            dados_dict = response.json()

            # Criar o DataFrame
            df = pd.json_normalize(dados_dict)
            df_exploded = pd.DataFrame({
                'pedidoCompleto': df['pedidoCompleto'].explode().reset_index(drop=True),
                'pedidoIncompleto': df['pedidoIncompleto'].explode().reset_index(drop=True)
            })
            coluna1 = pd.DataFrame(df_exploded['pedidoCompleto'])
            coluna1['situacao Pedido'] = 'completo'
            coluna1.rename(columns={'pedidoCompleto': 'codPedido'}, inplace=True)
            coluna2 = pd.DataFrame(df_exploded['pedidoIncompleto'])
            coluna2['situacao Pedido'] = 'Incompleto'
            coluna2.rename(columns={'pedidoIncompleto': 'codPedido'}, inplace=True)

            concatenar = pd.concat([coluna1, coluna2])

            concatenar = pd.merge(dataframe, concatenar, on='codPedido', how='left')

            Atualizado = self.obterHoraAtual()

            concatenar['Atualizado'] = Atualizado

            etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio,
                                                  f'resposta da api {response.status_code}')

            return response.status_code


        else:
            etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio,
                                                  f'resposta da api {response.status_code}')
            return response.status_code

    def StatusSugestaoPedidos(self):
        # pedidos = APIAtualizaPreFaturamento()
        conn = ConexaoCSW.Conexao()

        entrega = pd.read_sql(self.ObtendoEmbarqueUnico(), conn)
        pedidos = pd.read_sql(self.CapaSugestoes(), conn)
        condicoespgto = pd.read_sql(self.CondicoesDePGTO(), conn)  # codCondVenda
        condicoespgto['codCondVenda'] = condicoespgto['codCondVenda'].astype(str)
        pedidos['codCondVenda'] = pedidos['codCondVenda'].astype(str)

        # Buscando os faturamentos das sugestoes

        faturamentos = pd.read_sql(self.BuscarFaturamentoSugestoes(), conn)
        faturamentos['entregas_realiadas'] = 1
        faturamentos = faturamentos.groupby(['codPedido']).agg({
            'dataFaturamento': 'max',
            'entregas_realiadas': 'count'
        }).reset_index()
        pedidos = pd.merge(pedidos, faturamentos, on='codPedido', how='left')
        pedidos['entregas_realiadas'].fillna(0, inplace=True)

        sugestaoItem = pd.read_sql(self.SugestaoItemAberto(), conn)
        sugestaoItem = sugestaoItem.groupby(['codPedido']).agg({
            'qtdeSugerida': 'sum'
        }).reset_index()

        pedidos = pd.merge(pedidos, sugestaoItem, on=['codPedido'], how='left')

        conn.close()

        pedidos = pd.merge(pedidos, entrega, on='codPedido', how='left')
        pedidos = pd.merge(pedidos, condicoespgto, on='codCondVenda', how='left')
        pedidos.fillna('', inplace=True)
        pedidos['descricao'] = pedidos['codCondVenda'] + '-' + pedidos['descricao']
        # pedidos = pd.merge(pedidos,capaSugestao,on='codPedido',how='left')
        pedidos['prioridadeReserva'] = '-'
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['descricao'], row['prioridadeReserva'], '1 -A VISTA ANTECIPADO',
                                          'Antecipado'), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['descricao'], row['prioridadeReserva'], '2 - CARTAO', 'CART'), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['entregas_Solicitadas'], row['prioridadeReserva'], '3 - EMBARQUE UNICO',
                                          1.0), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['entregas_Solicitadas'], row['prioridadeReserva'], '3 - EMBARQUE UNICO',
                                          '-'), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['entregas_Solicitadas'], row['prioridadeReserva'], '3 - EMBARQUE UNICO',
                                          '-'), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['codTipoNota'], row['prioridadeReserva'], '4 - MPLUS', '39-'), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['descricao'], row['prioridadeReserva'], '5 - OUTROS', 'None-'), axis=1)
        pedidos['prioridadeReserva'] = pedidos.apply(
            lambda row: self.VerificaACondicao(row['descricao'], row['prioridadeReserva'], '5 - OUTROS', ' DD'), axis=1)

        pedidos = pedidos.sort_values(by=['prioridadeReserva'],
                                      ascending=True)  # escolher como deseja classificar

        PedidosItemGradeSugestao_ = self.PedidosItemGradeSugestao()
        pedidos = pd.merge(pedidos, PedidosItemGradeSugestao_, on='codPedido', how='left')

        pedidos.fillna('-', inplace=True)

        return pedidos

    def ObtendoEmbarqueUnico(self):
        df_Entregas_Solicitadas = """select top 100000 
                                             CAST(codPedido as varchar) as codPedido, 
                                             numeroEntrega as entregas_Solicitadas from asgo_ped.Entregas where 
                                             codEmpresa = 1  order by codPedido desc"""
        return df_Entregas_Solicitadas

    def CapaSugestoes(self):
        consulta = """SELECT s.codPedido, p.codCondVenda, p.codTipoNota, p.codCliente, 
    (SELECT  c.nome FROM fat.Cliente c where c.codempresa = 1 and c.codCliente = p.codCliente) as nomeCliente,
    (SELECT  c.NOMEESTADO FROM fat.Cliente c where c.codempresa = 1 and c.codCliente = p.codCliente) as UF,
    (SELECT  c.fantasia FROM fat.Cliente c where c.codempresa = 1 and c.codCliente = p.codCliente) as nomeFantasia
    from ped.SugestaoPed s 
                                join ped.Pedido  p on  p.codEmpresa = s.codEmpresa and p.codPedido = s.codPedido  
                                where p.codEmpresa = 1 and s.situacaoSugestao = 0"""
        return consulta

    def CondicoesDePGTO(self):
        consulta = """SELECT C.codigo as codCondVenda , C.descricao  FROM CAD.CondicaoDeVenda C WHERE C.codEmpresa = 1"""
        return consulta

    def BuscarFaturamentoSugestoes(self):
        consulta = """SELECT n.codPedido, n.dataFaturamento  FROM fat.NotaFiscal n
    WHERE n.codEmpresa = 1 
    and n.codPedido > 0
    and n.codPedido in (SELECT s.codpedido from ped.SugestaoPed s WHERE s.codempresa =1 )"""

        return consulta

    def SugestaoItemAberto(self):
        consulta = """SELECT p.codPedido , p.produto as codProduto , p.qtdeSugerida , p.qtdePecasConf  FROM ped.SugestaoPedItem p
    WHERE p.codEmpresa = 1"""

        return consulta

    def VerificaACondicao(self, descricaoPagto,prioridade, retorno, refe):

        refe = str(refe)
        descricaoPagto = str(descricaoPagto)
        if prioridade == '-' and refe in descricaoPagto:
            return retorno
        else:
            retorno = prioridade
            return retorno

    def PedidosItemGradeSugestao(self):
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        consultar = parquet_file.to_pandas()
        consultar['qtdeSugerida'].fillna(0, inplace=True)

        consultar["Pçaberto"] = consultar['qtdePedida'] - consultar['qtdeCancelada'] - consultar['qtdeFaturada']
        consultar["QtdePedida"] = consultar['qtdePedida'] - consultar['qtdeCancelada']
        consultar = consultar.groupby(['codPedido']).agg({
            'Pçaberto': 'sum',
            'QtdePedida': 'sum'

        }).reset_index()

        return consultar

    def obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        agora = agora.strftime('%d/%m/%Y %H:%M:%S')
        return agora



