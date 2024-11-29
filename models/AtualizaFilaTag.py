import pandas as pd
import ConexaoCSW
import ConexaoPostgreMPL
import controle
import numpy
import pytz
import datetime

class AtualizaFilaTag():
    '''Classe desenvolvida para atalizar as tags da fila, consultando o CSW'''

    def __init__(self, empresa, frequenciaAtualizacao, rotina = '', datainicio = ''):
        self.empresa = str(empresa)
        self.frequenciaAtualizacao = frequenciaAtualizacao
        self.rotina = rotina
        self.datainico = datainicio


        if self.empresa == '1':
            self.conn = ConexaoPostgreMPL.conexaoEngineMatriz()

        if self.empresa == '4':
            self.conn = ConexaoPostgreMPL.conexaoEngine()


    def consultaTagsEmEstoqueCSW(self):
        '''Funcao utilizada para acessar o erp CSW , Obter as tags e as transferir para o WMS'''
        xemp = "'" + self.empresa + "'"

        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                meuSql = """
                        SELECT 
                            codBarrasTag AS codbarrastag, 
                            codNaturezaAtual AS codnaturezaatual, 
                            codEngenharia, 
                            codReduzido AS codreduzido,
                            (SELECT i.nome FROM cgi.Item i WHERE i.codigo = t.codReduzido) AS descricao, 
                            numeroop AS numeroop,
                            (SELECT i2.codCor || '-' 
                             FROM cgi.Item2 i2 
                             WHERE i2.Empresa = 1 AND i2.codItem = t.codReduzido) || 
                            (SELECT i2.descricao 
                             FROM tcp.SortimentosProduto i2 
                             WHERE i2.codEmpresa = 1 AND i2.codProduto = t.codEngenharia AND t.codSortimento = i2.codSortimento) AS cor,
                            (SELECT tam.descricao 
                             FROM cgi.Item2 i2 
                             JOIN tcp.Tamanhos tam ON tam.codEmpresa = i2.Empresa AND tam.sequencia = i2.codSeqTamanho 
                             WHERE i2.Empresa = 1 AND i2.codItem = t.codReduzido) AS tamanho, 
                            codEmpresa AS codempresa 
                        FROM 
                            tcr.TagBarrasProduto t 
                        WHERE 
                            codEmpresa = %s AND 
                            codNaturezaAtual IN (5, 7, 54) AND 
                            situacao IN (3, 8)
                        """ % xemp

                cursor_csw.execute(meuSql)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                df_tags = pd.DataFrame(rows, columns=colunas)

                return df_tags

    def consultaOPEntradaEstoqueCsw(self):
                '''Metodo utilizado para contabilizar as ordem de producao produzida  nos ultimos 200Mil registro '''
                xemp = "'" + self.empresa + "'"

                meuSql2 = """SELECT 
                                top 200000 numeroOP as numeroop , 
                                totPecasOPBaixadas as totalop 
                            from 
                                tco.MovimentacaoOPFase 
                            WHERE 
                                codEmpresa = """ + xemp + """
                                and codFase im (236, 449, 885, 643, 654) 
                                order by numeroOP desc """

                with ConexaoCSW.Conexao() as conn:
                    with conn.cursor() as cursor_csw:

                        cursor_csw.execute(meuSql2)
                        colunas = [desc[0] for desc in cursor_csw.description]
                        # Busca todos os dados
                        rows = cursor_csw.fetchall()
                        # Cria o DataFrame com as colunas
                        df_opstotal = pd.DataFrame(rows, columns=colunas)

                return df_opstotal


    def LerEPC(self):
        '''Funcao utilizada para obter o EPC das tags'''
        xemp = self.empresa
        xemp = "'"+xemp+"'"
        with ConexaoCSW.Conexao() as conn:

            consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                               'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                               'WHERE t.codEmpresa = '+xemp+' and (t.codTransacao = 3500 or t.codTransacao = 501) '
                               'and (codLote like "23%" or  codLote like "24%" or codLote like "25%" '
                               ' or codLote like "26%" or codLote like "27%" or codLote like "28%" or codLote like "29%" or codLote like "3%" )',conn)

        return consulta


    def recarregarFilaTags(self):

        conn = self.conn

        etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.datainico, 'from tcr.TagBarrasProduto t')

        etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1, 'from tco.MovimentacaoOPFase ')

        df_tags = self.consultaTagsEmEstoqueCSW()
        df_opstotal = self.consultaOPEntradaEstoqueCsw()

        df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
        df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
        df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
        df_tags['totalop'] = df_tags['totalop'].astype(int)

        # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
        # Verificando as tag's que ja foram repostas
        TagsRepostas = pd.read_sql(
            'select "codbarrastag" as codbarrastag, "usuario" as usuario_  from "Reposicao"."tagsreposicao" tr '
            ' union select "codbarrastag" as codbarrastag, "usuario" as usuario_ from "Reposicao"."Reposicao".tagsreposicao_inventario ti ',
            conn)

        df_tags = pd.merge(df_tags, TagsRepostas, on='codbarrastag', how='left')
        df_tags = df_tags.loc[df_tags['usuario_'].isnull()]
        df_tags.drop('usuario_', axis=1, inplace=True)
        etapa3 = controle.salvarStatus_Etapa3(self.rotina, 'automacao', etapa2, 'WMS: "Reposicao"."tagsreposicao"   ')

        # Verificando as tag's que ja estam na fila
        ESTOQUE = pd.read_sql(
            'select "usuario", "codbarrastag" as codbarrastag, "Situacao" as sti_aterior  from "Reposicao"."filareposicaoportag" ',
            conn)
        df_tags = pd.merge(df_tags, ESTOQUE, on='codbarrastag', how='left')
        df_tags['Situacao'] = df_tags.apply(
            lambda row: 'Reposto' if not pd.isnull(row['usuario']) else 'Reposição não Iniciada', axis=1)
        etapa4 = controle.salvarStatus_Etapa4(self.rotina, 'automacao', etapa3, 'WMS: "Reposicao"."filareposicaoportag"   ')

        epc = self.LerEPC()
        etapa5 = controle.salvarStatus_Etapa5(self.rotina, 'automacao', etapa4, 'csw: ler os EPCS  ')

        df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
        df_tags.rename(columns={'codbarrastag': 'codbarrastag', 'codEngenharia': 'engenharia'
            , 'numeroop': 'numeroop'}, inplace=True)
        conn.close()
        df_tags = df_tags.loc[df_tags['sti_aterior'].isnull()]
        df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
        # Excluir a coluna 'B' inplace
        df_tags.drop('sti_aterior', axis=1, inplace=True)
        df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
        df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
        tamanho = df_tags['codbarrastag'].size
        dataHora = self.obterHoraAtual()
        df_tags['DataHora'] = dataHora

        if self.empresa == '1':
            restringe = self.BuscaResticaoSubstitutos()
            df_tags = pd.merge(df_tags, restringe, on=['numeroop', 'cor'], how='left')
            df_tags['resticao'].fillna('-', inplace=True)
            df_tags['considera'].fillna('-', inplace=True)
        else:
            print('empresa 4')

        # try:
        tamanho2 = 1000
        ConexaoPostgreMPL.Funcao_Inserir(df_tags, tamanho2, 'filareposicaoportag', 'append')

    def BuscaResticaoSubstitutos(self):
            conn = ConexaoPostgreMPL.conexao()

            consulta = pd.read_sql("select numeroop , codproduto||'||'||numeroop  as resticao,  "
                                   'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                                   "sso where sso.considera = 'sim'", conn)
            conn.close()
            return consulta

    def obterHoraAtual(self):
        '''Funcao utilizada para obter o valor da Data e Hora Atual'''

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%d/%m/%Y %H:%M')
        return hora_str



