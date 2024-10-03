import datetime
import numpy
import pandas as pd
from psycopg2 import sql
import pytz
import ConexaoCSW
import ConexaoPostgreMPL
import controle
import empresaConfigurada


class AutomacaoFilaTags():
    '''
    classe feita para Automatizar o processo de recarregar as Tags no WMS
    '''
    def __init__(self, empresa = None):
        '''Contrutor definindo a empresa
        :arg
        self.empesa : o Codigo da empresa em str da qual estamos acessando a automacao, seguindo do IF para caso "NONE" buscar da empresa parametrizada.
        '''
        if empresa != None:
            self.empresa = empresa
        else:
            self.empresa  = empresaConfigurada.EmpresaEscolhida()

    def obterHoraAtual(self):
        '''Funcao utilizada para obter o valor da Data e Hora Atual'''

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%d/%m/%Y %H:%M')
        return hora_str

    def recarregarTags(self,rotina, datainico):
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

                meuSql2 = """SELECT top 200000 numeroOP as numeroop , totPecasOPBaixadas as totalop from tco.MovimentacaoOPFase WHERE codEmpresa = """ + xemp + """and codFase = 236 
                  order by numeroOP desc """
                cursor_csw.execute(meuSql2)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                df_opstotal = pd.DataFrame(rows, columns=colunas)

        conn2 = ConexaoPostgreMPL.conexao()

        etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao', datainico, 'from tcr.TagBarrasProduto t')

        etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao', etapa1, 'from tco.MovimentacaoOPFase ')

        df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
        df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
        df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
        df_tags['totalop'] = df_tags['totalop'].astype(int)

        # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
        # Verificando as tag's que ja foram repostas
        TagsRepostas = pd.read_sql(
            'select "codbarrastag" as codbarrastag, "usuario" as usuario_  from "Reposicao"."tagsreposicao" tr '
            ' union select "codbarrastag" as codbarrastag, "usuario" as usuario_ from "Reposicao"."Reposicao".tagsreposicao_inventario ti ',
            conn2)

        df_tags = pd.merge(df_tags, TagsRepostas, on='codbarrastag', how='left')
        df_tags = df_tags.loc[df_tags['usuario_'].isnull()]
        df_tags.drop('usuario_', axis=1, inplace=True)
        etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao', etapa2, 'WMS: "Reposicao"."tagsreposicao"   ')

        # Verificando as tag's que ja estam na fila
        ESTOQUE = pd.read_sql(
            'select "usuario", "codbarrastag" as codbarrastag, "Situacao" as sti_aterior  from "Reposicao"."filareposicaoportag" ',
            conn2)
        df_tags = pd.merge(df_tags, ESTOQUE, on='codbarrastag', how='left')
        df_tags['Situacao'] = df_tags.apply(
            lambda row: 'Reposto' if not pd.isnull(row['usuario']) else 'Reposição não Iniciada', axis=1)
        etapa4 = controle.salvarStatus_Etapa4(rotina, 'automacao', etapa3, 'WMS: "Reposicao"."filareposicaoportag"   ')

        epc = self.LerEPC()
        etapa5 = controle.salvarStatus_Etapa5(rotina, 'automacao', etapa4, 'csw: ler os EPCS  ')

        df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
        df_tags.rename(columns={'codbarrastag': 'codbarrastag', 'codEngenharia': 'engenharia'
            , 'numeroop': 'numeroop'}, inplace=True)
        conn2.close()
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


    def LerEPC(self):
        '''Funcao utilizada para obter o EPC das tags'''
        xemp = empresaConfigurada.EmpresaEscolhida()
        xemp = "'"+xemp+"'"
        with ConexaoCSW.Conexao() as conn:

            consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                               'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                               'WHERE t.codEmpresa = '+xemp+' and (t.codTransacao = 3500 or t.codTransacao = 501) '
                               'and (codLote like "23%" or  codLote like "24%" or codLote like "25%" '
                               ' or codLote like "26%" or codLote like "27%" or codLote like "28%" or codLote like "29%" or codLote like "3%" )',conn)

        return consulta
    def avaliacaoFila(self,rotina,datahoraInicio):
        '''funcao utilizada para avalaiar a fila, verificando se existe tag que ja foi faturada na fila '''
        xemp = empresaConfigurada.EmpresaEscolhida()
        xemp = "'"+xemp+"'"
        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:

                sql1 = """select br.codBarrasTag as codbarrastag , 'estoque' as estoque  from Tcr.TagBarrasProduto br 
        WHERE br.codEmpresa in (%s) and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54, 8) """%xemp
                cursor_csw.execute(sql1)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                SugestoesAbertos = pd.DataFrame(rows, columns=colunas)



        etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datahoraInicio,'etapa csw Tcr.TagBarrasProduto p')

        conn2 = ConexaoPostgreMPL.conexao()

        tagWms = pd.read_sql('select * from "Reposicao".filareposicaoportag t ', conn2)
        tagWms2 = pd.read_sql('select * from "Reposicao".tagsreposicao t ', conn2)

        tagWms = pd.merge(tagWms,SugestoesAbertos, on='codbarrastag', how='left')
        tagWms2 = pd.merge(tagWms2,SugestoesAbertos, on='codbarrastag', how='left')

        etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa merge filatagsWMS+tagsProdutoCSW')


        tagWms = tagWms[tagWms['estoque']!='estoque']
        tagWms2 = tagWms2[tagWms2['estoque']!='estoque']


        tamanho =tagWms['codbarrastag'].size
        tamanho2 =tagWms2['codbarrastag'].size

        # Obter os valores para a cláusula WHERE do DataFrame
        lista = tagWms['codbarrastag'].tolist()
        lista2 = tagWms2['codbarrastag'].tolist()

        # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame


        #bACKUP DAS TAGS QUE TIVERAM SAIDA FORA DO WMS NA FILA

        if tamanho != 0:
            query = sql.SQL('DELETE FROM "Reposicao"."filareposicaoportag" WHERE codbarrastag IN ({})').format(
                sql.SQL(',').join(map(sql.Literal, lista))
            )

            query2 = sql.SQL('DELETE FROM "Reposicao"."tagsreposicao" WHERE codbarrastag IN ({})').format(
                sql.SQL(',').join(map(sql.Literal, lista2))
            )

            # Executar a consulta DELETE
            with conn2.cursor() as cursor:
                cursor.execute(query)
                conn2.commit()
                cursor.execute(query2)
                conn2.commit()

        else:
            print('2.1.1 sem tags para ser eliminadas na Fila Tags Reposicao')
        etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa2,'deletando saidas fora do WMS')



    def BuscaResticaoSubstitutos(self):
        conn = ConexaoPostgreMPL.conexao()

        consulta = pd.read_sql("select numeroop , codproduto||'||'||numeroop  as resticao,  "
                                'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                               "sso where sso.considera = 'sim'",conn)
        conn.close()
        return consulta


    def atualizarEmpresa(self):
        update = """
        update
	"Reposicao"."Reposicao".tagsreposicao
set
	codempresa = '4'
where
	codempresa is null
        """
        conn2 = ConexaoPostgreMPL.conexao()


        with conn2.cursor() as cursor:
            cursor.execute(update)
            conn2.commit()


        conn2.close()


