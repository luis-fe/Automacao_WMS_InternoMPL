import pandas as pd
import ConexaoCSW
import ConexaoPostgreMPL
import controle
from models import Ops_CSW, Produto_CSW


class Ops_Itens_Substituidos():
    '''Automacao que impulta no WMS os dados referente às OP's que ocorreram processo de substituicao de itens  nos utimos n dias'''

    def __init__(self, dataInicio, rotina=None, empresa='1'):
        '''Contrutor da classe'''

        self.dataInicio = dataInicio
        self.rotina = rotina
        self.empresa = empresa

    def substitutosSkuOP(self):
        '''Metodo que pesquisa no ERP CSW os sub'''

        # 1 - Instanciando objetos das classes  Ops_CSW, Produto_CSW
        ops_Csw = Ops_CSW.Ops_CSW(self.dataInicio, self.rotina, self.empresa, 100)
        produto_CSW = Produto_CSW.Produto_CSW()

        # 2 - Consultando Sql Obter os itens substitutos dos ultimos 100 dias
        consultaSubstitudos = ops_Csw.registroSubstituto_porReq()  # 2.1 Filtra os registros de substitutos de OP no Csw , "return DataFrame": numeroop, requisicao, codproduto, componente ...

        consultaSortVar = produto_CSW.componentesVariaveis_Subs_Eng()  # 2.2 - Filtra o cadastro de Engenharia x componente , "return DataFrame" : componente, codproduto , codSortimento, aplicacao
        consultaSortVar[
            'tipo'] = 'Variavel'  # 2.2.1 - Acrescentando ao DataFrame a coluna Tipo (identificando que trata-se de componentes variaveis.

        consultaCor = ops_Csw.consulta_OP_porCorSortimentoCSW()  # 2.3 - return DataFrame: numeroop, codSortimento, cor
        consultaCor['tipo'] = 'Variavel'
        consultaCor['codSortimento'] = consultaCor['codSortimento'].astype(str)

        consultaPad = produto_CSW.componentesPadroes_Subs_Eng()
        consultaPad['tipo'] = 'Padrao'

        etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio,
                                              'etapa busca no CSW: registro de substitutos')

        # 3 Acrescentando o codigoSortimento aos compontentes variaveis
        consultaSubstitudos = pd.merge(consultaSubstitudos, consultaSortVar, on=['codproduto', 'componente'],
                                       how='left')
        consultaSubstitudos['tipo'].fillna('Padrao',
                                           inplace=True)  # 3.1 Caso no merge nao identifique o tipo Variavel, substitui a informacao para tipo "Padrao" que será reanalizado junto ao DataFrame consultaPad.

        etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1,
                                              'etapa CSW: Acrescentando o codigoSortimento aos compontentes variaveis')

        # 4 Acrescentando as cores aos componentes variaveis

        consultaSubstitudos = pd.merge(consultaSubstitudos, consultaCor, on=['numeroop', 'codSortimento', 'tipo'],
                                       how='left')
        etapa3 = controle.salvarStatus_Etapa3(self.rotina, 'automacao', etapa2,
                                              'etapa CSW: Acrescentando as cores aos componentes variaveis')

        # 5 Levantamento dos compontentes que sao "padrao" para criar sortimentos e cores
        consultaSubstitudosPad = consultaSubstitudos
        consultaSubstitudosPad = consultaSubstitudosPad[consultaSubstitudosPad['tipo'] == 'Padrao']
        consultaSubstitudosPad.drop(['codSortimento', 'cor'], axis=1, inplace=True)

        consultaSubstitudosPad = pd.merge(consultaSubstitudosPad, consultaPad, on=['codproduto', 'componente', 'tipo'],
                                          how='left')
        etapa4 = controle.salvarStatus_Etapa4(self.rotina, 'automacao', etapa3,
                                              'etapa CSW: Levantamento dos compontentes padroes')

        # 6 Acrescentando as cores aos componentes padroes
        consultaCor['tipo'] = 'Padrao'
        consultaSubstitudosPad = pd.merge(consultaSubstitudosPad, consultaCor, on=['numeroop', 'tipo'], how='left')
        consultaSubstitudos = consultaSubstitudos[consultaSubstitudos['tipo'] == 'Variavel']

        # 7 Concatenando componentes variaveis + compontentes padroes em um unico dataFrame
        consulta = pd.concat([consultaSubstitudos, consultaSubstitudosPad], ignore_index=True)

        # 8 Acrescentando a aplicacao do componente
        consulta['aplicacaoPad'].fillna('-', inplace=True)
        consulta['aplicacao'] = consulta.apply(
            lambda row: row['aplicacaoPad'] if row['aplicacaoPad'] != '-' else row['aplicacao'], axis=True)

        # acrescentando as categorias
        consulta['categoria'] = '-'
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ZIPER', row['nomecompontente'], 'ZIPER', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ENTRETELA', row['nomecompontente'], 'ENTRETELA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('RIBANA', row['nomecompontente'], 'RIBANA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ENTRETELA', row['nomecompontente'], 'ENTRETELA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('GOLA', row['nomecompontente'], 'GOLAS', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('KIT GOLA', row['nomecompontente'], 'KIT GOLA/PUNHO', row['categoria']),
            axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('MALHA', row['nomecompontente'], 'MALHA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('TECIDO', row['nomecompontente'], 'TECIDO PLANO', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('BOTAO DE POLIESTER', row['nomecompontente'], 'RETIRAR', row['categoria']),
            axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('BOTAO DE MASSA', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('BOTAO', row['nomecompontente'], 'BOTAO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('MOLET', row['nomecompontente'], 'MOLETOM', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ETIQUETA DE TAMANHO', row['nomecompontente'], 'RETIRAR', row['categoria']),
            axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ETIQUETA TAMANHO', row['nomecompontente'], 'RETIRAR', row['categoria']),
            axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ETIQUETA', row['nomecompontente'], 'ETIQUETAS', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('CADAR', row['nomecompontente'], 'CADARCO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('ELAST', row['nomecompontente'], 'ELASTICOS', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('COLARINHO PARA CAMISA', row['nomecompontente'], 'RETIRAR', row['categoria']),
            axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('COLARIN', row['nomecompontente'], 'AV CAMISARIAS', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.__categoria('PUNHO', row['nomecompontente'], 'PUNHO', row['categoria']), axis=1)
        # colunas carragadas: requisicao, numeroop, codproduto, databaixa_req, componente, nomecompontente, subst, nomesub

        consulta['exessao'] = '-'
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101846', '250101051', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101918', '250101717', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250301842', '250301406', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101409', '250101836', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250301842', '250301309', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101842', '250101912', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250100009', '250100923', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101859', '250100214', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250198000', '250101912', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)

        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250198000', '250101848', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)

        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101848', '250198000', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)

        consulta['exessao'] = consulta.apply(
            lambda row: self.__excessoes('250101842', '250101848', row['coodigoPrincipal'], row['coodigoSubs'],
                                         row['exessao']), axis=1)

        # Incluindo os item que ja foram considerados como 'sim'
        ultimobackup = self.__consultaSubstitutosFlegadoSim()
        consulta = pd.merge(consulta, ultimobackup, on=['numeroop', 'componente'], how='left')

        consulta['considera'].fillna('-', inplace=True)
        consulta['tipo'].fillna('variavel', inplace=True)
        consulta['cor'].fillna('-', inplace=True)
        consulta['aplicacao'].fillna('-', inplace=True)

        consulta = consulta[consulta['cor'] != '-']
        consulta = consulta[consulta['categoria'] != 'RETIRAR']
        consulta = consulta[consulta['exessao'] != 'acessorios']

        consulta['id'] = 'req:' + consulta['requisicao'].astype(str) + '||cor:' + consulta['cor'] + '||aplicacao:' + \
                         consulta['aplicacao'] + '||rdzPrinc:' + consulta['componente'] + '||rdzSubst' + consulta[
                             'subst']
        consulta = consulta.drop_duplicates()

        # Verificando os registro de substituicao Ja Informados e marcando "sim" em considera
        registro = self.__consultarRegistroSubs()
        if not registro.empty:
            consulta = pd.merge(consulta, registro, on=['numeroop', 'cor'], how='left')
            consulta.fillna('-', inplace=True)
            consulta['considera'] = consulta['consideracao']

        # Carregando dados no Wms
        ConexaoPostgreMPL.Funcao_InserirPCPMatrizWMS(consulta, consulta['requisicao'].size, 'SubstitutosSkuOP',
                                                     'replace')
        etapa5 = controle.salvarStatus_Etapa5(self.rotina, 'automacao', etapa4, 'Carregando dados no Wms')

    def __categoria(self, contem, valorReferencia, valorNovo, categoria):
        if contem in valorReferencia and categoria == '-':
            return valorNovo
        else:
            return categoria

    def __excessoes(self, Pai, SubstFilho, codigoPrincipal, codigosubst, exessao):
        if (SubstFilho in codigosubst) and exessao == '-' and (Pai in codigoPrincipal):
            TerminacaoSubt = codigosubst[9:]
            TerminacaoPAi = codigoPrincipal[9:]

            if TerminacaoSubt == TerminacaoPAi:
                return 'acessorios'
            else:
                return '-'
        else:
            return exessao

    def __consultaSubstitutosFlegadoSim(self):
        '''Método que consulta no banco Postgres WMS se  o componente está sendo considerado como especial: Prateleira de Controle Substituto '''
        conn = ConexaoPostgreMPL.conexaoMatrizWMS()

        consulta = pd.read_sql('''
                                select 
                                    numeroop, 
                                    componente, 
                                    considera 
                                from 
                                    "Reposicao"."SubstitutosSkuOP" '
                               where 
                                    considera = %s ''',
                               conn, params=('sim',)
                               )
        conn.close()

        return consulta

    '''
        def atualizarEPCFaccoes(self):
                consulta = """
                SELECT E.codBarrasTag , E.numeroOP , e.codSortimento , p.ID as epc, e.codLote, t.descricao  
        FROM tcr.TagBarrasProduto E
        JOIN Tcr_Rfid.NumeroSerieTagEPC p 
        ON p.codTag = E.codBarrasTag 
        join tcp.Tamanhos t 
        on t.codempresa = 1 and t.sequencia  = e.seqTamanho
        WHERE E.codEmpresa = 1 
        AND E.numeroOP in (SELECT numeroop FROM tco.OrdemProd op WHERE op.situacao =3 and op.codfaseatual in (429))
                """

                with ConexaoCSW.Conexao() as conn:
                    with conn.cursor() as cursor_csw:
                        # Executa a primeira consulta e armazena os resultados
                        cursor_csw.execute(consulta)
                        colunas = [desc[0] for desc in cursor_csw.description]
                        rows = cursor_csw.fetchall()
                        consulta = pd.DataFrame(rows, columns=colunas)
                try:
                    consulta['epc'] = consulta['epc'].str.split('||').str[1]
                    consulta['epc'] = consulta['epc'].fillna('')
                except:
                    print('erro no epc')


                ConexaoPostgreMPL.Funcao_InserirMatriz(consulta, consulta['numeroOP'].size, 'opsEmTerceiros', 'replace')
    '''

    def __consultarRegistroSubs(self):
        '''Metodo utilizado para consultar o registro de substituto'''

        conn = ConexaoPostgreMPL.conexaoMatrizWMS()
        sql = '''
            select distinct 
                numeroop, 
                cor, 
                'sim' as consideracao 
            from 
                "Reposicao"."RegistroSubstituto" 
            where 
                empresa = %s
        '''

        consulta = pd.read_sql(sql, conn, params=(self.empresa,))

        conn.close()

        return consulta
