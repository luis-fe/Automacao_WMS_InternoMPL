import pandas as pd

import ConexaoCSW
import ConexaoPostgreMPL
import controle


class OpsSubstitutas():
    '''Automacao que impulta no WMS os dados referente as OP que ocorreram substituicao nos utimos 100 dias'''
    def __init__(self, dataInicio, rotina = None, empresa = None):
        self.dataInicio = dataInicio
        self.rotina = rotina
        self.empresa = empresa

    def SubstitutosSkuOP(self):
        with ConexaoCSW.Conexao() as conn:
            # Consultando Sql Obter os itens substitutos dos ultimos 100 dias
            consultaSubstitudos = pd.read_sql(self.RegistroSubstituto(), conn)
            consultaCor = pd.read_sql(self.ConsultaCOr(), conn)
            consultaPad = pd.read_sql(self.ComponentesPadraoEng(), conn)

        etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio,
                                              'etapa busca no CSW: registro de substitutos')

        # Acrescentando o codigoSortimento aos compontentes variaveis
        consultaSortVar = self.ComponentesPrincipalPorSKU()
        consultaSortVar['tipo'] = 'Variavel'
        consultaSubstitudos = pd.merge(consultaSubstitudos, consultaSortVar, on=['codproduto', 'componente'],
                                       how='left')
        consultaSubstitudos['tipo'].fillna('Padrao', inplace=True)  # Caso vazio, marcar como componente padrao
        etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1,
                                              'etapa CSW: Acrescentando o codigoSortimento aos compontentes variaveis')

        # etapa 3 Acrescentando as cores aos componentes variaveis
        consultaCor['tipo'] = 'Variavel'
        consultaCor['codSortimento'] = consultaCor['codSortimento'].astype(str)
        consultaSubstitudos = pd.merge(consultaSubstitudos, consultaCor, on=['numeroop', 'codSortimento', 'tipo'],
                                       how='left')
        etapa3 = controle.salvarStatus_Etapa3(self.rotina, 'automacao', etapa2,
                                              'etapa CSW: Acrescentando as cores aos componentes variaveis')

        # Levantamento dos compontentes padroes
        consultaPad['tipo'] = 'Padrao'
        consultaSubstitudosPad = consultaSubstitudos
        consultaSubstitudosPad = consultaSubstitudosPad[consultaSubstitudosPad['tipo'] == 'Padrao']
        consultaSubstitudosPad.drop(['codSortimento', 'cor'], axis=1, inplace=True)

        consultaSubstitudosPad = pd.merge(consultaSubstitudosPad, consultaPad, on=['codproduto', 'componente', 'tipo'],
                                          how='left')
        etapa4 = controle.salvarStatus_Etapa4(self.rotina, 'automacao', etapa3,
                                              'etapa CSW: Levantamento dos compontentes padroes')


        # Acrescentando as cores aos componentes padroes
        consultaCor['tipo'] = 'Padrao'
        consultaSubstitudosPad = pd.merge(consultaSubstitudosPad, consultaCor, on=['numeroop', 'tipo'], how='left')

        consultaSubstitudos = consultaSubstitudos[consultaSubstitudos['tipo'] == 'Variavel']
        consulta = pd.concat([consultaSubstitudos, consultaSubstitudosPad], ignore_index=True)

        consulta['aplicacaoPad'].fillna('-', inplace=True)
        consulta['aplicacao'] = consulta.apply(
            lambda row: row['aplicacaoPad'] if row['aplicacaoPad'] != '-' else row['aplicacao'], axis=True)
        # acrescentando as categorias
        consulta['categoria'] = '-'
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ZIPER', row['nomecompontente'], 'ZIPER', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ENTRETELA', row['nomecompontente'], 'ENTRETELA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('RIBANA', row['nomecompontente'], 'RIBANA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ENTRETELA', row['nomecompontente'], 'ENTRETELA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('GOLA', row['nomecompontente'], 'GOLAS', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('KIT GOLA', row['nomecompontente'], 'KIT GOLA/PUNHO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('MALHA', row['nomecompontente'], 'MALHA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('TECIDO', row['nomecompontente'], 'TECIDO PLANO', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BOTAO DE POLIESTER', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BOTAO DE MASSA', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BOTAO', row['nomecompontente'], 'BOTAO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('MOLET', row['nomecompontente'], 'MOLETOM', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ETIQUETA DE TAMANHO', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ETIQUETA TAMANHO', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ETIQUETA', row['nomecompontente'], 'ETIQUETAS', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('CADAR', row['nomecompontente'], 'CADARCO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('ELAST', row['nomecompontente'], 'ELASTICOS', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('COLARINHO PARA CAMISA', row['nomecompontente'], 'RETIRAR', row['categoria']), axis=1)

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('COLARIN', row['nomecompontente'], 'AV CAMISARIAS', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('PUNHO', row['nomecompontente'], 'PUNHO', row['categoria']), axis=1)
        # colunas carragadas: requisicao, numeroop, codproduto, databaixa_req, componente, nomecompontente, subst, nomesub

        consulta['exessao'] = '-'
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101846', '250101051', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101918', '250101717', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250301842', '250301406', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101409', '250101836', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250301842', '250301309', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101842', '250101912', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250100009', '250100923', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101859', '250100214', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)
        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250198000', '250101912', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)

        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250198000', '250101848', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)

        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101848', '250198000', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)

        consulta['exessao'] = consulta.apply(
            lambda row: self.Excessoes('250101842', '250101848', row['coodigoPrincipal'], row['coodigoSubs'],
                                  row['exessao']), axis=1)

        ultimobackup = self.ConsultaSubstitutosFlegadoSim()
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


        registro = self.RegistroSubstituto()
        if not registro.empty:
            consulta = pd.merge(consulta,registro,on=['numeroop','cor'],how='left')
            consulta.fillna('-',inplace=True)
            consulta['considera'] = consulta['consideracao']

        # Carregando dados no Wms
        ConexaoPostgreMPL.Funcao_InserirMatriz(consulta, consulta['requisicao'].size, 'SubstitutosSkuOP', 'replace')
        etapa5 = controle.salvarStatus_Etapa5(self.rotina, 'automacao', etapa4, 'Carregando dados no Wms')

    def RegistroSubstituto(self):
        registro = "SELECT s.codRequisicao as requisicao , r.numOPConfec as numeroop ," \
                   " (SELECT op.codProduto from tco.OrdemProd op WHERE op.codempresa = 1 and op.numeroop = r.numOPConfec ) as codproduto," \
                   " r.dtBaixa as databaixa_req, s.codItemPrincipal as componente, ri.nomeMaterial as nomecompontente, s.codMaterial as subst," \
                   " (select ri2.nomeMaterial from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codMaterial)" \
                   " as nomesub," \
                   " (select ri2.codmaterialedt from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codMaterial)" \
                   " as coodigoSubs, " \
                   "(select ri2.codmaterialedt from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codItemPrincipal) as coodigoPrincipal" \
                   " FROM TCQ.Requisicao R" \
                   " inner join tcq.RequisicaoItemSubst s on s.codEmpresa = r.codEmpresa and s.codRequisicao = r.numero" \
                   " left join tcq.RequisicaoItem ri on s.codEmpresa = ri.codEmpresa and s.codRequisicao = ri.codRequisicao  and ri.codMaterial = s.codItemPrincipal " \
                   " WHERE R.codEmpresa = "+ self.empresa+ "and r.dtBaixa  > DATEADD('day', -100, CURRENT_TIMESTAMP)"
        return registro

    def ConsultaCOr(self):
        consulta = " SELECT top 80000 " \
                   " op.numeroOP as numeroop , op.codSortimento, s.corbase||'-'||s.nomeCorBase as cor  FROM tco.OrdemProdGrades op" \
                   " inner join tcp.SortimentosProduto s on s.codEmpresa = op.codEmpresa and s.codProduto = op.codProduto and s.codSortimento = op.codSortimento " \
                   " WHERE op.codEmpresa = "+ self.empresa +" and op.codproduto like '%-0'" \
                   " order by numeroOP desc"
        return consulta

    def ComponentesPadraoEng(self):

        if self.empresa == '4':
            consulta = "SELECT c.CodComponente as componente, c.codProduto as codproduto, codaplicacao as aplicacaoPad  FROM tcp.ComponentesPadroes  c" \
                       ' WHERE c.codEmpresa = '+self.empresa +' and c.codProduto in (' \
                       ' SELECT top 10000 op.codproduto from tco.OrdemProd op WHERE op.codempresa =  '+self.empresa+ \
                       ' order by numeroOP desc) and c.CodComponente in (' \
                       ' SELECT s.codItemPrincipal from tcq.RequisicaoItemSubst s WHERE s.codempresa = '+self.empresa+ \
                       " ) and c.codproduto like '03%'"
        else:
            consulta = "SELECT c.CodComponente as componente, c.codProduto as codproduto, codaplicacao as aplicacaoPad  FROM tcp.ComponentesPadroes  c" \
                       ' WHERE c.codEmpresa = '+self.empresa +' and c.codProduto in (' \
                       ' SELECT top 10000 op.codproduto from tco.OrdemProd op WHERE op.codempresa =  '+self.empresa+ \
                       ' order by numeroOP desc) and c.CodComponente in (' \
                       ' SELECT s.codItemPrincipal from tcq.RequisicaoItemSubst s WHERE s.codempresa = '+self.empresa+ \
                       " ) and c.codproduto like '01%'"


        return consulta

    def ComponentesPrincipalPorSKU(self):
        with ConexaoCSW.Conexao() as conn:
            # Consultando Sql Obter os skus x compontente principal para poder "ligar" ao calculos
            consulta = pd.read_sql(self.ComponentesPrincipaisEngenharia(), conn)

        # Dividir os valores da coluna 2 por ";"
        consulta['codSortimento'] = consulta['codSortimento'].str.split(';')

        # Usar explode para expandir os valores em múltiplas linhas
        consulta = consulta.explode('codSortimento')

        return consulta

    def ComponentesPrincipaisEngenharia(self):
        consulta = 'SELECT c.CodComponente as componente , c.codSortimento, c.codProduto as codproduto, codAplicacao as aplicacao FROM tcp.ComponentesVariaveis c' \
                   ' WHERE c.codEmpresa = '+self.empresa +' and c.codProduto in (' \
                   ' SELECT top 10000 op.codproduto from tco.OrdemProd op WHERE op.codempresa = '+self.empresa + \
                   ' order by numeroOP desc) and c.CodComponente in (' \
                   ' SELECT s.codItemPrincipal from tcq.RequisicaoItemSubst s WHERE s.codempresa = '+self.empresa + \
                   " ) "
        return consulta

    def Categoria(self,contem, valorReferencia, valorNovo, categoria):
        if contem in valorReferencia and categoria == '-':
            return valorNovo
        else:
            return categoria

    def Excessoes(self, Pai, SubstFilho, codigoPrincipal, codigosubst, exessao):
        if (SubstFilho in codigosubst) and exessao == '-' and (Pai in codigoPrincipal):
            TerminacaoSubt = codigosubst[9:]
            TerminacaoPAi = codigoPrincipal[9:]

            if TerminacaoSubt == TerminacaoPAi:
                return 'acessorios'
            else:
                return '-'
        else:
            return exessao

    def ConsultaSubstitutosFlegadoSim(self):
        conn = ConexaoPostgreMPL.conexaoMatriz()

        consulta = pd.read_sql('select numeroop, componente, considera from "Reposicao"."SubstitutosSkuOP" '
                               'where considera = %s ', conn, params=('sim',))

        conn.close()

        return consulta

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

            consulta['epc'] = consulta['epc'].str.split('||').str[1]
            consulta['epc'] = consulta['epc'].fillna('')


            ConexaoPostgreMPL.Funcao_InserirMatriz(consulta, consulta['numeroOP'].size, 'opsEmTerceiros', 'replace')


    def consultarRegistroSubs(self):
        '''Metodo utilizado para consultar o registro de substituto'''

        conn = ConexaoPostgreMPL.conexaoMatriz()
        sql = '''
            select distinct numeroop, cor, 'sim' as consideracao where 'Reposicao'."RegistroSubstituto" where empresa = %s
        '''

        consulta = pd.read_sql(sql, conn, params=(self.empresa,))

        conn.close()

        return consulta
