import pandas as pd
import ConexaoCSW
import numpy as np
import ConexaoPostgreMPL
import controle


class Op_AbertoClass():
    '''Classe criada para a capturar as OPs em aberto no Csw a nivel de tamanho e cor e salvar no wms '''
    def __init__(self, empresa, rotina, datainicio):
        self.empresa = empresa
        self.rotina = rotina
        self.dataInicio = datainicio

    def buscandoOPCSW(self):
        '''Funcao que busca as ops no CSW'''

        sqlCswOpsnivelSku = """
                SELECT op.codfaseatual ,ot.codProduto ,ot.numeroop as numeroop , ot.codSortimento , seqTamanho, 
                case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs 
                FROM tco.OrdemProdTamanhos ot
                inner join tco.ordemprod op on op.codempresa = ot.codempresa and op.numeroop = ot.numeroop
                having ot.codEmpresa = """ + self.empresa + """ and ot.numeroOP IN """ + ' (select o.numeroOP  from tco.ordemprod o where o.situacao = 3 and o.codempresa = ' + self.empresa + ')'

        sqlCsw_em_aberto2 = ' select o.numeroOP as numeroop,  o.codTipoOP, codSeqRoteiroAtual as seqAtual  from tco.ordemprod o where o.situacao = 3 and codTipoOP <> 13 and o.codempresa = ' + self.empresa

        with ConexaoCSW.Conexao() as conn:  ##Abrindo Conexao Com o CSW
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlCswOpsnivelSku)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                get = pd.DataFrame(rows, columns=colunas)
                del rows

                cursor_csw.execute(sqlCsw_em_aberto2)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                em_aberto2 = pd.DataFrame(rows, columns=colunas)
                del rows

                get = pd.merge(get, em_aberto2, on='numeroop')
        print('resultado')
        print(get)
        etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio,
                                              'etapa  consultando no CSW as OPs em aberto')

        sku = self.PesquisandoReduzido()
        get['codProduto'] = get['codProduto'].astype(str)
        get['codSortimento'] = get['codSortimento'].astype(str)
        get['seqTamanho'] = get['seqTamanho'].astype(str)
        print(sku[sku["codProduto"] == '010220012-0'])
        get = pd.merge(get, sku, on=["codProduto", "codSortimento", "seqTamanho"], how='left')
        print(get.dtypes)

        # Atribui valores iniciais à coluna 'id' com base na coluna 'codTipoOP'
        get['id'] = np.where(get['codTipoOP'].isin([1, 3]), 9000, 2000)
        # Atualiza os valores da coluna 'id' com base na coluna 'codfaseatual'
        get['id'] = np.where(get['codFaseAtual'].isin(['1', '401']), 1000, get['id'])

        # contagem de duplicaçoes : reduzido + codTipoOP + codFaseAtual
        get['concatenar'] = get['codreduzido'] + get['codFaseAtual']
        get['pesquisa'] = get.groupby('concatenar')['concatenar'].transform('count')

        get2 = get[(get['pesquisa'] > 1) & (get['codTipoOP'].isin([1, 3, 5]))]
        get2 = get2.sort_values(by=['codreduzido', 'numeroop'], ascending=True)  # escolher como deseja classificar
        get2['NovoseqAtual'] = 1 - ((get2.groupby('concatenar')['concatenar'].cumcount() + 1) * 0.1)
        get2['seqAtual'] = get2['seqAtual'].astype(int)
        get2['seqAtual'] = get2.groupby('concatenar')['seqAtual'].transform('max')
        get2['NovoseqAtual'] = get2['seqAtual'].astype(str) + get2['NovoseqAtual'].astype(str)

        get2 = get2.groupby(['numeroop', 'codFaseAtual', 'codreduzido', 'NovoseqAtual']).agg(
            {'codProduto': 'first'}).reset_index()

        get = pd.merge(get, get2, on=['numeroop', 'codFaseAtual', 'codreduzido', 'codProduto'], how='left')
        get['NovoseqAtual'].fillna('-', inplace=True)
        get['seqAtual'] = get.apply(lambda r: r['NovoseqAtual'] if r['NovoseqAtual'] != '-' else r['seqAtual'], axis=1)
        get['id'] = get['id'] + get['seqAtual'].astype(float)

        get.drop(['pesquisa', 'concatenar', 'NovoseqAtual'],
                 axis=1,
                 inplace=True)

        get['id'] = get['id'].astype(str) + '||' + get['codreduzido'].astype(str)
        get = get.sort_values(by=['codreduzido', 'id'], ascending=False)  # escolher como deseja classificar

        get['ocorrencia_sku'] = get.groupby('codreduzido').cumcount() + 1
        get['id'] = get['id'].astype(str) + '||' + get['ocorrencia_sku'].astype(str)
        get['qtdAcumulada'] = get.groupby('codreduzido')['total_pcs'].cumsum()

        etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1,
                                              'etapa tratando as ops a nivel de sku')

        return get

    def PesquisandoReduzido(self):
        conn = ConexaoPostgreMPL.conexaoEngineMatrizPCP()

        consulta = """select "codItemPai" as "codProduto", "codSortimento"::varchar as "codSortimento", 
        "codSeqTamanho"::varchar as "seqTamanho", "codigo" as codreduzido, categoria  from "PCP".pcp.itens_csw ic """

        consulta = pd.read_sql(consulta, conn)

        consulta['codProduto'] = consulta['codProduto'] + "-0"
        consulta['codProduto'] = consulta['codProduto'].apply(lambda x: '0' + x if x.startswith(('1', '2')) else x)
        consulta['codSortimento'] = consulta['codSortimento'].astype(str)
        consulta['codSortimento'] = consulta['codSortimento'].replace('.0', '')

        consulta['seqTamanho'] = consulta['seqTamanho'].astype(str)
        return consulta

    def IncrementadoDadosPostgre(self):
        dados = self.buscandoOPCSW()
        ConexaoPostgreMPL.Funcao_InserirPCPMatriz(dados, dados['numeroop'].size, 'ordemprod', 'replace')
        return pd.DataFrame([{'status': True, 'Mensagem': 'Dados carregados com sucesso !'}])



