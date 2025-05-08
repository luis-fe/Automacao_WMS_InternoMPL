import gc
import pandas as pd
import ConexaoCSW, ConexaoPostgreMPL
import controle


class ProducaoFases():
    '''Classe que controla a producao das fases '''

    def __init__(self, periodoInicio = None, periodoFinal = None, codFase = None, dias_buscaCSW = 0, codEmpresa = None, limitPostgres = None, utimosDias = None, rotina = None , dataInicio = None):

        self.periodoInicio = periodoInicio
        self.periodoFinal = periodoFinal
        self.codFase = codFase
        self.dias_buscaCSW = dias_buscaCSW
        self.codEmpresa = codEmpresa
        self.limitPostgres = limitPostgres
        self.utimosDias = utimosDias
        self.rotina = rotina
        self.dataInicio = dataInicio



    def __buscarProducao_erpCSW(self):
        '''Metodo privado que procura no ERP CSW a producao de acordo com o numero de dias informado para buscar, atributo :: self.dias_buscaCSW '''

        sql = """
            SELECT 
                f.codEmpresa,
                f.numeroop as numeroop, 
                f.codfase as codfase, 
                f.seqroteiro, 
                f.databaixa, 
                f.nomeFaccionista, 
                f.codFaccionista,
                f.horaMov, f.totPecasOPBaixadas, 
                f.descOperMov, 
                (select op.codProduto  from tco.ordemprod op WHERE op.codempresa = 1 and op.numeroop = f.numeroop) as codEngenharia,
                (select op.codTipoOP  from tco.ordemprod op WHERE op.codempresa = 1 and op.numeroop = f.numeroop) as codtipoop 
            FROM 
                tco.MovimentacaoOPFase f
            WHERE 
                f.codEmpresa = """+str(self.codEmpresa)+""" and 
                f.databaixa >=  DATEADD(DAY, -""" + str(self.dias_buscaCSW) + """, GETDATE())"""

        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta

    def __limpezaDadosRepetidos_ProducaoFasesPostgre(self):
        '''Metodo que busca e limpa dados repetidos do banco Postgres'''

        sqlDelete = """
			WITH duplicatas AS 
				(
                SELECT chave, 
                        ctid,  -- Identificador físico da linha (evita deletar todas)
                        ROW_NUMBER() OVER (PARTITION BY chave ORDER BY ctid) AS rn
                FROM "pcp".realizado_fase
                    )
            DELETE FROM "pcp".realizado_fase
                WHERE ctid IN 
                    (
                        SELECT ctid FROM duplicatas WHERE rn > 1
                    );
        """

        conn1 = ConexaoPostgreMPL.conexaoMatrizWMS()
        curr = conn1.cursor()
        curr.execute(sqlDelete, )
        conn1.commit()
        curr.close()
        conn1.close()

        sql = """
        select distinct CHAVE, 'ok' as status from pcp.realizado_fase
        order by CHAVE desc limit %s
        """

        conn = ConexaoPostgreMPL.conexaoMatrizWMS()
        consulta = pd.read_sql(sql, conn, params=(self.limitPostgres,))

        return consulta

    def atualizandoDados_realizados(self):
        sql = """
            SELECT
                f.codEmpresa, 
                f.numeroop as numeroop, 
                f.codfase as codfase, 
                f.seqroteiro, 
                f.databaixa, 
                f.nomeFaccionista, 
                f.codFaccionista,
                f.horaMov, 
                f.totPecasOPBaixadas, 
                f.descOperMov, 
                (select op.codProduto  from tco.ordemprod op WHERE op.codempresa = f.codempresa and op.numeroop = f.numeroop) as codEngenharia,
                (select op.codTipoOP  from tco.ordemprod op WHERE op.codempresa = f.codempresa and op.numeroop = f.numeroop) as codtipoop,
				(select l.descricao from tcl.Lote l WHERE l.codEmpresa = f.codempresa and f.codLote = l.codLote) as descricaolote 
            FROM 
                tco.MovimentacaoOPFase f
            WHERE 
                f.codEmpresa in (1, 4) and f.databaixa >=  DATEADD(DAY, -""" + str(self.utimosDias) + """, GETDATE())
                """

        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sql = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        # Monta a chave

        sql['chave'] = sql['codEmpresa'].astype(str) + '||' +sql['numeroop'] + '||' + sql['codfase'].astype(str)
        etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio, 'buscando o realizado dos ultimos dias')

        dadosAnteriores = self.__limpezaDadosRepetidos_ProducaoFasesPostgre()
        sql = pd.merge(sql, dadosAnteriores, on='chave', how='left')
        sql['status'].fillna('-', inplace=True)
        sql = sql[sql['status'] == '-'].reset_index()
        print(sql[sql['chave'] == '1||153378-001||408' ])
        print(sql['numeroop'].size)
        sql = sql.drop(columns=['status', 'index'])
        etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1, 'limpando os dados anteriores')



        if sql['numeroop'].size > 0:
            # Implantando no banco de dados do Pcp
            ConexaoPostgreMPL.Funcao_InserirPCPMatrizWMS(sql, sql['numeroop'].size, 'realizado_fase', 'append')

            sqlDelete = """
            			WITH duplicatas AS 
            				(
                            SELECT chave, 
                                    ctid,  -- Identificador físico da linha (evita deletar todas)
                                    ROW_NUMBER() OVER (PARTITION BY chave ORDER BY ctid) AS rn
                            FROM "pcp".realizado_fase
                            order by chave asc , ctid desc 
                                )
                        DELETE FROM "pcp".realizado_fase
                            WHERE ctid IN 
                                (
                                    SELECT ctid FROM duplicatas WHERE rn > 1
                                );
                    """

            conn1 = ConexaoPostgreMPL.conexaoMatrizWMS()
            curr = conn1.cursor()
            curr.execute(sqlDelete, )
            conn1.commit()
            curr.close()
            conn1.close()

            etapa4 = controle.salvarStatus_Etapa3(self.rotina, 'automacao', etapa2, 'inserindo dados no postgree')

        else:
            print('segue o baile')

