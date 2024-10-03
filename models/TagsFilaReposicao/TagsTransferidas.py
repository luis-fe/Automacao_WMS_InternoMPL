import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW
import empresaConfigurada


class AutomacaoTransferenciaTags():
    '''Classe utilizada para checar se as tags foram transferidas de natureza e enviar de volta a fila'''
    def __init__(self, empresa):
        '''Contrutor definindo a empresa
        :arg
        self.empesa : o Codigo da empresa em str da qual estamos acessando a automacao, seguindo do IF para caso "NONE" buscar da empresa parametrizada.
        '''
        if empresa != None:
            self.empresa = empresa
        else:
            self.empresa  = empresaConfigurada.EmpresaEscolhida()

    def transferirTagsParaFila(self):
        '''FUNCAO QUE CONSULTA AS TAGS EM ESTOQUE FORA DA NATUREZA 5 e devolve para fila'''
        sql ="""
        SELECT t.codBarrasTag as codbarrastag, t.codNaturezaAtual as codnaturezaatual FROM Tcr.TagBarrasProduto t
        WHERE t.codEmpresa = """+ str(self.empresa)+""" and codnaturezaatual not in(5, 7) and situacao = 3
        """


        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                tags = pd.DataFrame(rows, columns=colunas)

        conn = ConexaoPostgreMPL.conexaoEngine()
        tagsReposicao = pd.read_sql("""SELECT "codreduzido", "engenharia","codbarrastag","numeroop", "descricao", "cor", "epc", "tamanho", "totalop"  from "Reposicao".tagsreposicao """,conn)

        tagsReposicao = pd.merge(tagsReposicao,tags,on='codbarrastag')

        Insert = 'INSERT INTO  "Reposicao"."filareposicaoportag" ("codreduzido", "engenharia","codbarrastag","numeroop", "descricao", "cor", "epc", "tamanho", "totalop", "Situacao",' \
                 ' , codnaturezaatual) ' \
                 'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'+"'Reposição não Iniciada'"+',%s );'

        with conn.connect() as connection:
            for index, row in tagsReposicao.iterrows():
                connection.execute(Insert, (
                row["codreduzido"], row["engenharia"], row["codbarrastag"], row["numeroop"], row["descricao"], row["cor"], row["epc"], row["tamanho"], row["totalop"],
                'Reposição não Iniciada', row["codnaturezaatual"]
            ))

    def mudancaNatureza(self):

        sql ="""
        SELECT t.codBarrasTag as codbarrastag, t.codNaturezaAtual as codnaturezaatual FROM Tcr.TagBarrasProduto t
        WHERE t.codEmpresa = """+ str(self.empresa)+""" and codnaturezaatual  in(5, 7) and situacao = 3
        """


        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                tags = pd.DataFrame(rows, columns=colunas)

        conn = ConexaoPostgreMPL.conexaoEngine()
        tagsReposicao = pd.read_sql(
            """SELECT "codbarrastag", "natureza" as "naturezaWMS"  from "Reposicao".tagsreposicao """,
            conn)

        tagsReposicao = pd.merge(tagsReposicao, tags, on='codbarrastag')
        tagsReposicao = tagsReposicao[tagsReposicao['naturezaWMS']!=tagsReposicao['codnaturezaatual']].reset_index()

        update = """
        update "Reposicao"."Reposicao".tagsreposicao
        set natureza = %s
        where codbarrastag = %s
        """


        with conn.connect() as connection:
            for index, row in tagsReposicao.iterrows():
                connection.execute(update, (
                row["codnaturezaatual"], row["codbarrastag"]
            ))








