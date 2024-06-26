import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW


def transferirTags(empresa):
    sql ="""
    SELECT t.codBarrasTag as codbarrastag, t.codNaturezaAtual as codnaturezaatual FROM Tcr.TagBarrasProduto t
    WHERE t.codEmpresa = """+ str(empresa)+""" and codnaturezaatual not in(5) and situacao = 3
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
