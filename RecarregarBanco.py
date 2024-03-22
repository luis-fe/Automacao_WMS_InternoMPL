import datetime
import numpy
import pandas as pd
import jaydebeapi
from psycopg2 import sql
import pytz
import ConexaoCSW
import ConexaoPostgreMPL
import controle
import empresaConfigurada

def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def FilaTags(datainico, rotina):
    xemp = empresaConfigurada.EmpresaEscolhida()
    xemp = "'"+xemp+"'"
    conn = ConexaoCSW.Conexao()
    conn2 = ConexaoPostgreMPL.conexao()
    df_tags = pd.read_sql(
        "SELECT  codBarrasTag as codbarrastag, codNaturezaAtual as codnaturezaatual , codEngenharia , codReduzido as codreduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao , numeroop as numeroop,"
        " (SELECT i2.codCor||'-'  FROM cgi.Item2  i2 WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) || "
        "(SELECT i2.descricao  FROM tcp.SortimentosProduto  i2 WHERE i2.codEmpresa = 1 and  i2.codProduto  = t.codEngenharia  and t.codSortimento  = i2.codSortimento) as cor,"
        " (SELECT tam.descricao  FROM cgi.Item2  i2 join tcp.Tamanhos tam on tam.codEmpresa = i2.Empresa and tam.sequencia = i2.codSeqTamanho  WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as tamanho, codEmpresa as codempresa "
        " from tcr.TagBarrasProduto t WHERE codEmpresa in ("+xemp+") and codNaturezaAtual in (5, 7 ,54) and situacao in (3, 8)", conn)
    etapa1 = controle.salvarStatus_Etapa1(rotina,'automacao', datainico,'from tcr.TagBarrasProduto t')

    df_opstotal = pd.read_sql('SELECT top 200000 numeroOP as numeroop , totPecasOPBaixadas as totalop  '
                              'from tco.MovimentacaoOPFase WHERE codEmpresa = '+xemp+ "and codFase = 236  "
                              'order by numeroOP desc ',conn)

    etapa2 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa1,'from tco.MovimentacaoOPFase ')


    df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
    df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
    df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
    df_tags['totalop'] = df_tags['totalop'].astype(int)

    # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
       # Verificando as tag's que ja foram repostas
    TagsRepostas = pd.read_sql('select "codbarrastag" as codbarrastag, "usuario" as usuario_  from "Reposicao"."tagsreposicao" tr '
                               ' union select "codbarrastag" as codbarrastag, "usuario" as usuario_ from "Reposicao"."Reposicao".tagsreposicao_inventario ti ',conn2)

    df_tags = pd.merge(df_tags, TagsRepostas, on='codbarrastag', how='left')
    df_tags = df_tags.loc[df_tags['usuario_'].isnull()]
    df_tags.drop('usuario_', axis=1, inplace=True)
    etapa3 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa2,'WMS: "Reposicao"."tagsreposicao"   ')


        # Verificando as tag's que ja estam na fila
    ESTOQUE = pd.read_sql('select "usuario", "codbarrastag" as codbarrastag, "Situacao" as sti_aterior  from "Reposicao"."filareposicaoportag" ',conn2)
    df_tags = pd.merge(df_tags,ESTOQUE,on='codbarrastag',how='left')
    df_tags['Situacao'] = df_tags.apply(lambda row: 'Reposto' if not pd.isnull(row['usuario']) else 'Reposição não Iniciada', axis=1)
    etapa4 = controle.salvarStatus_Etapa2(rotina,'automacao', etapa3,'WMS: "Reposicao"."filareposicaoportag"   ')

    epc = LerEPC()
    df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
    df_tags.rename(columns={'codbarrastag': 'codbarrastag','codEngenharia':'engenharia'
                            , 'numeroop':'numeroop'}, inplace=True)
    conn2.close()
    df_tags = df_tags.loc[df_tags['sti_aterior'].isnull()]
    df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
    # Excluir a coluna 'B' inplace
    df_tags.drop('sti_aterior', axis=1, inplace=True)
    df_tags.drop_duplicates(subset='codbarrastag', inplace=True)
    df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
    tamanho = df_tags['codbarrastag'].size
    dataHora = obterHoraAtual()
    df_tags['DataHora'] = dataHora

    restringe = BuscaResticaoSubstitutos()
    df_tags = pd.merge(df_tags,restringe,on=['numeroop','cor'],how='left')

    df_tags['resticao'].fillna('-',inplace=True)
    df_tags['considera'].fillna('-',inplace=True)

    #try:
    tamanho2 = 1000
    ConexaoPostgreMPL.Funcao_Inserir(df_tags, tamanho2,'filareposicaoportag', 'append')
    hora = obterHoraAtual()
    return tamanho2, hora
    #except:
     #   print('falha na funçao Inserir')
      #  hora = obterHoraAtual()
       # return tamanho, hora


def LerEPC():
    xemp = empresaConfigurada.EmpresaEscolhida()
    xemp = "'"+xemp+"'"
    conn = ConexaoCSW.Conexao()


    consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                           'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                           'WHERE t.codEmpresa = '+xemp+' and (t.codTransacao = 3500 or t.codTransacao = 501) '
                           'and (codLote like "23%" or  codLote like "24%" or codLote like "25%" '
                           'or codLote like "22%" )',conn)
    conn.close()

    return consulta
def avaliacaoFila():
    xemp = empresaConfigurada.EmpresaEscolhida()
    xemp = "'"+xemp+"'"
    conn = ConexaoCSW.Conexao()
    SugestoesAbertos = pd.read_sql(
        "select br.codBarrasTag as codbarrastag , 'estoque' as estoque  from Tcr.TagBarrasProduto br "
        'WHERE br.codEmpresa in ('+xemp+') and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54)', conn)
    conn2 = ConexaoPostgreMPL.conexao()

    tagWms = pd.read_sql('select * from "Reposicao".filareposicaoportag t ', conn2)
    tagWms = pd.merge(tagWms,SugestoesAbertos, on='codbarrastag', how='left')
    tagWms = tagWms[tagWms['estoque']!='estoque']
    tamanho =tagWms['codbarrastag'].size
    # Obter os valores para a cláusula WHERE do DataFrame
    lista = tagWms['codbarrastag'].tolist()
    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame


    if tamanho != 0:
        query = sql.SQL('DELETE FROM "Reposicao"."filareposicaoportag" WHERE codbarrastag IN ({})').format(
            sql.SQL(',').join(map(sql.Literal, lista))
        )

        # Executar a consulta DELETE
        with conn2.cursor() as cursor:
            cursor.execute(query)
            conn2.commit()
    else:
        print('2.1.1 sem tags para ser eliminadas na Fila Tags Reposicao')

    dataHora = obterHoraAtual()
    return tagWms['codbarrastag'].size, dataHora

def BuscaResticaoSubstitutos():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql("select numeroop , codproduto||'||'||numeroop  as resticao,  "
                            'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                           "sso where sso.considera = 'sim'",conn)

    conn.close()

    return consulta