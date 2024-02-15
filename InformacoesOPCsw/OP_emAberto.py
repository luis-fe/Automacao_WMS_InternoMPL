import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def BuscandoOPCSW(empresa):
    conn = ConexaoCSW.Conexao()##Abrindo Conexao Com o CSW

    em_aberto = ' (select o.numeroOP  from tco.ordemprod o where o.situacao = 3 and o.codempresa = '+empresa+')'

    get = pd.read_sql('SELECT ot.numeroop as numeroop, codItem as codreduzido, '
                      ' case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs '
                      "FROM tco.OrdemProdTamanhos ot "
                      "having ot.codEmpresa = " + empresa + " and ot.numeroOP IN " + em_aberto, conn)

    conn.close()# Fechado a conexao com o CSW

    return get

def IncrementadoDadosPostgre(empresa):
    dados = BuscandoOPCSW(empresa)
    try:
        ConexaoPostgreMPL.Funcao_InserirOFF(dados,dados['numeroop'].size,'ordemprod','replace')
        print('OPS INSERIDAS COM SUCESSO')
    except:
        print('FALHA AO TENTAR INSERIR OS DADOS')

