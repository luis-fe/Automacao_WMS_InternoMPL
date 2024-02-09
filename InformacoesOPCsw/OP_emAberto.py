import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def BuscandoOPCSW(empresa):
    conn = ConexaoCSW.Conexao()##Abrindo Conexao Com o CSW

    em_aberto = ' (select o.numeroop from tco.ordemprod o where o.situacao = 3 and o.codempresa = '+empresa+' )'

    get = pd.read_sql('SELECT op.numeroop,  '
                      '(SELECT i.codItem  from cgi.Item2 i WHERE i.Empresa = 1 and i.codseqtamanho = op.seqTamanho '
                      "and i.codsortimento = op.codSortimento and '0'||i.coditempai||'-0' = op.codproduto) as codreduzido, "
                      "case WHEN op.qtdePecas1Qualidade is null then op.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs "
                      "FROM tco.OrdemProdTamanhos op "
                      "WHERE op.codEmpresa = " + empresa + " and op.numeroOP IN " + em_aberto, conn)

    conn.close()# Fechado a conexao com o CSW

    return get

def IncrementadoDadosPostgre(empresa):
        dados = BuscandoOPCSW(empresa)
        print(dados['numeroop'].size)
    #try:
        #ConexaoPostgreMPL.Funcao_InserirOFF(dados,dados['numeroop'].size,'ordemprod','replace')
        #print('OPS INSERIDAS COM SUCESSO')
    #except:
       # print('FALHA AO TENTAR INSERIR OS DADOS')

