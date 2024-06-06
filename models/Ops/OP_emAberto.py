import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd

import controle
from funcoesGlobais import SalvarConsulta
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S')
    return agora

def BuscandoOPCSW(empresa):
    inicio = obterHoraAtual()

    with ConexaoCSW.Conexao() as conn:##Abrindo Conexao Com o CSW

        em_aberto = ' (select o.numeroOP  from tco.ordemprod o where o.situacao = 3 and o.codempresa = '+empresa+')'

        get = pd.read_sql('SELECT ot.codProduto ,ot.numeroop as numeroop , codSortimento , seqTamanho, '
                      ' case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs '
                      "FROM tco.OrdemProdTamanhos ot "
                      "having ot.codEmpresa = " + empresa + " and ot.numeroOP IN " + em_aberto, conn)

        em_aberto2 = ' select o.numeroOP as numeroop,  o.codTipoOP, codSeqRoteiroAtual as seqAtual  from tco.ordemprod o where o.situacao = 3 and o.codempresa = '+empresa
        em_aberto2 = pd.read_sql(em_aberto2,conn)

    get = pd.merge(get,em_aberto2,on='numeroop')

    sku = PesquisandoReduzido()
    get['codProduto'] = get['codProduto'].astype(str)
    get['codSortimento'] = get['codSortimento'] .astype(str)
    get['seqTamanho'] = get['seqTamanho'] .astype(str)
    get = pd.merge(get, sku, on=["codProduto", "codSortimento", "seqTamanho"], how='left')


    get['id'] = get.apply(lambda r: 9000 if r['codTipoOP'] in [1, 4] else 1000, axis=1)
    get['id'] = get['id'] + get['seqAtual'].astype(int)
    get['id'] = get['id'].astype(str) + '||'+get['codreduzido'].astype(str)
    get = get.sort_values(by=['codreduzido','id'], ascending=False)  # escolher como deseja classificar

    get['ocorrencia_sku'] = get.groupby('codreduzido').cumcount() + 1
    get['id'] = get['id'].astype(str) + '||'+get['ocorrencia_sku'].astype(str)
    get['qtdAcumulada'] = get.groupby('codreduzido')['total_pcs'].cumsum()


    SalvarConsulta.salvar('sql tco.OrdemProdTamanhos ot','off.ordemprod',inicio)
    return get

def IncrementadoDadosPostgre(empresa, rotina, datainicio):
    inicio = obterHoraAtual()
    diferencaTempo = SalvarConsulta.UltimaAtualizacao('off.ordemprod',inicio)

    if diferencaTempo >= 600:

        dados = BuscandoOPCSW(empresa)
        etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao', datainicio,
                                              'etapa  consultando no CSW Buscando OPs em aberto')

        try:
            ConexaoPostgreMPL.Funcao_InserirOFF(dados,dados['numeroop'].size,'ordemprod','replace')
            etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao', etapa1,
                                                  'etapa inserindo ops no wms')
            print('OPS INSERIDAS COM SUCESSO')
        except:
            print('FALHA AO TENTAR INSERIR OS DADOS')
    else:
        print('A Comunicacao com a Classe ordemprod do WMS ja foi realizado a 10 min, nao precisa atualizar.')


def PesquisandoReduzido():
    conn = ConexaoPostgreMPL.conexaoPCP()

    consulta = """select "codItemPai" as "codProduto"  ,"codSortimento" as "codSortimento" , "codSeqTamanho" as "seqTamanho", "codSKU" as codreduzido from "pcp"."SKU" """

    consulta = pd.read_sql(consulta,conn)
    conn.close()

    consulta['codProduto'] = consulta['codProduto'] + "-0"
    consulta['codProduto'] = consulta['codProduto'].apply(lambda x: '0'+ x if x.startswith(('1', '2')) else x)
    consulta['codSortimento'] = consulta['codSortimento'] .astype(str)
    consulta['seqTamanho'] = consulta['seqTamanho'] .astype(str)

    return consulta

#IncrementadoDadosPostgre('1','teste',controle.obterHoraAtual())