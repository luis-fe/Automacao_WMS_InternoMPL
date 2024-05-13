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

    conn = ConexaoCSW.Conexao()##Abrindo Conexao Com o CSW

    em_aberto = ' (select o.numeroOP  from tco.ordemprod o where o.situacao = 3 and o.codempresa = '+empresa+')'

    get = pd.read_sql('SELECT ot.numeroop as numeroop, codItem as codreduzido, '
                      ' case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs '
                      "FROM tco.OrdemProdTamanhos ot "
                      "having ot.codEmpresa = " + empresa + " and ot.numeroOP IN " + em_aberto, conn)

    em_aberto2 = ' select o.numeroOP as numeroop,  o.codTipoOP, codSeqRoteiroAtual as seqAtual  from tco.ordemprod o where o.situacao = 3 and o.codempresa = '+empresa
    em_aberto2 = pd.read_sql(em_aberto2,conn)

    get = pd.merge(get,em_aberto2,on='numeroop')

    conn.close()# Fechado a conexao com o CSW

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

