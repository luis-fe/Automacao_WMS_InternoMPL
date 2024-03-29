import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S')
    return agora


def DefeitosTecidos():
    conn = ConexaoCSW.Conexao()

    mvtoBalanca = pd.read_sql(BuscasAvancadas.MovBalanca(),conn) #coditem,  m.dataLcto, m.numeroLcto , m.codRequisicao, qtdBrutoRolo
    Movimento = pd.read_sql(BuscasAvancadas.Movimento(),conn)#coditem , mo.nomeItem, mo.codFornecNota, mo.dataLcto , mo.numDocto, mo.numeroLcto

    mvtoBalanca['coditem'] = mvtoBalanca['coditem'].astype(str)
    Movimento['coditem'] = mvtoBalanca['coditem'].astype(str)


    consulta = pd.merge(mvtoBalanca,Movimento,on=['coditem','dataLcto','numeroLcto'])


    conn.close()
    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta, consulta['coditem'].size, 'OPSDefeitoTecidos', 'replace')
    return consulta