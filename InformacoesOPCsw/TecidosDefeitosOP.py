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

    consulta = pd.read_sql(BuscasAvancadas.Movimento(),conn)#coditem , mo.nomeItem, mo.codFornecNota, mo.dataLcto , mo.numDocto, mo.numeroLcto
    consulta.drop(['numDocto', 'numeroLcto','dataLcto'], axis=1, inplace=True)


    consulta = consulta.drop_duplicates()
    consulta['repeticoessku'] = consulta.groupby('coditem').cumcount() + 1


    conn.close()
    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta, consulta['coditem'].size, 'OPSDefeitoTecidos', 'replace')
    return consulta