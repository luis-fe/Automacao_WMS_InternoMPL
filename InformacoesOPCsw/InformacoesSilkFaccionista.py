import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
from funcoesGlobais import SalvarConsulta
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S')
    return agora

def ObterOpsEstamparia():
    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql(BuscasAvancadas.RelacaoDeOPsSilk(),conn)
    conn.close()

    #Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta,consulta['OPpai'].size,'OpsEstamparia','replace')

    return consulta
