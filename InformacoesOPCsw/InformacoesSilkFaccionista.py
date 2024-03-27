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

    conn2 = ConexaoPostgreMPL.conexao()

    consulta2 = pd.read_sql('select * from "Reposicao"."Reposicao"."OpsEstamparia" oe ',conn2)

    conn2.close()

    # Merge dos DataFrames
    merged = pd.merge(consulta, consulta2, on='OPpai', how='left', indicator=True)

    # Filtrar apenas as linhas da esquerda que não têm correspondência na direita
    nao_encontrados = merged[merged['_merge'] == 'left_only']

    # Remover a coluna indicadora de merge
    nao_encontrados = nao_encontrados.drop(columns='_merge')

    if nao_encontrados.empty:
        print('nao foram encontraos ops para atualizar')
    else:
        #Carregando dados no Wms
        ConexaoPostgreMPL.Funcao_Inserir(consulta,nao_encontrados['OPpai'].size,'OpsEstamparia','append')

