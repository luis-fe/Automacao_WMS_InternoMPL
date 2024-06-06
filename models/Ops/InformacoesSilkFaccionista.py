import BuscasAvancadas
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

def ObterOpsEstamparia(rotina, datainicio):
    with ConexaoCSW as conn:

        consulta = pd.read_sql(BuscasAvancadas.RelacaoDeOPsSilk(),conn)

    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datainicio,'etapa csw consultando RelacaoDeOPsSilk')



    conn2 = ConexaoPostgreMPL.conexao()

    consulta2 = pd.read_sql('select "OPpai" from "Reposicao"."Reposicao"."OpsEstamparia" oe ',conn2)

    conn2.close()

    # Merge dos DataFrames
    merged = pd.merge(consulta, consulta2, on='OPpai', how='left', indicator=True)

    # Filtrar apenas as linhas da esquerda que não têm correspondência na direita
    nao_encontrados = merged[merged['_merge'] == 'left_only']

    # Remover a coluna indicadora de merge
    nao_encontrados = nao_encontrados.drop(columns='_merge')

    etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa relacao merge csw com o Portal Qualidade tabela estamparia')


    if nao_encontrados.empty:
        print('nao foram encontraos ops para atualizar')
        etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa2,'etapa atualizando dados na tabela estamparia do PortalQualidade')

    else:
        #Remove Duplicatas
        nao_encontrados = nao_encontrados.drop_duplicates()

        #Carregando dados no Wms
        ConexaoPostgreMPL.Funcao_Inserir(consulta,nao_encontrados['OPpai'].size,'OpsEstamparia','append')
        etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa2,'etapa atualizando dados na tabela estamparia do PortalQualidade')

