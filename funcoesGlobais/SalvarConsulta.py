### Esse arquivo contem as funcoes de salvar as utimas consulta no banco de dados do POSTGRE WMS, com o
#objetivo especifico de controlar as atualizacoes

import ConexaoPostgreMPL
import locale
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S')
    return agora


def salvar(sql, classe,datahoraInicio):
    datahora = obterHoraAtual()
    print(datahora)