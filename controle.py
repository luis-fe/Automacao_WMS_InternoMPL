### Esse arquivo contem as funcoes de salvar as utimas consulta no banco de dados do POSTGRE , com o
#objetivo especifico de controlar as requisicoes
import pandas as pd
import ConexaoPostgreMPL
import locale
from datetime import datetime
import pytz


# Funcao Para obter a Data e Hora
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S')
    return agora


def salvar(rotina, ip,datahoraInicio):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%d/%m/%Y %H:%M:%S")
    data2_obj = datetime.strptime(datahorafinal, "%d/%m/%Y %H:%M:%S")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreMPL.conexao()

    consulta = 'insert into "Reposicao".configuracoes.controle_requisicao_csw (rotina, fim, inicio, ip_origem, "tempo_processamento(s)") ' \
          'values (%s , %s , %s , %s, %s )'

    cursor = conn.cursor()

    cursor.execute(consulta,(rotina,datahorafinal, datahoraInicio, ip, tempoProcessamento ))
    conn.commit()
    cursor.close()

    conn.close()

# Funcao que retorna a utima atualizacao
def UltimaAtualizacao(classe, dataInicial):

    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('Select max(datahora_final) as ultimo from "Reposicao".automacao_csw.atualizacoes where classe = %s ', conn, params=(classe,))

    conn.close()

    datafinal = consulta['ultimo'][0]

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(dataInicial, "%d/%m/%Y %H:%M:%S")
    data2_obj = datetime.strptime(datafinal, "%d/%m/%Y %H:%M:%S")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()


    return float(diferenca_total_segundos)



def ExcluirHistorico(diasDesejados):
    conn = ConexaoPostgreMPL.conexao()

    deletar = "DELETE FROM pcp.controle_requisicao_csw crc " \
              "WHERE rotina = 'Portal Consulta OP' " \
              "AND ((SUBSTRING(fim, 7, 4)||'-'||SUBSTRING(fim, 4, 2)||'-'||SUBSTRING(fim, 1, 2))::date - now()::date) < -%s"

    cursor = conn.cursor()

    cursor.execute(deletar, (diasDesejados,))
    conn.commit()
    cursor.close()
    conn.close()


def TempoUltimaAtualizacao(dataHoraAtual, rotina):
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select max(fim) as "ultimaData" from "Reposicao".configuracoes.controle_requisicao_csw crc '
                          "where rotina = %s ", conn, params=(rotina,) )



    conn.close()
    utimaAtualizacao = consulta['ultimaData'][0]

    if utimaAtualizacao != None:

        print(utimaAtualizacao)
        # Converte as strings para objetos datetime
        data1_obj = datetime.strptime(dataHoraAtual, "%d/%m/%Y %H:%M:%S")
        data2_obj = datetime.strptime(utimaAtualizacao, "%d/%m/%Y %H:%M:%S")

        # Calcula a diferença entre as datas
        diferenca = data1_obj - data2_obj

        # Obtém a diferença em dias como um número inteiro
        diferenca_em_dias = diferenca.days

        # Obtém a diferença total em segundos
        diferenca_total_segundos = diferenca.total_seconds()

        print(f'\n a data e hora atual é {data1_obj} a data e hora da ultima atualizacao {data2_obj} \ne a diferenca em segundos {diferenca_total_segundos}')
        return diferenca_total_segundos


    else:
        diferenca_total_segundos = 9999
        return diferenca_total_segundos


def conversaoData(data):
    data1_obj = datetime.strptime(data, "%d/%m/%Y %H:%M:%S")

    return data1_obj