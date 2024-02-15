### Esse arquivo contem as funcoes de salvar as utimas consulta no banco de dados do POSTGRE WMS, com o
#objetivo especifico de controlar as atualizacoes
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


def salvar(sql, classe,datahoraInicio):
    dataHora = obterHoraAtual()
    conn = ConexaoPostgreMPL.conexao()

    consulta = 'insert into "Reposicao".automacao_csw.atualizacoes (sql, datahora_final, datahora_inicio, classe) ' \
          'values (%s , %s , %s , %s)'

    cursor = conn.cursor()

    cursor.execute(consulta,(sql,dataHora, datahoraInicio, classe ))
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
    diferenca = data2_obj - data1_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()



    print(str(diferenca_total_segundos))
    print(str(data2_obj))
