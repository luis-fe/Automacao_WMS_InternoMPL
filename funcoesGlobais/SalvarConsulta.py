### Esse arquivo contem as funcoes de salvar as utimas consulta no banco de dados do POSTGRE WMS, com o
#objetivo especifico de controlar as atualizacoes

import ConexaoPostgreMPL
import locale
import datetime
import pytz


# Funcao Para obter a Data e Hora
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
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