import ConexaoPostgreMPL
from datetime import datetime
import pytz
import pandas as pd
import psutil

class Controle ():
    '''Classe criada para controle da automacao , registrando os dados para analisar performa-se e a aplicacao!'''
    def __init__(self,empresa, PID, PIDLiberado):
        self.empresa = empresa
        self.PID = str(PID)
        self.PIDLiberado = PIDLiberado
    def inserirNovoPID(self):

        insertPID = """insert into configuracoes."ControlePID" ("PID","DataHoraCriacao", status) values (%s , %s, %s)  """
        dataHora = self.oBterDataHora('en')
        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:

                curr.execute(insertPID,(self.PID,dataHora, self.PIDLiberado ))
                conn.commit()

    def excluirPID(self):

        excluirPID = """DELETE FROM configuracoes."ControlePID" WHERE "PID" = %s """
        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:

                curr.execute(excluirPID,(self.PID,))
                conn.commit()

    def oBterDataHora(self, padrao = 'br'):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        if padrao == 'br':
            agora = agora.strftime('%d/%m/%Y %H:%M:%S.%f')[:-3]

        else:
            agora = agora.strftime('%Y-%m-%d %H:%M:%S')


        return agora


    def statusLiberacaoPID(self):
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = """Select status from configuracoes."ControlePID" where PID = %s """
        consulta = pd.read_sql(consulta,conn,params=(self.PID))

        return bool(consulta['status'][0])
