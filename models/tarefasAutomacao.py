import ConexaoPostgreMPL
import pandas as pd
import os

class TarefaAutomacao():

    def __init__(self, tarefa = ''):

        self.tarefa = tarefa

    def get_SqlTarefas(self):
        '''Metodo que busca as tarefas de automacao cadastras'''


        sql = """
        select
            crc.rotina ,
            max(crc.fim) as "UltimaAtualizacao"
        from
            configuracoes.controle_requisicao_csw crc
        group by rotina
        order by
            "UltimaAtualizacao" desc 
        """

        sql2 = """
        select
            *,
            inet_server_addr() as srv
        from
            configuracoes."ServicoAutomacao"
        """

        conn = ConexaoPostgreMPL.conexaoEngine()

        consulta = pd.read_sql(sql,conn)
        consulta2 = pd.read_sql(sql2,conn)


        consulta = pd.merge(consulta, consulta2, on="rotina")

        screen_name = os.environ.get('STY')

        consulta['Nome Screen'] = screen_name
        return consulta



