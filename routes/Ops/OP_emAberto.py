## Funcao de Automacao 13: Ordem Producao  :
import gc
import controle
from models.Ops import OP_AbertoClass as classe


def OrdemProducao(IntervaloAutomacao):
        print('\n 13 - Importando as Ordem de Producao')
        rotina = 'ordem de producao'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ordem de producao')
        limite = IntervaloAutomacao * 60  # (limite de 10 minutos , convertido para segundos)

        if tempo > limite:
            print('\nETAPA importando Ordem de Producao- Inicio')
            controle.InserindoStatus(rotina, client_ip, datainicio)

            op_aberto = classe.Op_AbertoClass('1',rotina,datainicio)
            op_aberto.IncrementadoDadosPostgre()
            controle.salvarStatus(rotina, client_ip, datainicio)
            print('ETAPA importando Ordem de Producao- Fim')
            gc.collect()

        else:
            print(f'JA EXISTE UMA ATUALIZACAO em importando Ordem de Producao EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()

        return True

