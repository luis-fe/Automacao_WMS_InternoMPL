import gc
import controle
from models.Pedidos import ReservaPreFaturamentoClass


def AutomacaoReservarPedidosPreFat(IntervaloAutomacao):
        print('\n 02- ReservarPedidosPreFat')

        rotina = 'ReservarPedidosPreFat'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ReservarPedidosPreFat')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
                print('\nETAPA ReservarPedidosPreFat- Inicio')
                controle.InserindoStatus(rotina, client_ip, datainicio)
                automacaoPedidos = ReservaPreFaturamentoClass.ReservaPreFaturamento('1')
                automacaoPedidos.conexaoAPIreservaFatCsw()
                #automacaoPedidos.IncrementadoDadosPostgre()
                controle.salvarStatus(rotina, client_ip, datainicio)
                print('ETAPA  Atualizar ReservarPedidosPreFat- Fim')
                gc.collect()



        else:
            print(f'JA EXISTE UMA ATUALIZACAO dos Pedidos  EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()