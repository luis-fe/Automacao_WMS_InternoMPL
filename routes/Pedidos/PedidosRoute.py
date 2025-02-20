import gc
import controle
from models.Pedidos import PedidosClass
from models import AutualizaRealizadoFases


def AutomacaoPedidos(IntervaloAutomacao):
        print('\n 01- Atualizar Pedidos')

        rotina = 'AutomacaoPedidos'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoPedidos')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
                print('\nETAPA Atualizar Pedidos- Inicio')
                controle.InserindoStatus(rotina, client_ip, datainicio)
                automacaoPedidos = PedidosClass.AutomacaoPedidos('1',rotina, datainicio)
                automacaoPedidos.incrementarPedidos()
                automacaoPedidos.trasferenciaDeArquivo()
                automacaoPedidos.trasferenciaDeArquivo2()
                #automacaoPedidos.IncrementadoDadosPostgre()
                controle.salvarStatus(rotina, client_ip, datainicio)
                print('ETAPA  Atualizar Pedidos- Fim')
                gc.collect()



        else:
            print(f'JA EXISTE UMA ATUALIZACAO dos Pedidos  EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()

def AutomacaoRealizadoFases(IntervaloAutomacao):
        print('\n 02- Automacao RealizadoFases')

        rotina = 'AutomacaoRealizadoFases'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoRealizadoFases')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
                print('\nETAPA Atualizar AutomacaoRealizadoFases- Inicio')
                controle.InserindoStatus(rotina, client_ip, datainicio)
                automacaoRealizado = AutualizaRealizadoFases.ProducaoFases('','','',100,'',5000,60,rotina,datainicio)
                automacaoRealizado.atualizandoDados_realizados()
                controle.salvarStatus(rotina, client_ip, datainicio)
                print('ETAPA  Atualizar AutomacaoRealizadoFases- Fim')
                gc.collect()



        else:
            print(f'JA EXISTE UMA ATUALIZACAO dos AutomacaoRealizadoFases  EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()


def Componentes(IntervaloAutomacao):
        print('\n 01- Atualizar Pedidos')

        rotina = 'AutomacaoComponentes'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoComponentes')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
                print('\nETAPA Atualizar AutomacaoComponentes- Inicio')
                controle.InserindoStatus(rotina, client_ip, datainicio)
                automacaoPedidos = PedidosClass.AutomacaoPedidos('1',rotina, datainicio)
                automacaoPedidos.inserirComponentesVariaveis()
                automacaoPedidos.trasferenciaDeArquivoVariaveis()
                controle.salvarStatus(rotina, client_ip, datainicio)
                print('ETAPA  Atualizar AutomacaoComponentes- Fim')
                gc.collect()



        else:
            print(f'JA EXISTE UMA ATUALIZACAO dos Pedidos  EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()
