import gc
import psutil
from colorama import Fore
import controle
from models.Pedidos import Pedidos, RecarregaPedidos
import os

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # Retorna o uso de memória em bytes

## Funcao de Automacao 1 : Buscando a atualizacao dos SKUs: Duracao media de 30 Segundos
def AtualizarSKU(IntervaloAutomacao):
    cpu_percent = psutil.cpu_percent()
    memoria_antes = memory_usage()
    print(memoria_antes)
    print("Uso da CPU inicio do processo:", cpu_percent, "%")
    print(Fore.LIGHTYELLOW_EX+f'\nETAPA 1 - ATUALIZACAO DO AutomacaoCadastroSKU uso atual da cpu {cpu_percent}%')
    client_ip = 'automacao'
    rotina  = 'AutomacaoCadastroSKU'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoCadastroSKU')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print(f'\nETAPA {rotina}- Inicio: {controle.obterHoraAtual()}')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            cpu_percent = psutil.cpu_percent()
            print("Uso da CPU:", cpu_percent, "%")
            Pedidos.CadastroSKU(rotina,datainicio)
            controle.salvarStatus(rotina,client_ip,datainicio)
            cpu_percent = psutil.cpu_percent()
            print("Uso da CPU final do processo:", cpu_percent, "%")
            print(f'ETAPA {rotina}- Fim : {controle.obterHoraAtual()}')
            gc.collect()
            memoria_depois = memory_usage()
            print(memoria_depois)

    else:
            cpu_percent = psutil.cpu_percent()
            gc.collect()

            print("Uso da CPU final do processo", cpu_percent, "%")
            memoria_depois = memory_usage()
            print(memoria_depois)
            print(f' :JA EXISTE UMA ATUALIZACAO Dos {rotina}   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo: ({controle.obterHoraAtual()}')


## Funcao de Automacao 2 : Buscando a atualizacao dos pedidos a nivel de sku das 1milhao de ultimas linhas: Duracao media de x Segundos

def AtualizarPedidos(IntervaloAutomacao):
    print(Fore.LIGHTRED_EX+'\nETAPA 2 - ATUALIZACAO DOS PEDIDOS-item-grade ultimos 1 Milhao de linhas ')
    memoria_antes = memory_usage()
    print(memoria_antes)
    rotina = 'pedidosItemgrade'
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'pedidosItemgrade')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print(f'\nETAPA AtualizarPedidos- Inicio: {controle.obterHoraAtual()}')
            controle.InserindoStatus('pedidosItemgrade',client_ip,datainicio)
            Pedidos.IncrementarPedidos(rotina, datainicio)
            controle.salvarStatus('pedidosItemgrade', client_ip, datainicio)
            print(f'ETAPA AtualizarPedidos- FIM: {controle.obterHoraAtual()}')
            gc.collect()
            memoria_depois = memory_usage()
            print(memoria_depois)
    else:
            gc.collect()
            memoria_depois = memory_usage()
            print(memoria_depois)
            print(f'JA EXISTE UMA ATUALIZACAO Dos pedidosItemgrade   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo: {controle.obterHoraAtual()}')

def EliminaPedidosFaturados(IntervaloAutomacao):
        print('\nETAPA 7 - Elimina Pedidos ja Faturados ')

        rotina = 'EliminaPedidosjaFaturados'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'EliminaPedidosjaFaturados')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)

        if tempo > limite:
            controle.InserindoStatus(rotina,client_ip,datainicio)
            print('\nETAPA Elimina Pedidos ja Faturados- Inicio')
            RecarregaPedidos.avaliacaoPedidos(rotina, datainicio)
            controle.salvarStatus(rotina, 'automacao', datainicio)
            print('ETAPA Elimina Pedidos ja Faturados- Fim')
            gc.collect()

        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA Elimina Pedidos ja Faturados EM MENOS DE {IntervaloAutomacao} MINUTOS')
            gc.collect()

def EliminaPedidosFaturadosNivelSKU(IntervaloAutomacao):
        print('\n 8 - Elimina Pedidos Faturados NivelSKU')

        rotina = 'EliminaPedidosjaFaturados'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'EliminaPedidosjaFaturados')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)

        if tempo > limite:
            print('\nETAPA Elimina Pedidos sku Faturados- Inicio')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            RecarregaPedidos.LimpezaPedidosSku(rotina, datainicio)
            controle.salvarStatus(rotina, 'automacao', datainicio)
            print('ETAPA Elimina Pedidos sku Faturados- Fim')
            del limite
            del datainicio
            del tempo
            gc.collect()


        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA Elimina Pedidos FaturadosNivelSKU EM MENOS DE {IntervaloAutomacao} MINUTOS')
            gc.collect()
            del limite
            del datainicio
            del tempo
            gc.collect()


