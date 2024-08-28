import pytz
import empresaConfigurada
from flask import Flask, render_template, jsonify, request,session
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import os
import datetime
import subprocess
from colorama import init, Fore
import gc
from routes.Ops.InformacoesSilkFaccionista import AtualizarOPSilks
from routes.Ops.OP_emAberto import OrdemProducao
from routes.Pedidos import Pedidos, ReservaPreFaturamento
from routes.Pedidos.Pedidos import EliminaPedidosFaturados, EliminaPedidosFaturadosNivelSKU
from routes.Qualidade.MateriaisSubstitutosPorSku import SubstitutosSkuOP
from routes.Qualidade.TecidosDefeitosOP import AtualizarOPSDefeitoTecidos
from routes.TagsFilaReposicao.TagsReposicao import LimpandoTagSaidaReposicao
from routes.TagsReposicaoOff import TagOff
from routes.TagsFilaReposicao import TagsReposicao
from routes.backup import backup
from models.TagsFilaReposicao import TagsTransferidas
import sys
import psutil
import time


"""
NESSE DOCUMENTO .mani é realizado o processo de automacao via python da transferencia PLANEJADA de dados do banco de dados Origem "CACHÉ" do ERP CSW , 
PARA O BANCO DE DADOS DA APLICACAO DE WMS E PORTAL DA QUALIDADE
"""

app = Flask(__name__)
CORS(app) # O CORS garante que o serviço seja apontado via protocolo HTTP
port = int(os.environ.get('PORT', 8000)) # Define a porta 9000 como padrao para essa aplicacao de AUTOMACAO
def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # Retorna o uso de memória em bytes
def obterHoraAtual(): # Funcao criada para obter a data e hora do sistema, convetendo em fuso horario Brasil
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%H')
    return hora_str
def restart_server(): # Funcao que "reseta" a aplicacao para erros de execcao e outras processos
    print("Reiniciando o aplicativo...")
    subprocess.call(["python", "app.py"])

def my_task():
    hora = obterHoraAtual()
    empresa = '1'  # Simulação de chamada empresaConfigurada.EmpresaEscolhida()
    horas_permitidas = {
        '1': ['06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18'],
        '2': ['05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19']
    }

    if empresa == '1' and  hora in ['06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18']:

        gc.collect()
        my_task2()

    elif empresa == '4' and  hora in ['05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19'] :
        gc.collect()
        my_task2()


def my_task2():
        gc.collect()
        memoria_antes = memory_usage()
        print(f'A memoria usanda antes desse ciclo é {memoria_antes}')
        automacao()
        gc.collect()




scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Sao_Paulo'))
scheduler.start()
scheduler.add_job(func=my_task, trigger='interval', seconds=40)


def automacao():
    if empresa == '1':
        memoria_antes = memory_usage()
        print(f'Memoria antes de Atualizar SKU - Etapa 1: {round(memoria_antes / 1000000, 3)} GB')
        Pedidos.AtualizarSKU(60)  # 1 ok
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar SKU - Etapa 1: {round(memoria_apos / 1000000, 3)} GB')

        Pedidos.AtualizarPedidos(60)  # 2
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar Pedidos - Etapa 2: {round(memoria_apos / 1000000, 3)} GB')

        TagOff.atualizatagoff(20)  # 3 ok
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar TagOff - Etapa 3: {round(memoria_apos / 1000000, 3)} GB')

        ReservaPreFaturamento.AtualizaApiReservaFaruamento(90)  # 4 ok
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar ReservaPreFaturamento - Etapa 4: {round(memoria_apos / 1000000, 3)} GB')

        TagsReposicao.AtualizaFilaTagsEstoque(15)  # 5 ok
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar AtualizaFilaTagsEstoque - Etapa 5: {round(memoria_apos / 1000000, 3)} GB')

        TagsReposicao.LimpezaTagsSaidaForaWMS(15)  # 6 ok
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar LimpezaTagsSaidaForaWMS - Etapa 6: {round(memoria_apos / 1000000, 3)} GB')

        EliminaPedidosFaturados(10)  # 7
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar EliminaPedidosFaturados - Etapa 7: {round(memoria_apos / 1000000, 3)} GB')

        EliminaPedidosFaturadosNivelSKU(10)  # 8
        gc.collect()
        memoria_apos = memory_usage()
        print(
            f'Memoria apos Atualizar EliminaPedidosFaturadosNivelSKU - Etapa 8: {round(memoria_apos / 1000000, 3)} GB')

        LimpandoTagSaidaReposicao(10)  # 9
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar LimpandoTagSaidaReposicao - Etapa 9: {round(memoria_apos / 1000000, 3)} GB')

        AtualizarOPSilks(90)  # 10
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar AtualizarOPSilks - Etapa 10: {round(memoria_apos / 1000000, 3)} GB')

        AtualizarOPSDefeitoTecidos(90)  # 11
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar AtualizarOPSDefeitoTecidos - Etapa 11: {round(memoria_apos / 1000000, 3)} GB')

        SubstitutosSkuOP(60)  # 12
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar SubstitutosSkuOP - Etapa 12: {round(memoria_apos / 1000000, 3)} GB')

        OrdemProducao(40)  # 13
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar OrdemProducao - Etapa 13: {round(memoria_apos / 1000000, 3)} GB')

        backup.BackupTabelaPrateleira(90)  # 14
        gc.collect()
        memoria_apos = memory_usage()
        print(f'Memoria apos Atualizar BackupTabelaPrateleira - Etapa 14: {round(memoria_apos / 1000000, 3)} GB')

    elif empresa == '4':


        TagsReposicao.AtualizaFilaTagsEstoque(15)

        gc.collect()

        TagsReposicao.LimpezaTagsSaidaForaWMS(15)
        gc.collect()

        EliminaPedidosFaturados(10)
        gc.collect()

        EliminaPedidosFaturadosNivelSKU(10)
        gc.collect()

        LimpandoTagSaidaReposicao(10)
        gc.collect()

        backup.BackupTabelaPrateleira(90)
        gc.collect()

        SubstitutosSkuOP(60)  # 12
        gc.collect()

        OrdemProducao(10)  # 13
        gc.collect()

        os.system('clear')
        pid = os.getpid()
        print(f"Encerrando processo com PID: {pid}")
        # Iniciar nova instância do script após 5 segundos
        new_process = f"{sys.executable} {sys.argv[0]}"
        os.system(f"sleep 60 && {new_process} &")

        # Encerrar o processo atual
        p = psutil.Process(pid)
        p.terminate()



    else:
        print('sem empresa selecionada')


# INICIANDO O PROCESSO DE AUTOMACAO
if __name__ == '__main__':
    print('\n################# INICIANDO A AUTOMACAO DOS DADOS ########################### \n')
    empresa = empresaConfigurada.EmpresaEscolhida()  # Busca a empresa que a aplicacao de automaca vai rodar
    print(f'\n Estamaos na empresa: {empresa}\nno PID {os.getpid()}')

    # Etapa 1: Comaça a rodar a automacao pelas etapas, de acordo com a empresa ("Algumas empresa possuem regras diferentes de uso dai essa necessidade")

    automacao()

    # Etapa 2: Liga a automacao do my_task que é uma funcao de AGENDAMENTO DE PROCESSOS
    try:
        my_task()

    except Exception as e:  # Caso ocorra erros, a automacao é reiniciada
        print(f"Erro detectado: {str(e)}")
        restart_server()

    app.run(host='0.0.0.0', port=port)