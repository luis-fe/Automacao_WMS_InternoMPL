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
import ControleClass


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

        verificador = []
        TagsReposicao.AtualizaFilaTagsEstoque(5)

        gc.collect()

        TagsReposicao.LimpezaTagsSaidaForaWMS(15)
        gc.collect()
        verificador.append(True)

        EliminaPedidosFaturados(10)
        gc.collect()
        verificador.append(True)


        EliminaPedidosFaturadosNivelSKU(10)
        gc.collect()
        verificador.append(True)


        LimpandoTagSaidaReposicao(10)
        gc.collect()
        verificador.append(True)


        backup.BackupTabelaPrateleira(90)
        gc.collect()
        verificador.append(True)


        SubstitutosSkuOP(60)  # 12
        gc.collect()
        verificador.append(True)


        OrdemProducao(10)  # 13
        gc.collect()
        verificador.append(True)

    else:
        print('sem empresa selecionada')



# INICIANDO O PROCESSO DE AUTOMACAO
if __name__ == '__main__':
    print('\n################# INICIANDO A AUTOMACAO DOS DADOS ########################### \n')
    empresa = empresaConfigurada.EmpresaEscolhida()  # Busca a empresa que a aplicacao de automaca vai rodar
    print(f'\n Estamaos na empresa: {empresa}\nno PID {os.getpid()}!')

    PID = os.getpid()
    '''Instanciando o objeto controle para controlar o registro da automacao'''
    controle = ControleClass.Controle(empresa,PID, False)
    controle.inserirNovoPID()

    # Etapa 1: Comaça a rodar a automacao pelas etapas, de acordo com a empresa ("Algumas empresa possuem regras diferentes de uso dai essa necessidade")

    try:
        automacao()
    except:
        print('erro')
    os.system('clear')



    # Iniciar nova instância do script após N segundos
    new_process = f"{sys.executable} {sys.argv[0]}"
    print(f'gerado o process {new_process}')
    os.system(f"sleep 180 && {new_process} &")

    #Encerrando o Registro de controle do PID
    controle.excluirPID()
    #Encerrando o  PID atual
    p = psutil.Process(PID)
    p.terminate()

    app.run(host='0.0.0.0', port=port)