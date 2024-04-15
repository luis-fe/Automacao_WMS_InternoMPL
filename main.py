import pytz
import controle
import empresaConfigurada
from InformacoesOPCsw import OP_emAberto, MateriaisSubstitutosPorSku, InformacoesSilkFaccionista, TecidosDefeitosOP, pedios
import RecarregaPedidos
import RecarregarBanco
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from functools import wraps
from apscheduler.schedulers.background import BackgroundScheduler
import os
import datetime
import TagsDisponivelGarantia
import TratamentoErro
import Usuarios
import subprocess
import subprocess
import requests
from models.backup import backupWMS
"""
NESSE DOCUMENTO .mani é realizado o processo de automacao via python da transferencia PLANEJADA de dados do banco de dados Origem "CACHÉ" do ERP CSW , 
PARA O BANCO DE DADOS DA APLICACAO DE WMS E PORTAL DA QUALIDADE
"""


app = Flask(__name__) # DEFINE A VARIAVEL app como o caminho para excecutar o FrameWork Flask
CORS(app) # O CORS garante que o serviço seja apontado via protocolo HTTP
port = int(os.environ.get('PORT', 9000)) # Define a porta 9000 como padrao para essa aplicacao de AUTOMACAO


def obterHoraAtual(): # Funcao criada para obter a data e hora do sistema, convetendo em fuso horario Brasil
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%H')
    return hora_str

def restart_server(): # Funcao que "reseta" a aplicacao para erros de execcao e outras processos
    print("Reiniciando o aplicativo...")
    subprocess.call(["python", "main.py"])


## Funcao de Automacao 1 : Buscando a atualizacao dos SKUs: Duracao media de 30 Segundos
def AtualizarSKU(IntervaloAutomacao):
    print('\nETAPA 1 - ATUALIZACAO DO AutomacaoCadastroSKU')
    client_ip = 'automacao'
    rotina  = 'AutomacaoCadastroSKU'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoCadastroSKU')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print(f'\nETAPA {rotina}- Inicio')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            pedios.CadastroSKU(rotina,datainicio)
            controle.salvarStatus(rotina,client_ip,datainicio)
            print(f'ETAPA {rotina}- Fim : {controle.obterHoraAtual()}')
    else:
            print(f' :JA EXISTE UMA ATUALIZACAO Dos {rotina}   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo: ({controle.obterHoraAtual()}')


## Funcao de Automacao 2 : Buscando a atualizacao dos pedidos a nivel de sku das 1milhao de ultimas linhas: Duracao media de x Segundos

def AtualizarPedidos(IntervaloAutomacao):
    print('\nETAPA 2 - ATUALIZACAO DOS PEDIDOS-item-grade ultimos 1 Milhao de linhas ')
    rotina = 'pedidosItemgrade'
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'pedidosItemgrade')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print('\nETAPA AtualizarPedidos- Inicio')
            controle.InserindoStatus('pedidosItemgrade',client_ip,datainicio)
            pedios.IncrementarPedidos(rotina, datainicio)
            controle.salvarStatus('pedidosItemgrade', client_ip, datainicio)
            print('ETAPA AtualizarPedidos- FIM')

    else:
            print(f'JA EXISTE UMA ATUALIZACAO Dos pedidosItemgrade   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo')

## Funcao de Automacao 3 : Buscando a atualizacao das tag's em situacao gerada, disponibiliza os dados da situacao de tags em aberta : Duracao media de x Segundos
def atualizatagoff(IntervaloAutomacao):
    print('\nETAPA 3 - Atualiza tag off (disponibiliza os dados da situacao de tags em aberta ) ')
    try:
        rotina = 'atualiza tag off'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'atualiza tag off')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
            controle.InserindoStatus(rotina, client_ip, datainicio)
            TagsDisponivelGarantia.SalvarTagsNoBancoPostgre(rotina, client_ip, datainicio)
            controle.salvarStatus('atualiza tag off', client_ip, datainicio)

        else:
            print('JA EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE 1 HORA - 60 MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 4 : Nessa etapa é acionada a API do CSW que faz o processo de AtualizaReserva das Sugestoes em aberto de acordo com a politica definida
def AtualizaApiReservaFaruamento(IntervaloAutomacao):
    print('\nETAPA 4 - Atualiza Api ReservaFaruamento')
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AtualizaApiReservaFaruamento')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
        controle.InserindoStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)
        print('ETAPA AtualizaApiReservaFaruamento- Inicio')
        url = 'http://192.168.0.183:8000/pcp/api/ReservaPreFaturamento'

        token = "a44pcp22"


        # Defina os parâmetros em um dicionário

        # Defina os headers
        headers = {
            'accept': 'application/json',
            'Authorization': f'{token}'
        }


        # Faça a requisição POST com parâmetros e headers usando o método requests.post()
        response = requests.get(url,  headers=headers,  verify=False)
        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            # Converter os dados JSON em um dicionário
            dados_dict = response.json()
            etapa1 = controle.salvarStatus_Etapa1('AtualizaApiReservaFaruamento', 'automacao', datainicio, 'resposta 200 ok')
            controle.salvarStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)
            print('ETAPA AtualizaApiReservaFaruamento- Fim')
        else:
            print(f'AtualizaApiReservaFaruamento : erro  {response.status_code} ')
            etapa1 = controle.salvarStatus_Etapa1('AtualizaApiReservaFaruamento', 'automacao', datainicio, f'resposta {response.status_code}')
            controle.salvarStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)


## Funcao de Automacao 5 : Atualiza as tags no status em estoque para o WMS
def AtualizaFilaTagsEstoque(IntervaloAutomacao):
    print('\nETAPA 5 - Atualiza Fila das Tags para Repor na situacao em ESTOQUE ')

    try:

        # coloque o código que você deseja executar continuamente aqui
        rotina = 'fila Tags Reposicao'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'fila Tags Reposicao')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()
        if tempo > limite and empresa == '1':

            controle.InserindoStatus(rotina,client_ip,datainicio)
            RecarregarBanco.FilaTags(datainicio, rotina)
            controle.salvarStatus('fila Tags Reposicao','automacao',datainicio)
            print('ETAPA fila Tags Reposicao- Fim')

        elif empresa == '4':
            RecarregarBanco.FilaTags(datainicio, rotina)
            print('ETAPA fila Tags Reposicao- Fim')
        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! ja tinha atualizacao congelada')


    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})


## Funcao de Automacao 6 : Limpando Tags que sairam do estoque sem ser via WMS

def LimpezaTagsSaidaForaWMS(IntervaloAutomacao):
    print('\nETAPA 6 - LimpezaTagsSaidaForaWMS ')

    try:
        # coloque o código que você deseja executar continuamente aqui
        rotina = 'LimpezaTagsSaidaForaWMS'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'LimpezaTagsSaidaForaWMS')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)

        if tempo > limite:
            controle.InserindoStatus(rotina,client_ip,datainicio)
            print('\nETAPA LimpezaTagsSaidaForaWMS- Inicio')
            RecarregarBanco.avaliacaoFila(rotina, datainicio)
            controle.salvarStatus(rotina,'automacao',datainicio)
            print('ETAPA LimpezaTagsSaidaForaWMS- Fim')
        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA LimpezaTagsSaidaForaWMS EM MENOS DE {IntervaloAutomacao} MINUTOS')


    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})


## Funcao de Automacao 7 : Elimina Pedidos ja Faturados
def EliminaPedidosFaturados(IntervaloAutomacao):
    print('\nETAPA 7 - Elimina Pedidos ja Faturados ')
    try:
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
        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA Elimina Pedidos ja Faturados EM MENOS DE {IntervaloAutomacao} MINUTOS')
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 8 :Elimina Pedidos Faturados NivelSKU

def EliminaPedidosFaturadosNivelSKU(IntervaloAutomacao):
    print('\n 8 - Elimina Pedidos Faturados NivelSKU')
    try:
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

        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA Elimina Pedidos FaturadosNivelSKU EM MENOS DE {IntervaloAutomacao} MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 9: Atualizar :

def LimpandoTagSaidaReposicao(IntervaloAutomacao):
    print('\n 9- Limpando as saidas de Tags Repostas fora do WMS')
    try:
        # coloque o código que você deseja executar continuamente aqui
        rotina = 'LimpandoTagSaidaReposicao'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'avaliacaoReposicao')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()

        if tempo > limite and empresa == '1':
            print('\nETAPA LimpandoTagSaidaReposicao- Inicio')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            RecarregaPedidos.avaliacaoReposicao(rotina, datainicio)
            controle.salvarStatus(rotina, client_ip, datainicio)
            print('ETAPA LimpandoTagSaidaReposicao- Fim')


        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA  Limpando Tag Saida da Reposicao EM MENOS DE {IntervaloAutomacao} MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 10: Atualizar Ops Silk :
def AtualizarOPSilks(IntervaloAutomacao):
    print('\n 10- ATUALIZA OP Silks Faccionistas')

    try:
        rotina ='OPsSilksFaccionista'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPsSilksFaccionista')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
                print('\nETAPA Atualizar OP de Silks- Inicio')
                controle.InserindoStatus(rotina, client_ip, datainicio)
                InformacoesSilkFaccionista.ObterOpsEstamparia(rotina, datainicio)
                controle.salvarStatus('OPsSilksFaccionista', client_ip, datainicio)
                print('ETAPA Atualizar OP de Silks- Fim')


        else:
                print(f'JA EXISTE UMA ATUALIZACAO Dos OPs SilksFaccionista   EM MENOS DE {IntervaloAutomacao} MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})


## Funcao de Automacao 11: AtualizarOPSDefeitoTecidos  :
def AtualizarOPSDefeitoTecidos(IntervaloAutomacao):
    print('\n 11- ATUALIZA  OPS Defeito Tecidos')

    try:
        rotina = 'OPSDefeitoTecidos'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPSDefeitoTecidos')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
                print('\nETAPA Atualizar OPS Defeito Tecidos- Inicio')
                controle.InserindoStatus(rotina, client_ip, datainicio)
                TecidosDefeitosOP.DefeitosTecidos(rotina, datainicio)
                controle.salvarStatus('OPSDefeitoTecidos', client_ip, datainicio)
                print('ETAPA Atualizar OPS Defeito Tecidos- Fim')


        else:
                print(f'JA EXISTE UMA ATUALIZACAO das OPS Defeito Tecidos   EM MENOS DE - {IntervaloAutomacao} MINUTOS')
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 12: SubstitutosSkuOP  :
def SubstitutosSkuOP(IntervaloAutomacao):
    print('\n 12 - Salvando as OPs que tiveram substitutos')

    try:
        rotina = 'SubstitutosSkuOP'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'SubstitutosSkuOP')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)

        if tempo > limite:
            print('\nETAPA Atualizar Substitutos Sku OP- Inicio')
            controle.InserindoStatus(rotina, client_ip, datainicio)
            MateriaisSubstitutosPorSku.SubstitutosSkuOP(rotina, datainicio)
            controle.salvarStatus('SubstitutosSkuOP', client_ip, datainicio)
            print('ETAPA Atualizar Substitutos Sku OP- Final')

        else:

            print(f'JA EXISTE UMA ATUALIZACAO dos Substitutos Sku por OP EM MENOS DE - {IntervaloAutomacao} MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 13: Ordem Producao  :

def OrdemProducao(IntervaloAutomacao):
    print('\n 13 - Importando as Ordem de Producao')
    try:
        rotina = 'ordem de producao'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ordem de producao')
        limite = IntervaloAutomacao * 60  # (limite de 10 minutos , convertido para segundos)

        if tempo > limite:
            print('\nETAPA importando Ordem de Producao- Inicio')
            controle.InserindoStatus(rotina, client_ip, datainicio)
            OP_emAberto.IncrementadoDadosPostgre('1', rotina, datainicio)
            controle.salvarStatus(rotina, client_ip, datainicio)
            print('ETAPA importando Ordem de Producao- Fim')
        else:
            print(f'JA EXISTE UMA ATUALIZACAO em importando Ordem de Producao EM MENOS DE - {IntervaloAutomacao} MINUTOS')
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 14: Ordem Producao  :

def RemoveDuplicatasUsuario(IntervaloAutomacao):
    print('\n 14- RemoveDuplicatasUsuario')
    try:
        rotina = 'RemoveDuplicatasUsuario'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, rotina)
        limite = IntervaloAutomacao * 60  # (limite de 10 minutos , convertido para segundos)

        if tempo > limite:
            print('\nETAPA RemoveDuplicatasUsuario- Inicio')
            controle.InserindoStatus(rotina, client_ip, datainicio)
            TratamentoErro.RemoveDuplicatasUsuario(rotina, datainicio)
            controle.salvarStatus(rotina, client_ip, datainicio)

            print('ETAPA RemoveDuplicatasUsuario- Fim')
        else:
            print(
                f'JA EXISTE UMA ATUALIZACAO em Remove Duplicatas Usuario EM MENOS DE - {IntervaloAutomacao} MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

def BackupTabelaPrateleira():
    backupWMS.Backuptagsreposicao()


def my_task():
    hora = obterHoraAtual()

    if hora in ['07','08','09','10', '11', '12', '13', '14', '15', '16','17','18']:
        my_task2()
    else:
        print('Aguardando a Data e Hora correto '+hora)


def my_task2():
    print('\n ###  Inicando cilco de Automacao  do WMS ###\n ')



    empresa = empresaConfigurada.EmpresaEscolhida()
    if empresa == '1':
        AtualizarSKU(60) #1
        AtualizarPedidos(60) #2
        atualizatagoff(20) #3
        AtualizaApiReservaFaruamento(90) #4
        AtualizaFilaTagsEstoque(15) #5
        LimpezaTagsSaidaForaWMS(15) #6
        EliminaPedidosFaturados(10) #7
        EliminaPedidosFaturadosNivelSKU(10) #8
        LimpandoTagSaidaReposicao(10) #9
        AtualizarOPSilks(90) #10
        AtualizarOPSDefeitoTecidos(90) #11
        SubstitutosSkuOP(60) #12
        OrdemProducao(40) #13

    else:
        AtualizaFilaTagsEstoque(15)
        LimpezaTagsSaidaForaWMS(15)
        EliminaPedidosFaturados(10)
        EliminaPedidosFaturadosNivelSKU(10)
        LimpandoTagSaidaReposicao(10)

        print(empresa)

        print('Fim do Ciclo')



scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Sao_Paulo'))
scheduler.add_job(func=my_task, trigger='interval', seconds=300)
scheduler.start()


# INICIANDO O PROCESSO DE AUTOMACAO
if __name__ == '__main__':

    print('\n################# INICIANDO A AUTOMACAO DOS DADOS ########################### \n')
    empresa = empresaConfigurada.EmpresaEscolhida() #Busca a empresa que a aplicacao de automaca vai rodar
    print(f'\n Estamaos na empresa: {empresa}\n')

    # Etapa 1: Comeca a rodar a automacao pela etapas, de acordo com a empresa ("Algumas empresa possuem regras diferentes de uso dai essa necessidade")

    if empresa == '1':
        BackupTabelaPrateleira()

        AtualizarSKU(60) #1
        AtualizarPedidos(60) #2
        atualizatagoff(20) #3
        AtualizaApiReservaFaruamento(90) #4
        AtualizaFilaTagsEstoque(15) #5
        LimpezaTagsSaidaForaWMS(15) #6
        EliminaPedidosFaturados(10) #7
        EliminaPedidosFaturadosNivelSKU(10) #8
        LimpandoTagSaidaReposicao(10) #9
        AtualizarOPSilks(90) #10
        AtualizarOPSDefeitoTecidos(90) #11
        SubstitutosSkuOP(60) #12
        OrdemProducao(40) #13



    elif empresa == '4':
        AtualizaFilaTagsEstoque(15)
        LimpezaTagsSaidaForaWMS(15)
        EliminaPedidosFaturados(10)
        EliminaPedidosFaturadosNivelSKU(10)
        LimpandoTagSaidaReposicao(10)




        print('empresa 4')

    else:
        print('sem empresa selecionada')

    # Etapa 2: Liga a automacao do my_task que é uma funcao de AGENDAMENTO DE PROCESSOS
    try:
        my_task()

    except Exception as e: #Caso ocorre erros a automacao é reiniciada
        print(f"Erro detectado: {str(e)}")
        restart_server()


app.run(host='0.0.0.0', port=port)
