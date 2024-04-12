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
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AutomacaoCadastroSKU')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print('\nETAPA AutomacaoCadastroSKU- Inicio')
            controle.InserindoStatus('AutomacaoCadastroSKU',client_ip,datainicio)
            pedios.CadastroSKU('AutomacaoCadastroSKU',datainicio)
            controle.salvarStatus('AutomacaoCadastroSKU',client_ip,datainicio)
            print('ETAPA AutomacaoCadastroSKU- Fim')
    else:
            print(f'JA EXISTE UMA ATUALIZACAO Dos AutomacaoCadastroSKU   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo')


## Funcao de Automacao 2 : Buscando a atualizacao dos pedidos a nivel de sku das 1milhao de ultimas linhas: Duracao media de x Segundos

def AtualizarPedidos(IntervaloAutomacao):
    print('\nETAPA 2 - ATUALIZACAO DOS PEDIDOS-item-grade ultimos 1 Milhao de linhas ')
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'pedidosItemgrade')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print('\nETAPA AtualizarPedidos- Inicio')

            pedios.IncrementarPedidos()
            controle.salvar('pedidosItemgrade', client_ip, datainicio)
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
            controle.salvar('AtualizaApiReservaFaruamento', client_ip, datainicio)
            print('ETAPA AtualizaApiReservaFaruamento- Fim')
        else:
            print(f'AtualizaApiReservaFaruamento : erro  {response.status_code} ')

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
            tamnho1, datahora1 = RecarregarBanco.FilaTags(datainicio, rotina)
            controle.salvarStatus('fila Tags Reposicao','automacao',datainicio)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')

        elif empresa == '4':
            tamnho1, datahora1 = RecarregarBanco.FilaTags(datainicio, rotina)

            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! {tamnho1} tags foram adicionadas, as {datahora1}')
        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! ja tinha atualizacao congelada')
    except:
        print(' 1.1.1 falha na automacao - Fila Reposicao \n Atenção! 0 tags foram adicionadas')


## Funcao de Automacao 6 : Limpando Tags que sairam do estoque sem ser via WMS

def LimezaTagsSaidaForaWMS(IntervaloAutomacao):
    print('\nETAPA 6 - LimezaTagsSaidaForaWMS ')

    try:
        # coloque o código que você deseja executar continuamente aqui
        rotina = 'fila Tags Reposicao'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'fila Tags Reposicao')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)

        # coloque o código que você deseja executar continuamente aqui
        tamanho2, datahora2 = RecarregarBanco.avaliacaoFila()
        print(f' 2.1- Sucesso - avaliacao Fila Reposicao \n Atencao!{tamanho2} tags foram eliminadas, as {datahora2}')
    except:
        print(' 2.1.1 falha na automacao - avaliacao Fila Reposicao')


## Funcao de Automacao 7 : Elimina Pedidos ja Faturados
def EliminaPedidosFaturados():
    print('\n 3 - Limpando os Pedidos Faturados da Fila')
    try:
        # coloque o código que você deseja executar continuamente aqui
        tamnho3, datahora3 = RecarregaPedidos.avaliacaoPedidos()
        print(f' 3.1 Sucesso - avaliacao Fila Pedidos \n Atenção!  {tamnho3} pedidos eliminados, as {datahora3}')
    except:
        print(' 3.1.1 falha na automacao - avaliacao Fila Pedidos')

## Funcao de Automacao 8 :Elimina Pedidos Faturados NivelSKU

def EliminaPedidosFaturadosNivelSKU():
    print('\n 6 - Limpando PedidoSku')
    try:
        # coloque o código que você deseja executar continuamente aqui
        datahora6 = RecarregaPedidos.LimpezaPedidosSku()
        print(f'6.1 Sucesso - Pedios Faturados Limpados do PedidoSKU, as {datahora6}')
    except:
        print(f'6.1.1 falha na automacao - Limpeza PedidosSku')

## Funcao de Automacao 9: Atualizar :

def LimpandoTag():
    print('\n 7- Limpando as saidas de Tags Repostas fora do WMS')
    try:
        # coloque o código que você deseja executar continuamente aqui
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'avaliacaoReposicao')
        limite = 5 * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()
        if tempo > limite and empresa == '1':

            tamnho7, datahora7 = RecarregaPedidos.avaliacaoReposicao()
            controle.salvar('avaliacaoReposicao', client_ip, datainicio)

            print(f'7.1 Sucesso - Limpando Saida da Reposicao \nAtenção!  {tamnho7} tags limpadas, as {datahora7}')
        else:
            print(f'7.1 Sucesso - Limpando Saida da Reposicao \nCongelado')

    except:
        print(f'7.1.1 falha na automacao - Limpando Saida da Reposicao')

## Funcao de Automacao 10: Atualizar Ops Silk :

def AtualizarOPSilks():
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPsSilksFaccionista')
    limite = 30 * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            InformacoesSilkFaccionista.ObterOpsEstamparia()
            controle.salvar('OPsSilksFaccionista', client_ip, datainicio)

    else:

            print('JA EXISTE UMA ATUALIZACAO Dos OPsSilksFaccionista   EM MENOS DE 1 HORA - 60 MINUTOS')


## Funcao de Automacao 11: AtualizarOPSDefeitoTecidos  :


def AtualizarOPSDefeitoTecidos():
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPSDefeitoTecidos')
    limite = 30 * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            TecidosDefeitosOP.DefeitosTecidos()
            controle.salvar('OPSDefeitoTecidos', client_ip, datainicio)

    else:

            print('JA EXISTE UMA ATUALIZACAO Dos OPSDefeitoTecidos   EM MENOS DE 1 HORA - 60 MINUTOS')

## Funcao de Automacao 12: SubstitutosSkuOP  :

def SubstitutosSkuOP():
    print('\n 12 - Salvando as OPs que tiveram substitutos')

    try:
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'SubstitutosSkuOP')
        limite = 60 * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
            MateriaisSubstitutosPorSku.SubstitutosSkuOP()
            controle.salvar('SubstitutosSkuOP', client_ip, datainicio)

        else:

            print('JA EXISTE UMA ATUALIZACAO Dos SubstitutosSkuOP   EM MENOS DE 1 HORA - 60 MINUTOS')

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 13: Ordem Producao  :

def OrdemProducao():
    print('\n 11 - Importando as Ordem de Producao')

    try:
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'ordem de producao')
        limite = 10 * 60  # (limite de 10 minutos , convertido para segundos)
        if tempo > limite:
            OP_emAberto.IncrementadoDadosPostgre('1')
            controle.salvar('ordem de producao', client_ip, datainicio)

        else:

            print('JA EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE 1 HORA - 60 MINUTOS')
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})

## Funcao de Automacao 14: Ordem Producao  :

def RemoveDuplicatasUsuario():
    print('\n 9- TratamentoErrosDuplicacoes')
    try:
        datahora9 = TratamentoErro.RemoveDuplicatasUsuario()
        print(f'9.1 Sucesso - Limpeza de Duplicatas Usuario Atribuido na Reposicao, as {datahora9}')
    except:
        print('9.1.1 Falha na automacao - Tratamento de Erros')


def my_task():
    hora = obterHoraAtual()

    if hora in ['07','08','09','10', '11', '12', '13', '14', '15', '16','17','18']:
        my_task2()
    else:
        print('Aguardando a Data e Hora correto '+hora)


def my_task2():
    print('\n ###  Inicando cilco de Automacao  do WMS ###\n ')



    empresa = empresaConfigurada.EmpresaEscolhida()
    if empresa == '10':
        AtualizarOPSilks()
        AtualizarSKU(30)
        AtualizarPedidos(60)
        AtualizaApiReservaFaruamento(60)
        SubstitutosSkuOP()
        atualizatagoff()
        OrdemProducao()
        AtualizarOPSDefeitoTecidos()
    else:
        print(empresa)


        print('Fim do Ciclo')
        restart_server()



scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Sao_Paulo'))
scheduler.add_job(func=my_task, trigger='interval', seconds=300)
scheduler.start()


# INICIANDO O PROCESSO DE AUTOMACAO
if __name__ == '__main__':

    print('\n#################              INICIANDO A AUTOMACAO DOS DADOS        ########################### \n')
    empresa = empresaConfigurada.EmpresaEscolhida() #Busca a empresa que a aplicacao de automaca vai rodar
    print(f'\n Estamaos na empresa: {empresa}\n')

    # Etapa 1: Comeca a rodar a automacao pela etapas, de acordo com a empresa ("Algumas empresa possuem regras diferentes de uso dai essa necessidade")
    if empresa == '1':
        AtualizarSKU(30)
    elif empresa == '4':
        AtualizarSKU(30)

    else:
        print('sem empresa selecionada')


    # Etapa 2: Liga a automacao do my_task que é uma funcao de AGENDAMENTO DE PROCESSOS
    try:
        my_task()

    except Exception as e: #Caso ocorre erros a automacao é reiniciada
        print(f"Erro detectado: {str(e)}")
        restart_server()


app.run(host='0.0.0.0', port=port)
