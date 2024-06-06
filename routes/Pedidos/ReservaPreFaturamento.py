## Funcao de Automacao 4 : Nessa etapa é acionada a API do CSW que faz o processo de AtualizaReserva das Sugestoes em aberto de acordo com a politica definida
import gc
import requests
import controle


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
            gc.collect()
            del dados_dict
            del etapa1

        else:
            print(f'AtualizaApiReservaFaruamento : erro  {response.status_code} ')
            etapa1 = controle.salvarStatus_Etapa1('AtualizaApiReservaFaruamento', 'automacao', datainicio, f'resposta {response.status_code}')
            controle.salvarStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)
            gc.collect()
            del etapa1
