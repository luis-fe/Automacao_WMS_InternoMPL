import gc
from colorama import Fore
from flask import jsonify
import controle
from models.TagsReposicaoOff import TagOff


## Funcao de Automacao 3 : Buscando a atualizacao das tag's em situacao gerada, disponibiliza os dados da situacao de tags em aberta : Duracao media de x Segundos
def atualizatagoff(IntervaloAutomacao):
    print(Fore.WHITE+'\nETAPA 3 - Atualiza tag off (disponibiliza os dados da situacao de tags em aberta ) ')
    try:
        rotina = 'atualiza tag off'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'atualiza tag off')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
            controle.InserindoStatus(rotina, client_ip, datainicio)
            TagOff.TagsOFF('1', rotina, client_ip, datainicio)
            controle.salvarStatus('atualiza tag off', client_ip, datainicio)
            gc.collect()


        else:
            print('JA EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE 1 HORA - 60 MINUTOS')
            gc.collect()


    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})