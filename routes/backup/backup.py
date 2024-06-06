import gc
import controle
from models.backup import backupWMS


def BackupTabelaPrateleira(IntervaloAutomacao):
    print('\nETAPA 15 - ATUALIZACAO DO Backuptagsreposicao')
    client_ip = 'automacao'
    rotina  = 'Backuptagsreposicao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'Backuptagsreposicao')
    limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print(f'\nETAPA {rotina}- Inicio')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            backupWMS.Backuptagsreposicao()
            controle.salvarStatus(rotina,client_ip,datainicio)
            print(f'ETAPA {rotina}- Fim : {controle.obterHoraAtual()}')
            gc.collect()

    else:
            print(f' :JA EXISTE UMA ATUALIZACAO Dos {rotina}   EM MENOS DE {IntervaloAutomacao} MINUTOS, limite de intervalo de tempo: ({controle.obterHoraAtual()}')
            gc.collect()
