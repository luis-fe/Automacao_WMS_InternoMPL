import gc

import controle
from models.Qualidade import TecidosDefeitosOP


## Funcao de Automacao 11: AtualizarOPSDefeitoTecidos  :
def AtualizarOPSDefeitoTecidos(IntervaloAutomacao):
        print('\n 11- ATUALIZA  OPS Defeito Tecidos')

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
                gc.collect()



        else:
            print(f'JA EXISTE UMA ATUALIZACAO das OPS Defeito Tecidos   EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()



