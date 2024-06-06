import gc
import controle
from models.Ops import InformacoesSilkFaccionista


def AtualizarOPSilks(IntervaloAutomacao):
        print('\n 10- ATUALIZA OP Silks Faccionistas')


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
                gc.collect()



        else:
            print(f'JA EXISTE UMA ATUALIZACAO Dos OPs SilksFaccionista   EM MENOS DE {IntervaloAutomacao} MINUTOS')
            gc.collect()

