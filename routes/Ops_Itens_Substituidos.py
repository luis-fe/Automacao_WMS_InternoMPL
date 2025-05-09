## Funcao de Automacao 12: SubstitutosSkuOP  :
import gc
import controle
from models import Ops_Itens_Substituidos


def SubstitutosSkuOP(IntervaloAutomacao):

        rotina = 'SubstitutosSkuOP'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'SubstitutosSkuOP')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)

        if tempo > limite:
            print('\nETAPA Atualizar Substitutos Sku OP- Inicio')
            controle.InserindoStatus(rotina, client_ip, datainicio)

            itensSubstitutos = Ops_Itens_Substituidos.Ops_Itens_Substituidos(datainicio, rotina, '1')
            itensSubstitutos.substitutosSkuOP()
            #itensSubstitutos.atualizarEPCFaccoes()
            controle.salvarStatus('SubstitutosSkuOP', client_ip, datainicio)
            print('ETAPA Atualizar Substitutos Sku OP- Final')
            gc.collect()


        else:

            print(f'JA EXISTE UMA ATUALIZACAO dos Substitutos Sku por OP EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()

