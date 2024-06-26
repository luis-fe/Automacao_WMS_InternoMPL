## Funcao de Automacao 5 : Atualiza as tags no status em estoque para o WMS
import gc

import controle
import empresaConfigurada
from models.Pedidos.RecarregaPedidos import avaliacaoReposicao
from models.TagsFilaReposicao import RecarregarBanco, TagsTransferidas


def AtualizaFilaTagsEstoque(IntervaloAutomacao):
        print('\nETAPA 5 - Atualiza Fila das Tags para Repor na situacao em ESTOQUE ')



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
            gc.collect()

        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! ja tinha atualizacao congelada')
            gc.collect()
## Funcao de Automacao 6 : Limpando Tags que sairam do estoque sem ser via WMS

def LimpezaTagsSaidaForaWMS(IntervaloAutomacao):
        print('\nETAPA 6 - LimpezaTagsSaidaForaWMS ')

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
            gc.collect()

        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA LimpezaTagsSaidaForaWMS EM MENOS DE {IntervaloAutomacao} MINUTOS')
            gc.collect()
## Funcao de Automacao 9: Atualizar :

def LimpandoTagSaidaReposicao(IntervaloAutomacao):
        print('\n 9- Limpando as saidas de Tags Repostas fora do WMS')

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
            avaliacaoReposicao(rotina, datainicio)
            TagsTransferidas.transferirTags(4)
            controle.salvarStatus(rotina, client_ip, datainicio)
            print('ETAPA LimpandoTagSaidaReposicao- Fim')
            gc.collect()
        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA  Limpando Tag Saida da Reposicao EM MENOS DE {IntervaloAutomacao} MINUTOS')
            gc.collect()
