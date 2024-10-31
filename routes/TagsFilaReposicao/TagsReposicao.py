## Funcao de Automacao 5 : Atualiza as tags no status em estoque para o WMS
import gc

import controle
import empresaConfigurada
from models.Pedidos.RecarregaPedidos import avaliacaoReposicao
from models.TagsFilaReposicao import RecarregarBanco, TagsTransferidas


def AtualizaFilaTagsEstoque(IntervaloAutomacao):
        print('\nETAPA 5 - Atualiza Fila das Tags para Repor na situacao em ESTOQUE ')



        # coloque o código que você deseja executar continuamente aqui
        rotina = 'AtualizarTagsEstoque'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'AtualizarTagsEstoque')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()
        if tempo > limite and empresa == '1':

            controle.InserindoStatus(rotina,client_ip,datainicio)
            automacao = RecarregarBanco.AutomacaoFilaTags('1')
            automacao.recarregarTags(rotina, datainicio)
            controle.salvarStatus('AtualizarTagsEstoque','automacao',datainicio)
            del(automacao)
            print('ETAPA fila Tags Reposicao- Fim')

        elif empresa == '4' and tempo > limite :
            controle.InserindoStatus(rotina,client_ip,datainicio)
            automacao = RecarregarBanco.AutomacaoFilaTags('4')
            automacao.recarregarTags(rotina, datainicio)
            automacao.atualizarEmpresa()
            controle.salvarStatus('AtualizarTagsEstoque','automacao',datainicio)
            print('ETAPA fila Tags Reposicao- Fim')
            gc.collect()

        else:
            print(f'    1.1 Sucesso - Fila das Tag \n   Atenção! ja tinha atualizacao congelada')
            gc.collect()
## Funcao de Automacao 6 : Limpando Tags que sairam do estoque sem ser via WMS

def LimpezaTagsSaidaForaWMS(IntervaloAutomacao, empresa = '4'):
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
            automacao = RecarregarBanco.AutomacaoFilaTags()
            automacao.avaliacaoFila(rotina, datainicio)
            if empresa == '4':
                automacao.atualizaNatureza()
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
        tempo = controle.TempoUltimaAtualizacao(datainicio, rotina)
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        empresa = empresaConfigurada.EmpresaEscolhida()

        if tempo > limite and empresa == '1':
            print('\nETAPA LimpandoTagSaidaReposicao- Inicio')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            avaliacaoReposicao(rotina, datainicio)

            tranferencia = TagsTransferidas.AutomacaoTransferenciaTags(1)
            tranferencia.transferirTagsParaFila()

            controle.salvarStatus(rotina, client_ip, datainicio)
            print('ETAPA LimpandoTagSaidaReposicao- Fim')
            gc.collect()


        elif tempo > limite and empresa == '4':
            print('\nETAPA LimpandoTagSaidaReposicao- Inicio')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            avaliacaoReposicao(rotina, datainicio)

            tranferencia = TagsTransferidas.AutomacaoTransferenciaTags(4)
            tranferencia.transferirTagsParaFila()
            tranferencia.mudancaNatureza()

            controle.salvarStatus(rotina, client_ip, datainicio)
            print('ETAPA LimpandoTagSaidaReposicao- Fim')
            gc.collect()
        else:
            print(f'JA EXISTE UMA ATUALIZACAO DA  Limpando Tag Saida da Reposicao EM MENOS DE {IntervaloAutomacao} MINUTOS')
            gc.collect()
