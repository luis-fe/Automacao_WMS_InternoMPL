import pandas as pd
import ConexaoPostgreMPL
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def ListaDeEnderecosOculpados(natureza):
    conn = ConexaoPostgreMPL.conexao()

    enderecosSku = pd.read_sql(' select  codreduzido, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco"  '
                               ' where "SaldoLiquid" > 0 and natureza = %s order by "SaldoLiquid" asc',conn,params=(natureza,))

    enderecosSku['repeticoessku'] = enderecosSku.groupby('codreduzido').cumcount() + 1
    enderecosSku['codreduzido'] = enderecosSku['codreduzido'].astype(str)

    return enderecosSku


def Calculo(natureza):
    conn = ConexaoPostgreMPL.conexao()
    total = 0 # Para Totalizar o numer de atualizcoes
    inseridosDuplos = 0
    for i in range(18):
    # Loop de iteracao

        lista = ListaDeEnderecosOculpados(natureza)
        lista = lista[lista['repeticoessku'] == (i + 1)]
        pedidosku = pd.read_sql('SELECT * FROM "Reposicao".pedidossku WHERE necessidade > 0 '
                            "and reservado = 'nao'", conn)
        pedidosku.rename(columns={'produto': "codreduzido"}, inplace=True)
        pedidoskuIteracao = pd.merge(pedidosku, lista, on='codreduzido', how='left')


        tamanho = pedidoskuIteracao['codreduzido'].size
        pedidoskuIteracao = pedidoskuIteracao.reset_index(drop=False)


        for i in range(tamanho):
            necessidade = pedidoskuIteracao['necessidade'][i]
            saldoliq = pedidoskuIteracao['SaldoLiquid'][i]
            endereco = pedidoskuIteracao['codendereco2'][i]
            produto = pedidoskuIteracao['codreduzido'][i]
            pedido = pedidoskuIteracao['codpedido'][i]

            if necessidade<= saldoliq:
                    update = 'UPDATE "Reposicao".pedidossku '\
                             'SET endereco = %s , reservado = %s'\
                             'WHERE codpedido = %s AND produto = %s and reservado = %s '


                    # Filtrar e atualizar os valores "a" para "aa"
                    pedidoskuIteracao.loc[(pedidoskuIteracao['codendereco2'] == endereco) &
                    (pedidoskuIteracao['codreduzido'] == produto), 'SaldoLiquid'] \
                        = pedidoskuIteracao['SaldoLiquid'][i] - pedidoskuIteracao['necessidade'][i]

                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (endereco,'sim',
                                    pedido, produto,'nao')
                                    )

                    # Confirmar as alterações
                    conn.commit()

                    total = total + 1

            if saldoliq >0 and necessidade > saldoliq:
                qtde_sugerida = pd.read_sql('select qtdesugerida from "Reposicao".pedidossku '
                                            "where reservado = 'nao' and codpedido = "+"'"+pedido+"' and produto ="
                                                                                                  " '"+produto+"'",conn)
                if not qtde_sugerida.empty:
                    qtde_sugerida = qtde_sugerida['qtdesugerida'][0]
                    qtde_sugerida2 = qtde_sugerida - saldoliq

                    insert = 'insert into "Reposicao".pedidossku (codpedido, datahora, endereco, necessidade, produto, qtdepecasconf, ' \
                             'qtdesugerida, reservado, status, valorunitarioliq) ' \
                             'select codpedido, datahora, %s, %s, produto, qtdepecasconf, ' \
                             '%s, %s, status, valorunitarioliq from "Reposicao".pedidossku ' \
                             'WHERE codpedido = %s AND produto = %s and reservado = %s ' \
                             ' limit 1;'
                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(insert,
                                   ('Não Reposto', qtde_sugerida2, qtde_sugerida2, 'nao',
                                    pedido, produto,'nao')
                                   )

                    # Confirmar as alterações
                    conn.commit()

                    update = 'UPDATE "Reposicao".pedidossku ' \
                             'SET endereco = %s , qtdesugerida = %s , reservado = %s, necessidade = %s ' \
                             'WHERE codpedido = %s AND produto = %s and reservado = %s and qtdesugerida = %s'


                    # Filtrar e atualizar os valores "a" para "aa"
                    pedidoskuIteracao.loc[(pedidoskuIteracao['codendereco2'] == endereco) &
                                          (pedidoskuIteracao['codreduzido'] == produto), 'SaldoLiquid'] \
                        = 0

                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (endereco, saldoliq,'sim',saldoliq,
                                    pedido, produto,'nao',qtde_sugerida)
                                   )

                    # Confirmar as alterações
                    conn.commit()

                    inseridosDuplos = 1 + inseridosDuplos
            else:
                encerra = i
    datahora = obterHoraAtual()
    print(f'{total} atualizacoes realizadas, as {datahora}')
    return total, inseridosDuplos







