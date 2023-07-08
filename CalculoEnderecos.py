import pandas as pd
import ConexaoPostgreMPL


def ListaDeEnderecosOculpados():
    conn = ConexaoPostgreMPL.conexao()

    enderecosSku = pd.read_sql(' select  codreduzido, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco"  '
                               ' order by saldo desc',conn)

    # Passo 3: obt
    enderecosSku['repeticoessku'] = enderecosSku.groupby('codreduzido').cumcount() + 1
    enderecosSku['codreduzido'] = enderecosSku['codreduzido'].astype(str)




    return enderecosSku


def Calculo():
    conn = ConexaoPostgreMPL.conexao()
    total = 0 # Para Totalizar o numer de atualizcoes
    for i in range(10):
    # Loop de iteracao

        lista = ListaDeEnderecosOculpados()
        lista = lista[lista['repeticoessku'] == (i + 1)]
        pedidosku = pd.read_sql('SELECT * FROM "Reposicao".pedidossku WHERE necessidade > 0 '
                            "and reservado = 'nao'", conn)
        pedidosku.rename(columns={'produto': "codreduzido"}, inplace=True)
        pedidoskuIteracao = pd.merge(pedidosku, lista, on='codreduzido', how='left')


        tamanho = pedidoskuIteracao['codreduzido'].size
        pedidoskuIteracao = pedidoskuIteracao.reset_index(drop=False)


        for i in range(tamanho):
            necessidade = pedidoskuIteracao['necessidade'][i]
            saldo = pedidoskuIteracao['SaldoLiquid'][i]
            endereco = pedidoskuIteracao['codendereco2'][i]
            produto = pedidoskuIteracao['codreduzido'][i]
            pedido = pedidoskuIteracao['codpedido'][i]

            if necessidade<= saldo:
                    update = 'UPDATE "Reposicao".pedidossku '\
                             'SET endereco = %s , reservado = %s'\
                             'WHERE codpedido = %s AND produto = %s'


                    # Filtrar e atualizar os valores "a" para "aa"
                    pedidoskuIteracao.loc[(pedidoskuIteracao['codendereco2'] == endereco) &
                    (pedidoskuIteracao['codreduzido'] == produto), 'SaldoLiquid'] \
                        = pedidoskuIteracao['SaldoLiquid'][i] - pedidoskuIteracao['necessidade'][i]

                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (endereco,'sim',
                                    pedido, produto)
                                    )

                    # Confirmar as alterações
                    conn.commit()

                    total = total + 1

            if saldo >0 and necessidade > saldo:
                qtde_sugerida = pd.read_sql('select qtdesugerida from "Reposicao".pedidossku '
                                            "where reservado = 'nao' and codpedido = "+"'"+pedido+"' and produto ="
                                                                                                  " '"+produto+"'",conn)
                qtde_sugerida = qtde_sugerida['qtdesugerida'][0]
                update = 'UPDATE "Reposicao".pedidossku ' \
                         'SET endereco = %s , qtdesugerida = %s , reservado = %s' \
                         'WHERE codpedido = %s AND produto = %s'


                # Filtrar e atualizar os valores "a" para "aa"
                pedidoskuIteracao.loc[(pedidoskuIteracao['codendereco2'] == endereco) &
                                      (pedidoskuIteracao['codreduzido'] == produto), 'SaldoLiquid'] \
                    = 0

                cursor = conn.cursor()

                # Executar a atualização na tabela "Reposicao.pedidossku"
                cursor.execute(update,
                               (endereco, saldo,'sim',
                                pedido, produto)
                               )

                # Confirmar as alterações
                conn.commit()
                insert = 'insert into "Reposicao".pedidossku (codpedido, datahora, endereco, necessidade, produto, qtdepecasconf, ' \
                     'qtdesugerida, reservado, status, valorunitarioliq) ' \
                     'select codpedido, datahora, %s, %s, produto, qtdepecasconf, ' \
                     '%s, %s, status, valorunitarioliq from "Reposicao".pedidossku ' \
                     'WHERE codpedido = %s AND produto = %s'
                cursor = conn.cursor()
                qtde_sugerida = qtde_sugerida - saldo

                # Executar a atualização na tabela "Reposicao.pedidossku"
                cursor.execute(insert,
                           ('Não Reposto', 0, qtde_sugerida, 'nao',
                            pedido, produto)
                           )

                # Confirmar as alterações
                conn.commit()

            else:
                    print('nao atualizado')
    print(f'{total} atualizacoes realizadas')
    return 'true'


Calculo()





