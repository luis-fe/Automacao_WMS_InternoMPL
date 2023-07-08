import pandas as pd
import ConexaoPostgreMPL


def ListaDeEnderecosOculpados():
    conn = ConexaoPostgreMPL.conexao()

    enderecosSku = pd.read_sql(' select * from "Reposicao"."calculoEndereco"  '
                               ' order by saldo desc',conn)

    # Passo 3: obt
    enderecosSku['repeticoessku'] = enderecosSku.groupby('codreduzido').cumcount() + 1
    enderecosSku['codreduzido'] = enderecosSku['codreduzido'].astype(str)




    return enderecosSku


def Calculo():
    conn = ConexaoPostgreMPL.conexao()
    total = 0 # Para Totalizar o numer de atualizcoes
    for i in range(20):
    # Loop de iteracao

        lista = ListaDeEnderecosOculpados()
        lista = lista[lista['repeticoessku'] == (i + 1)]
        pedidosku = pd.read_sql('SELECT * FROM "Reposicao".pedidossku WHERE necessidade > 0 '
                            "and reservado = 'nao'", conn)
        pedidosku.rename(columns={'produto': "codreduzido"}, inplace=True)
        pedidoskuIteracao = pd.merge(pedidosku, lista, on='codreduzido', how='left')


        tamanho = pedidoskuIteracao['codreduzido'].size
        pedidoskuIteracao = pedidoskuIteracao.reset_index(drop=False)
        print(pedidoskuIteracao)

        for i in range(tamanho):
            necessidade = pedidoskuIteracao['necessidade'][i]
            print(necessidade)

            if pedidoskuIteracao['necessidade'][i] <= pedidoskuIteracao['SaldoLiquid'][i]:
                    update = 'UPDATE "Reposicao".pedidossku '\
                             'SET endereco = %s '\
                             'WHERE codpedido = %s AND produto = %s'

                    endereco = pedidoskuIteracao['codendereco2'][i]
                    produto = pedidoskuIteracao['codreduzido'][i]
                    pedido = pedidoskuIteracao['codpedido'][i]

                    # Filtrar e atualizar os valores "a" para "aa"
                    pedidoskuIteracao.loc[pedidoskuIteracao['codendereco2'] == endereco, 'SaldoLiquid'] \
                        = pedidoskuIteracao['SaldoLiquid'][i] - pedidoskuIteracao['necessidade'][i]

                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (endereco,
                                    pedido, produto)
                                    )

                    # Confirmar as alterações
                    conn.commit()

                    update2 = 'UPDATE "Reposicao".pedidossku ' \
                             'SET reservado = %s ' \
                             'WHERE codpedido = %s AND produto = %s'

                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update2,
                                   ('sim',pedido, produto)
                                    )

                    # Confirmar as alterações
                    conn.commit()

                    total = total + 1
            else:
                    print('nao atualizado')
    print(f'{total} atualizacoes realizadas')
    return 'true'


Calculo()





