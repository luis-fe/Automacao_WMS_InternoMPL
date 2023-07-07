import pandas as pd
import ConexaoPostgreMPL


def ListaDeEnderecosOculpados():
    conn = ConexaoPostgreMPL.conexao()

    enderecosSku = pd.read_sql('select codendereco as codendereco2 , saldo, codreduzido from "Reposicao".enderecoporsku '
                               ' order by saldo desc',conn)

    # Passo 3: obt
    enderecosSku['repeticoessku'] = enderecosSku.groupby('codreduzido').cumcount() + 1
    enderecosSku['codreduzido'] = enderecosSku['codreduzido'].astype(str)
    enderecosSku['Reserva'] = 0
    enderecosSku['SaldoLiquid'] = enderecosSku['saldo']


    return enderecosSku


def Calculo():
    conn = ConexaoPostgreMPL.conexao()
    # Trazer a Lista de Saldos por codreduzido e  Enderecos
    lista = ListaDeEnderecosOculpados()
    pedidosku = pd.read_sql('SELECT * FROM "Reposicao".pedidossku WHERE necessidade > 0', conn)
    pedidosku.rename(columns={'produto': "codreduzido"}, inplace=True)

    pedidosku['validado'] = 'nao'

    for i in range(2):
    # Loop de iteracao
        lista = lista[lista['repeticoessku'] == 1]
        pedidosku = pedidosku[pedidosku['validado'] == 'nao']
        pedidosku = pd.merge(pedidosku, lista, on='codreduzido', how='left')






        pedidoskuIteracao = pedidosku[pedidosku['SaldoLiquid'] >= 0]
        tamanho = pedidoskuIteracao['codreduzido'].size
        pedidoskuIteracao = pedidoskuIteracao.reset_index(drop=False)
        print(pedidoskuIteracao)
        pedidoskuIteracao['necessidade'] = pedidoskuIteracao['necessidade'].astype(int)
        pedidoskuIteracao['SaldoLiquid'] = pedidoskuIteracao['SaldoLiquid'].astype(int)
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
                                   (pedidoskuIteracao['codendereco2'][i],
                                    str(pedidoskuIteracao['codpedido'][i]), str(pedidoskuIteracao['codreduzido'][i])
                                    ))

                    # Confirmar as alterações
                    conn.commit()
                    pedidosku.loc[pedidosku['codreduzido'] == produto and pedidosku['codpedido'] == pedido  , 'validado']='ok'

            else:
                    print('nao atualizado')

    return 'true'


Calculo()





