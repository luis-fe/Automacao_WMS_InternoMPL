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
    lista = ListaDeEnderecosOculpados()

    lista = lista[lista['repeticoessku'] == 1]

    pedidosku = pd.read_sql('SELECT * FROM "Reposicao".pedidossku WHERE necessidade > 0',conn)

    pedidosku.rename(columns={'produto': "codreduzido"}, inplace=True)

    pedidosku = pd.merge(pedidosku, lista, on='codreduzido', how='left')

    pedidoskuIteracao = pedidosku[pedidosku['SaldoLiquid'] >= 0]
    tamanho = pedidoskuIteracao['codreduzido'].size

    for i in range(tamanho):
        if pedidoskuIteracao['necessidade'][i] <= pedidoskuIteracao['SaldoLiquid'][i]:
            update = 'UPDATE "Reposicao".pedidossku '\
                     'SET necessidade = 0, endereco = %s '\
                     'WHERE codpedido = %s AND produto = %s'

            pedidoskuIteracao['SaldoLiquid'][i] = pedidoskuIteracao['SaldoLiquid'][i] - pedidoskuIteracao['necessidade'][i]

            cursor = conn.cursor()

            # Executar a atualização na tabela "Reposicao.pedidossku"
            cursor.execute(update,
                           (pedidoskuIteracao['codendereco2'][i],
                            str(pedidoskuIteracao['codpedido'][i]), str(pedidoskuIteracao['codreduzido'][i])
                            ))

            # Confirmar as alterações
            conn.commit()

    return 'true'


Calculo()





