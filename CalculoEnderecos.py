import pandas as pd
import ConexaoPostgreMPL


def ListaDeEnderecosOculpados():
    conn = ConexaoPostgreMPL.conexao()

    enderecosSku = pd.read_sql(' SELECT * from "Reposicao".enderecoporsku '
                               ' order by saldo desc',conn)

    # Passo 3: obt
    enderecosSku['repeticoessku'] = enderecosSku.groupby('codreduzido').cumcount() + 1



    return enderecosSku


x = ListaDeEnderecosOculpados()
print(x)