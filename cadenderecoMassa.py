import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW

def ImportEndereco(rua, ruaLimite, modulo, moduloLimite, posicao, posicaoLimite, tipo, codempresa, natureza):

    conn = ConexaoPostgreMPL.conexao()
    query = 'insert into "Reposicao".cadendereco (codendereco, rua, modulo, posicao, tipo, codempresa, natureza) ' \
            'values (%s, %s, %s, %s, %s, %s, %s )'


    r = int(rua)
    ruaLimite = int(ruaLimite)

    m = int(modulo)
    moduloLimite = int(moduloLimite)

    p = int(posicao)
    posicaoLimite = int(posicaoLimite)

    while r < ruaLimite:
        ruaAtual = Acres_0(r)
        while m < moduloLimite:
            moduloAtual = Acres_0(m)
            while p < posicaoLimite:
                posicaoAtual = Acres_0(p)
                codendereco = ruaAtual + '-' + moduloAtual +"-"+posicaoAtual
                cursor = conn.cursor()
                select = pd.read_sql('select codendereco from "Reposicao".cadendereco where codendereco = %s ', conn,
                                     params=(codendereco,))
                if select.empty:
                    cursor.execute(query, (codendereco, ruaAtual, moduloAtual, posicaoAtual, tipo, codempresa, natureza,))
                    conn.commit()
                    cursor.close()
                else:
                    cursor.close()
                    print(f'{codendereco} ja exite')
                p += 1
            p = int(posicao)
            m +=1
        m = int(modulo)
        r += 1



def Acres_0(valor):
    if valor < 10:
        valor = str(valor)
        valor = '0'+valor
        return valor
    else:
        valor = str(valor)
        return valor












