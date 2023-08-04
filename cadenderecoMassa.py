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
                try:
                    cursor.execute(query, (codendereco, ruaAtual, moduloAtual, posicaoAtual, tipo, codempresa, natureza,))
                    conn.commit()
                    cursor.close()
                except:
                    print(f'{codendereco} ja exite')
                p += 1
            m +=1
        r += 1



def Acres_0(valor):
    if valor < 10:
        valor = str(valor)
        valor = '0'+valor
        return valor
    else:
        valor = str(valor)
        return valor












