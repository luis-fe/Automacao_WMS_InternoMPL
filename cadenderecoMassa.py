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

    while r <= ruaLimite:
        ruaAtual = str(r)
        while m <= moduloLimite:
            moduloAtual = str(m)
            while p <= posicaoLimite:
                posicaoAtual = str(p)
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














