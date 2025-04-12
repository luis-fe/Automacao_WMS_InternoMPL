import jaydebeapi
import empresaConfigurada as emp
from contextlib import contextmanager





empresa = emp.EmpresaEscolhida()
print(empresa)

def Conexao():
    empresa = emp.EmpresaEscolhida()

    if empresa == '1':
        x1 = ConexaoInternoMPL()
        return x1
    else:
        x4 = ConexaoCianorte()
        return x4



def ConexaoCianorte():
   # try:
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://10.162.0.193:1972/CONSISTEM',
    {'user': '_system', 'password': 'ccscache'},
    'CacheDB.jar'
    )
    return conn


# Função de conectar com o CSW, com 2 opções de conexao:
@contextmanager
def ConexaoInternoMPL():
    conn = None
    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            'jdbc:Cache://10.162.0.193:1972/CONSISTEM',
            {'user': '_system', 'password': 'ccscache'},
            'CacheDB.jar'
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()
