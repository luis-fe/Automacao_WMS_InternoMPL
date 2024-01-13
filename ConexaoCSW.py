import jaydebeapi
import empresaConfigurada as emp





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
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
    {'user': '_system', 'password': 'ccscache'},
    'CacheDB.jar'
    )
    return conn


# Função de conectar com o CSW, com 2 opções de conexao:
def ConexaoInternoMPL():
   # try:
        conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
    {'user': '_system', 'password': 'ccscache'},
    'CacheDB.jar'
    )
        return conn
  #  except:
   #     conn2 = Conexao2()
    #    return conn2
