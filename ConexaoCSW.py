import jaydebeapi
def Conexao():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/SISTEMAS',
    {'user': 'root', 'password': 'ccscache'},
    'CacheDB.jar'
)
    return conn