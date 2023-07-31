import ConexaoPostgreMPL
import pandas as pd

def RelatorioSeparadores():

    conn = ConexaoPostgreMPL.conexao()
    relatorio = pd.read_sql('select * from "Reposicao".tags_separacao order by dataseparacao desc', conn)
    Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
    Usuarios['usuario'] = Usuarios['usuario'].astype(str)
    relatorio = pd.merge(relatorio, Usuarios, on='usuario', how='left')
    relatorio = relatorio.reset_index(drop=True)

    relatorio['horario'] = relatorio['dataseparacao'].str.slice(11, 21)
    relatorio['data'] = relatorio['dataseparacao'].str.slice(0, 10)
    relatorio['horario'] = pd.to_datetime(relatorio['horario']).dt.time
    relatorio = relatorio.dropna(subset=['horario'])

    # Ordene o DataFrame pelo usuario, data e horario
    relatorio.sort_values(by=['usuario', 'data', 'horario'], inplace=True)

    def horario_centecimal(time):
        return time.hour + (time.minute / 60) + (time.second / 3600)

    relatorio['horario'] = relatorio['horario'].apply(horario_centecimal)
    relatorio['ritmo'] = relatorio.groupby(['usuario', 'data'])['horario'].diff()
    relatorio['ritmo'] = relatorio['ritmo'] * 3600
    relatorio.fillna(500, inplace=True)

    for _, row in relatorio.head(100).iterrows():
        ritmo = row['ritmo']
        pedido = row['codpedido']
        datahora = row['dataseparacao']
        update = 'UPDATE "Reposicao".tags_separacao ' \
                 'SET ritmo = %s ' \
                 'WHERE codpedido = %s AND dataseparacao = %s'

        cursor = conn.cursor()
        cursor.execute(update, (ritmo, pedido, datahora))
        conn.commit()
        cursor.close()

    return relatorio
