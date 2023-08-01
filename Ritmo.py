import ConexaoPostgreMPL
import pandas as pd

def RelatorioSeparadores(limite):

    conn = ConexaoPostgreMPL.conexao()
    relatorio = pd.read_sql(
        'select * from "Reposicao".tags_separacao '
        'where ritmo is null and dataseparacao is not null and dataseparacao '
        "like '2023-08%' order by dataseparacao desc", conn
    )
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

    # Remova esta linha, pois o ritmo já foi calculado corretamente
    # relatorio.fillna(500, inplace=True)

    update = 'UPDATE "Reposicao".tags_separacao ' \
             'SET ritmo = %s ' \
             'WHERE codpedido = %s AND dataseparacao = %s'

    cursor = conn.cursor()
    cursor.executemany(update, relatorio.head(limite)[['ritmo', 'codpedido', 'dataseparacao']].values)
    conn.commit()
    cursor.close()

    return relatorio
