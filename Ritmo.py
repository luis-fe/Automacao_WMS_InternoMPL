import ConexaoPostgreMPL
import pandas as pd

def RelatorioSeparadoresLimite(limite, usuario):

    conn = ConexaoPostgreMPL.conexao()
    relatorio = pd.read_sql('SELECT datatempo, usuario, codpedido from "Reposicao"."ProducaoSeparadores" where ritmo is null  '
                            "and dataseparacao >= '2023-07-20' and usuario = %s "
                            'order by dataseparacao desc', conn, params=(usuario,))
    if not relatorio.empty:
        relatorio = relatorio.reset_index(drop=True)

        relatorio['horario'] = relatorio['datatempo'].str.slice(11, 21)
        relatorio['data'] = relatorio['datatempo'].str.slice(0, 10)
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
        relatorio.fillna(500, inplace=True)

        update = 'UPDATE "Reposicao".tags_separacao ' \
                 'SET ritmo = %s ' \
                 'WHERE codpedido = %s AND dataseparacao = %s and usuario = %s'

        cursor = conn.cursor()
        cursor.executemany(update, relatorio.head(limite)[['ritmo', 'codpedido', 'datatempo', usuario]].values)
        conn.commit()
        cursor.close()
    else:
        print('ok')
