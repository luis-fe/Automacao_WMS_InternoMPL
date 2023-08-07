import ConexaoPostgreMPL
import pandas as pd

def RelatorioSeparadoresLimite(limite):

    conn = ConexaoPostgreMPL.conexao()
    relatorio = pd.read_sql('SELECT datatempo, usuario, codpedido, ritmo as r1 from "Reposicao"."ProducaoSeparadores" where   '
                            " dataseparacao >= '2023-08-01' "
                            'order by dataseparacao ', conn)
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
        limitein = 0
        i =0
        while limitein < limite:

            cursor = conn.cursor()
            if relatorio['r1'][i] != 500:
                print(f'ja tem{relatorio["r1"][i]} ')
                i += 1
            else:
                print(relatorio['r1'][i])
                update = 'UPDATE "Reposicao".tags_separacao ' \
                         'SET ritmo = %s ' \
                         'WHERE codpedido = %s AND dataseparacao = %s '


                cursor.execute(update,(relatorio['ritmo'][i], relatorio['codpedido'][i], relatorio['datatempo'][i]))
                conn.commit()
                i += 1
                limitein +=1
            cursor.close()
    else:
        print('ok')
