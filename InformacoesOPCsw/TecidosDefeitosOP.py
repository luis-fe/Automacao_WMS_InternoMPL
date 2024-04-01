import pandas as pd
import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S')
    return agora


def DefeitosTecidos():
    conn = ConexaoCSW.Conexao()

    consulta = pd.read_sql(BuscasAvancadas.Movimento(),conn)#coditem , mo.nomeItem, mo.codFornecNota, mo.dataLcto , mo.numDocto, mo.numeroLcto
    consulta.drop(['numDocto', 'numeroLcto','dataLcto'], axis=1, inplace=True)

    fornecedor = pd.read_sql(BuscasAvancadas.Fornecedor(),conn)

    fornecedor['codFornecNota'] = fornecedor['codFornecNota'].astype(str)
    consulta['codFornecNota'] = consulta['codFornecNota'].astype(str)

    consulta = pd.merge(consulta, fornecedor, on='codFornecNota', how='left')
    consulta.drop(['codFornecNota'], axis=1, inplace=True)
    consulta = consulta.drop_duplicates()
    consulta['repeticoessku'] = consulta.groupby('coditem').cumcount() + 1

    conn.close()

    consulta['categoria'] = '-'
    consulta['categoria']  = consulta.apply(lambda row : Categoria('RIBANA',row['nomeItem'],row['categoria'])  )
    # Carregando dados no Wms
    ConexaoPostgreMPL.Funcao_Inserir(consulta, consulta['coditem'].size, 'OPSDefeitoTecidos', 'replace')
    return consulta



def Categoria(contem, descricao, retorno):
    if retorno != '-':
        return '-'
    elif contem in descricao:
        return 'limpar'
    else:
        return '-'

